//! Write-ahead log: one file per generation, `wal-<gen>.log`, holding a sequence of frames
//! `[len u32 LE][crc32 u32 LE][payload]`. A frame is written with a single `write(2)` and
//! no user-space buffer, so a process that dies after `append` returns has handed the
//! record to the OS (SIGKILL-safe for every fsync policy). `FsyncPolicy` only controls
//! when `fsync` is called.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use super::codec::crc32;
use super::{sync_dir, FsyncPolicy};

const FRAME_HEADER: usize = 8;

pub(crate) fn path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("wal-{gen}.log"))
}

fn parse_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Generations of all WAL files in `dir`, ascending.
pub(crate) fn list(dir: &Path) -> io::Result<Vec<u64>> {
    let mut gens = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(g) = entry.file_name().to_str().and_then(parse_name) {
            gens.push(g);
        }
    }
    gens.sort_unstable();
    Ok(gens)
}

fn closed() -> io::Error {
    io::Error::other("store is closed")
}

/// The active WAL file. `file` is `None` after shutdown, which stops all logging.
pub(crate) struct Wal {
    file: Option<File>,
    gen: u64,
    bytes: u64,
    /// Appended since the last `fsync` (only tracked for `FsyncPolicy::EverySec`).
    unsynced: bool,
    /// Reused frame buffer, so appending allocates only when a record outgrows it.
    scratch: Vec<u8>,
}

impl Wal {
    /// Opens generation `gen` for appending, creating it if missing.
    pub(crate) fn open(dir: &Path, gen: u64) -> io::Result<Wal> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path(dir, gen))?;
        sync_dir(dir)?;
        let bytes = file.metadata()?.len();
        Ok(Wal {
            file: Some(file),
            gen,
            bytes,
            unsynced: false,
            scratch: Vec::new(),
        })
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    /// True when the active generation holds records, so a snapshot would change the
    /// on-disk state. Covers records replayed at open as well as new appends.
    pub(crate) fn dirty(&self) -> bool {
        self.bytes > 0
    }

    /// Appends one frame. `fill` writes the payload. On a failed write the file is cut back
    /// to its previous length, so a partial frame never sits in front of later records.
    pub(crate) fn append(
        &mut self,
        fsync: FsyncPolicy,
        fill: impl FnOnce(&mut Vec<u8>),
    ) -> io::Result<()> {
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        self.scratch.clear();
        self.scratch.extend_from_slice(&[0u8; FRAME_HEADER]);
        fill(&mut self.scratch);
        let payload = self.scratch.len() - FRAME_HEADER;
        let len = u32::try_from(payload)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "WAL record over 4 GiB"))?;
        let crc = crc32(&self.scratch[FRAME_HEADER..]);
        self.scratch[0..4].copy_from_slice(&len.to_le_bytes());
        self.scratch[4..8].copy_from_slice(&crc.to_le_bytes());
        if let Err(e) = file.write_all(&self.scratch) {
            let _ = file.set_len(self.bytes);
            return Err(e);
        }
        self.bytes += self.scratch.len() as u64;
        match fsync {
            FsyncPolicy::Always => file.sync_data()?,
            FsyncPolicy::EverySec => self.unsynced = true,
            FsyncPolicy::Never => {}
        }
        Ok(())
    }

    /// Makes everything appended so far durable, if anything is pending.
    pub(crate) fn sync(&mut self) -> io::Result<()> {
        if self.unsynced {
            if let Some(file) = self.file.as_ref() {
                file.sync_data()?;
            }
            self.unsynced = false;
        }
        Ok(())
    }

    /// Syncs the current file and starts generation `gen + 1` as the new active file.
    /// Returns the new generation. The old file is kept until the snapshot supersedes it.
    pub(crate) fn rotate(&mut self, dir: &Path) -> io::Result<u64> {
        let Some(old) = self.file.as_ref() else {
            return Err(closed());
        };
        old.sync_data()?;
        self.unsynced = false;
        let next = self.gen + 1;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path(dir, next))?;
        sync_dir(dir)?;
        self.file = Some(file);
        self.gen = next;
        self.bytes = 0;
        Ok(next)
    }

    /// Syncs and stops accepting records.
    pub(crate) fn close(&mut self) -> io::Result<()> {
        let result = self.sync();
        self.file = None;
        result
    }

    /// Stops accepting records without syncing. Used to simulate a process kill: data already
    /// handed to the OS by `append` is still on disk, nothing else is written.
    pub(crate) fn abandon(&mut self) {
        self.file = None;
    }
}

/// Result of scanning one WAL file.
pub(crate) struct Replay {
    /// Byte length of the valid prefix.
    pub(crate) valid_len: u64,
    /// True when bytes follow the valid prefix (a torn or corrupt tail).
    pub(crate) torn: bool,
}

/// Feeds every valid record of `path` to `apply`, stopping at the first short or
/// checksum-failing frame. Errors from `apply` abort the scan.
pub(crate) fn replay(
    path: &Path,
    mut apply: impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<Replay> {
    let file = File::open(path)?;
    let total = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut pos = 0u64;
    let mut head = [0u8; FRAME_HEADER];
    let mut buf = Vec::new();
    loop {
        if pos == total {
            return Ok(Replay {
                valid_len: pos,
                torn: false,
            });
        }
        if reader.read_exact(&mut head).is_err() {
            break;
        }
        let len = u64::from(u32::from_le_bytes([head[0], head[1], head[2], head[3]]));
        let crc = u32::from_le_bytes([head[4], head[5], head[6], head[7]]);
        if pos + FRAME_HEADER as u64 + len > total {
            break;
        }
        buf.resize(len as usize, 0);
        if reader.read_exact(&mut buf).is_err() || crc32(&buf) != crc {
            break;
        }
        apply(&buf)?;
        pos += FRAME_HEADER as u64 + len;
    }
    Ok(Replay {
        valid_len: pos,
        torn: true,
    })
}

/// Cuts a WAL file back to its valid prefix and makes the cut durable.
pub(crate) fn truncate(path: &Path, len: u64) -> io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_all()
}
