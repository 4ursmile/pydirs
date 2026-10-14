//! Snapshot file (`snapshot.tcs`), written shard by shard into a buffered writer.
//!
//! Layout (all integers little-endian):
//! ```text
//! magic "TCSNAP01" | version u32 (= 1) | generation u64
//! record*:  tag u8 (1 str, 2 list, 3 hash, 4 set) | key (varint len + bytes) | expires_at u64
//!           str:  value (varint len + bytes)
//!           list/set: count varint, then members (varint len + bytes each)
//!           hash: count varint, then field and value pairs
//! end:      tag 0xFF | record count u64
//! trailer:  crc32 u32 over every byte before the trailer
//! ```
//! `load` verifies the CRC in a first pass and parses only after it passes, so a damaged
//! file is rejected before any of it reaches the store.

use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::path::Path;

use super::codec::{put_varint, Crc32};
use crate::core::value::{Collection, Entry};

pub(crate) const MAGIC: &[u8; 8] = b"TCSNAP01";
const VERSION: u32 = 1;
/// Smallest valid file: header, an empty end marker, and the trailer.
const MIN_LEN: u64 = 8 + 4 + 8 + 1 + 8 + 4;

const T_STR: u8 = 1;
const T_LIST: u8 = 2;
const T_HASH: u8 = 3;
const T_SET: u8 = 4;
const T_END: u8 = 0xFF;

fn bad(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("snapshot: {msg}"))
}

/// Streaming writer. Everything written except the trailer feeds the running CRC.
pub(crate) struct SnapWriter {
    out: BufWriter<File>,
    crc: Crc32,
    count: u64,
    hdr: Vec<u8>,
}

impl SnapWriter {
    pub(crate) fn create(path: &Path, gen: u64) -> io::Result<SnapWriter> {
        let file = File::create(path)?;
        let mut w = SnapWriter {
            out: BufWriter::with_capacity(64 * 1024, file),
            crc: Crc32::default(),
            count: 0,
            hdr: Vec::with_capacity(10),
        };
        w.put(MAGIC)?;
        w.put(&VERSION.to_le_bytes())?;
        w.put(&gen.to_le_bytes())?;
        Ok(w)
    }

    fn put(&mut self, b: &[u8]) -> io::Result<()> {
        self.crc.update(b);
        self.out.write_all(b)
    }

    fn var(&mut self, n: u64) -> io::Result<()> {
        self.hdr.clear();
        put_varint(&mut self.hdr, n);
        let hdr = std::mem::take(&mut self.hdr);
        let r = self.put(&hdr);
        self.hdr = hdr;
        r
    }

    fn bytes(&mut self, b: &[u8]) -> io::Result<()> {
        self.var(b.len() as u64)?;
        self.put(b)
    }

    /// Writes one live entry. The caller skips expired entries.
    pub(crate) fn entry(&mut self, e: &Entry) -> io::Result<()> {
        let key = e.key();
        if let Some(v) = e.as_str() {
            self.put(&[T_STR])?;
            self.bytes(key)?;
            self.put(&e.expires_at.to_le_bytes())?;
            self.bytes(v)?;
        } else if let Some(c) = e.coll() {
            self.put(&[match &c.data {
                Collection::List(_) => T_LIST,
                Collection::Hash(_) => T_HASH,
                Collection::Set(_) => T_SET,
            }])?;
            self.bytes(key)?;
            self.put(&e.expires_at.to_le_bytes())?;
            match &c.data {
                Collection::List(l) => {
                    self.var(l.len() as u64)?;
                    for item in l {
                        self.bytes(item)?;
                    }
                }
                Collection::Hash(h) => {
                    self.var(h.len() as u64)?;
                    for (f, v) in h {
                        self.bytes(f)?;
                        self.bytes(v)?;
                    }
                }
                Collection::Set(s) => {
                    self.var(s.len() as u64)?;
                    for m in s {
                        self.bytes(m)?;
                    }
                }
            }
        }
        self.count += 1;
        Ok(())
    }

    /// Writes the end marker and trailer, syncs the file, and returns its size in bytes.
    pub(crate) fn finish(mut self) -> io::Result<u64> {
        let count = self.count.to_le_bytes();
        self.put(&[T_END])?;
        self.put(&count)?;
        let crc = self.crc.value();
        self.out.write_all(&crc.to_le_bytes())?;
        self.out.flush()?;
        let file = self.out.into_inner().map_err(|e| e.into_error())?;
        file.sync_all()?;
        Ok(file.metadata()?.len())
    }
}

/// One decoded record, owned so the caller can insert it without borrowing the file.
pub(crate) enum Record {
    Str {
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    },
    List {
        key: Vec<u8>,
        items: Vec<Vec<u8>>,
        expires_at: u64,
    },
    Hash {
        key: Vec<u8>,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
        expires_at: u64,
    },
    Set {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
        expires_at: u64,
    },
}

fn read_u8(r: &mut impl Read) -> io::Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

fn read_u64(r: &mut impl Read) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_le_bytes(b))
}

fn read_varint(r: &mut impl Read) -> io::Result<u64> {
    let mut n = 0u64;
    let mut shift = 0u32;
    loop {
        let b = read_u8(r)?;
        if shift > 63 {
            return Err(bad("varint overflow"));
        }
        n |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(n);
        }
        shift += 7;
    }
}

fn read_bytes(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let n = usize::try_from(read_varint(r)?).map_err(|_| bad("length overflow"))?;
    let mut v = vec![0u8; n];
    r.read_exact(&mut v)?;
    Ok(v)
}

fn read_list(r: &mut impl Read) -> io::Result<Vec<Vec<u8>>> {
    let n = read_varint(r)?;
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(read_bytes(r)?);
    }
    Ok(out)
}

/// Loads `path`, passing each record to `sink`. Returns the file's generation.
/// Fails with `InvalidData` on a bad magic, version, length or CRC. Nothing is passed to
/// `sink` unless the CRC matches.
pub(crate) fn load(path: &Path, mut sink: impl FnMut(Record)) -> io::Result<u64> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    if len < MIN_LEN {
        return Err(bad("file truncated"));
    }

    // Pass 1: checksum everything except the trailing four bytes.
    let mut crc = Crc32::default();
    let mut remaining = len - 4;
    let mut buf = vec![0u8; 64 * 1024];
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        let n = f.read(&mut buf[..want])?;
        if n == 0 {
            return Err(bad("file truncated"));
        }
        crc.update(&buf[..n]);
        remaining -= n as u64;
    }
    let mut stored = [0u8; 4];
    f.read_exact(&mut stored)?;
    if u32::from_le_bytes(stored) != crc.value() {
        return Err(bad("checksum mismatch"));
    }

    // Pass 2: parse. The reader stops at the trailer, so the parser never sees it.
    let mut r = io::BufReader::new(File::open(path)?.take(len - 4));
    let mut magic = [0u8; 8];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(bad("bad magic"));
    }
    let mut ver = [0u8; 4];
    r.read_exact(&mut ver)?;
    if u32::from_le_bytes(ver) != VERSION {
        return Err(bad("unsupported version"));
    }
    let gen = read_u64(&mut r)?;

    let mut seen = 0u64;
    loop {
        let tag = read_u8(&mut r)?;
        if tag == T_END {
            if read_u64(&mut r)? != seen {
                return Err(bad("record count mismatch"));
            }
            break;
        }
        let key = read_bytes(&mut r)?;
        let expires_at = read_u64(&mut r)?;
        let rec = match tag {
            T_STR => Record::Str {
                key,
                value: read_bytes(&mut r)?,
                expires_at,
            },
            T_LIST => Record::List {
                key,
                items: read_list(&mut r)?,
                expires_at,
            },
            T_HASH => {
                let n = read_varint(&mut r)?;
                let mut pairs = Vec::new();
                for _ in 0..n {
                    let f = read_bytes(&mut r)?;
                    pairs.push((f, read_bytes(&mut r)?));
                }
                Record::Hash {
                    key,
                    pairs,
                    expires_at,
                }
            }
            T_SET => Record::Set {
                key,
                members: read_list(&mut r)?,
                expires_at,
            },
            _ => return Err(bad("unknown record type")),
        };
        sink(rec);
        seen += 1;
    }
    Ok(gen)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_loads_with_its_generation() {
        let dir = std::env::temp_dir().join(format!("tc-snap-unit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("empty.tcs");
        SnapWriter::create(&p, 7).unwrap().finish().unwrap();
        let mut n = 0;
        assert_eq!(load(&p, |_| n += 1).unwrap(), 7);
        assert_eq!(n, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn flipped_byte_fails_checksum_before_any_record() {
        let dir = std::env::temp_dir().join(format!("tc-snap-crc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("bad.tcs");
        SnapWriter::create(&p, 1).unwrap().finish().unwrap();
        let mut bytes = std::fs::read(&p).unwrap();
        bytes[10] ^= 0xFF;
        std::fs::write(&p, &bytes).unwrap();
        let mut n = 0;
        let err = load(&p, |_| n += 1).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(n, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
