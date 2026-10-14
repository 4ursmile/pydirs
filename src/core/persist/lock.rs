//! Advisory single-owner lock: a `LOCK` file holding the owner's pid.
//!
//! A lock is taken with `create_new`, so exactly one process can create it. An existing
//! file is taken over only when its pid is not alive (or it is this process's pid and no
//! live handle in this process holds it, which is what a SIGKILLed test store looks like).
//! A file that is empty or unparsable is treated as stale. Within one process, a second
//! `acquire` on a held directory always fails.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::Mutex;

/// Directories currently held by a live `LockFile` in this process (canonical paths).
static HELD: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

pub(crate) const LOCK_FILE: &str = "LOCK";

#[derive(Debug)]
pub(crate) struct LockFile {
    path: PathBuf,
    key: PathBuf,
    /// Set by the crash-simulation hook: the file is left behind, as after SIGKILL.
    abandoned: AtomicBool,
}

fn would_block() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "data directory is locked by another live process",
    )
}

impl LockFile {
    /// Takes the lock on `dir`, which must already exist.
    pub(crate) fn acquire(dir: &Path) -> io::Result<LockFile> {
        let key = dir.canonicalize()?;
        let path = dir.join(LOCK_FILE);
        let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
        if held.contains(&key) {
            return Err(would_block());
        }
        for _ in 0..3 {
            match OpenOptions::new().write(true).create_new(true).open(&path) {
                Ok(mut f) => {
                    writeln!(f, "{}", process::id())?;
                    held.push(key.clone());
                    return Ok(LockFile {
                        path,
                        key,
                        abandoned: AtomicBool::new(false),
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                    let owner = fs::read_to_string(&path)
                        .ok()
                        .and_then(|s| s.trim().parse::<u32>().ok());
                    if let Some(pid) = owner {
                        if pid != process::id() && pid_alive(pid) {
                            return Err(would_block());
                        }
                    }
                    // Stale: its owner is gone. Remove it and retry the create.
                    match fs::remove_file(&path) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(would_block())
    }

    /// Test hook: behave as a SIGKILLed owner. The file is left on disk and the in-process
    /// registration is released at once, so the directory can be reopened right away. The
    /// leftover pid is this process's own, which `acquire` treats as stale.
    pub(crate) fn abandon(&self) {
        self.abandoned.store(true, Relaxed);
        let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
        held.retain(|p| *p != self.key);
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        if !self.abandoned.load(Relaxed) {
            let _ = fs::remove_file(&self.path);
            let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
            held.retain(|p| *p != self.key);
        }
    }
}

/// Whether `pid` names a live process. Errs on the side of "alive" when unsure.
#[cfg(unix)]
fn pid_alive(pid: u32) -> bool {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const ESRCH: i32 = 3;
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    // Signal 0 performs the existence and permission check without sending anything.
    // SAFETY: `kill` with signal 0 has no side effects beyond the existence check.
    let rc = unsafe { kill(pid, 0) };
    rc == 0 || io::Error::last_os_error().raw_os_error() != Some(ESRCH)
}

#[cfg(windows)]
fn pid_alive(pid: u32) -> bool {
    extern "system" {
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut core::ffi::c_void;
        fn GetExitCodeProcess(handle: *mut core::ffi::c_void, code: *mut u32) -> i32;
        fn CloseHandle(handle: *mut core::ffi::c_void) -> i32;
    }
    const PROCESS_QUERY_LIMITED_INFORMATION: u32 = 0x1000;
    const STILL_ACTIVE: u32 = 259;
    const ERROR_ACCESS_DENIED: i32 = 5;
    // SAFETY: plain Win32 calls; the handle is closed before returning.
    unsafe {
        let h = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if h.is_null() {
            // Access denied means the process exists but belongs to someone else.
            return io::Error::last_os_error().raw_os_error() == Some(ERROR_ACCESS_DENIED);
        }
        let mut code = 0u32;
        let ok = GetExitCodeProcess(h, &mut code);
        CloseHandle(h);
        // If the exit code cannot be read, assume alive (the safe direction).
        ok == 0 || code == STILL_ACTIVE
    }
}

#[cfg(not(any(unix, windows)))]
fn pid_alive(_pid: u32) -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("tc-lock-unit-{name}-{}", process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn second_acquire_in_process_is_would_block() {
        let d = dir("contend");
        let a = LockFile::acquire(&d).unwrap();
        assert_eq!(
            LockFile::acquire(&d).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(a);
        assert!(LockFile::acquire(&d).is_ok());
    }

    #[test]
    fn dead_pid_is_taken_over() {
        let d = dir("stale");
        // Above any real pid on Linux or macOS, so the owner cannot exist.
        fs::write(d.join(LOCK_FILE), "2147483646\n").unwrap();
        assert!(LockFile::acquire(&d).is_ok());
    }

    #[test]
    fn live_foreign_pid_blocks() {
        let d = dir("live");
        // pid 1 (init/launchd) is alive and not ours.
        fs::write(d.join(LOCK_FILE), "1\n").unwrap();
        assert_eq!(
            LockFile::acquire(&d).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
}
