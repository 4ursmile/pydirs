//! Durability: snapshot + write-ahead log (PLAN.md 4d).
//!
//! Files in the data directory:
//! - `snapshot.tcs`: the full dataset as of generation `g` (see [`snapshot`]);
//! - `snapshot.tmp`: a snapshot being written; deleted on open if present;
//! - `wal-<n>.log`: mutations logged after the rotation into generation `n` (see [`wal`]);
//! - `LOCK`: owner pid, so two live processes cannot open the same directory (see [`lock`]).
//!
//! Recovery loads `snapshot.tcs` (generation `g`, or 0 if absent) and replays every
//! `wal-n.log` with `n >= g` in order. Older WAL files are superseded and deleted.
//!
//! Logging rules that replay depends on:
//! - every mutation is logged under the shard write lock that applied it, so the log order
//!   matches the per-key order;
//! - every removal of a key that is not an ordinary command effect (lazy expiry, sweeper,
//!   eviction, DEL) is logged as DEL, because replay does not expire anything itself;
//! - writes are logged as the resulting state (SET with the absolute expiry) or as the
//!   operation on a collection, which replays to the same state.
//!
//! `save` rotates the WAL to `g+1` while holding every shard's read lock, so no write can
//! fall between the snapshot and the new log. It then streams the shards into
//! `snapshot.tmp`, renames it over `snapshot.tcs`, and deletes the older logs. A crash at
//! any point leaves a state that recovery reads back exactly.

pub(crate) mod codec;
mod lock;
mod snapshot;
mod wal;

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;

use super::expiry;
use super::store::{Config, Inner, Policy, Store};
use super::value::{elem_size, pair_size, Collection, Entry, Map, Set};
use codec::{
    Rd, OP_DEL, OP_EXPIRE, OP_FLUSH, OP_HDEL, OP_HSET, OP_LPOP, OP_LPUSH, OP_RPOP, OP_RPUSH,
    OP_SADD, OP_SET, OP_SREM,
};
use lock::LockFile;
use snapshot::Record;
use wal::Wal;

const SNAP_FILE: &str = "snapshot.tcs";
const SNAP_TMP: &str = "snapshot.tmp";
/// Granularity at which the background threads check for shutdown.
const TICK: Duration = Duration::from_millis(10);
const FSYNC_PERIOD: Duration = Duration::from_secs(1);

/// When WAL appends are fsynced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// `fsync` after every record: a successful write survives power loss.
    Always,
    /// `fsync` once per second from a background thread.
    EverySec,
    /// Leave flushing to the OS. Records survive a process kill but not power loss.
    Never,
}

/// Where and how the store persists its data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistConfig {
    pub dir: PathBuf,
    pub fsync: FsyncPolicy,
    /// Seconds between background snapshots, taken only when there are unsaved writes.
    /// Zero disables the timer; snapshots then happen only on `save()` and `close()`.
    pub snapshot_interval_secs: u64,
}

/// Point-in-time persistence counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistStats {
    /// Wall-clock ms of the last successful snapshot in this process; 0 if none.
    pub last_save_ms: u64,
    /// Size of the current `snapshot.tcs` in bytes; 0 if none.
    pub snapshot_bytes: u64,
    /// Bytes in the active WAL generation.
    pub wal_bytes: u64,
    /// Live keys present after recovery.
    pub loaded_keys: usize,
    /// Most recent persistence error or recovery warning. Cleared by a successful save.
    pub last_error: Option<String>,
}

/// Points at which [`Store::save_stopping_after`] returns early, as if the process died
/// there (test hook).
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveStep {
    /// The WAL has been rotated; the snapshot has not been written.
    AfterRotate,
    /// `snapshot.tmp` is complete and synced; it has not been renamed.
    AfterTmpWrite,
    /// The snapshot is in place; the superseded WAL files still exist.
    AfterRename,
    /// The superseded WAL files are deleted; the directory is not yet synced.
    AfterWalDelete,
}

#[derive(Default)]
struct Meta {
    last_save_ms: u64,
    snapshot_bytes: u64,
    loaded_keys: usize,
    last_error: Option<String>,
}

/// Persistence state attached to a store once recovery has finished.
pub(crate) struct Persist {
    dir: PathBuf,
    fsync: FsyncPolicy,
    /// The active WAL. Locked while a shard lock is held, never the other way round.
    wal: Mutex<Wal>,
    /// Serialises saves (manual, background, close).
    save_serial: Mutex<()>,
    meta: Mutex<Meta>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// Held until `close`, which releases the directory for another process. Removes the
    /// LOCK file when dropped.
    lock: Mutex<Option<LockFile>>,
}

impl Persist {
    fn append(&self, fill: impl FnOnce(&mut Vec<u8>)) {
        let mut wal = self.wal.lock();
        if let Err(e) = wal.append(self.fsync, fill) {
            drop(wal);
            self.set_error(format!("WAL append failed: {e}"));
        }
    }

    fn dirty(&self) -> bool {
        self.wal.lock().dirty()
    }

    fn set_error(&self, msg: String) {
        self.meta.lock().last_error = Some(msg);
    }

    fn join_threads(&self) {
        let handles = std::mem::take(&mut *self.threads.lock());
        for h in handles {
            let _ = h.join();
        }
    }
}

impl Inner {
    /// Logs one mutation if persistence is enabled. Callers hold the shard lock for the key.
    /// With persistence off this is a single `Option` check; the closure is never built.
    #[inline]
    pub(crate) fn log(&self, fill: impl FnOnce(&mut Vec<u8>)) {
        if let Some(p) = &self.durable {
            p.append(fill);
        }
    }
}

pub(crate) fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Fsyncs a directory so that renames and file creations inside it are durable.
fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

fn not_enabled() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "persistence is not enabled for this store",
    )
}

fn corrupt(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("WAL record malformed: {what}"),
    )
}

fn need<T>(v: Option<T>) -> io::Result<T> {
    v.ok_or_else(|| corrupt("truncated"))
}

impl Store {
    /// Opens (or creates) the store in `cfg.persist.dir` and recovers its contents. Without
    /// `cfg.persist` this is the same as [`Store::new`].
    ///
    /// Errors:
    /// - `WouldBlock`: another live process holds the directory.
    /// - `InvalidData`: `snapshot.tcs` fails its checksum or structure. The store does not
    ///   fall back to an older state: the WAL files that would be needed were deleted when
    ///   that snapshot was written. Recovery is left to the operator (move the file aside
    ///   to start empty).
    /// - `InvalidData` from a WAL record that is well-framed but malformed. This is not a
    ///   torn tail (a torn tail is cut off, see below).
    ///
    /// A torn or checksum-failing WAL tail is truncated at the first bad frame and reported in
    /// `last_error`; records after that point are discarded.
    pub fn open(cfg: Config) -> io::Result<Store> {
        let Some(pc) = cfg.persist.clone() else {
            return Ok(Store::new(cfg));
        };
        fs::create_dir_all(&pc.dir)?;
        let lock = LockFile::acquire(&pc.dir)?;

        // Replay runs with a frozen clock at 0, where no key is expired, and with no memory
        // limit, so the logged state is restored exactly. The real clock, limit and policy
        // are installed afterwards. Nothing is logged during replay because nothing is
        // attached yet.
        let replay_cfg = Config {
            max_memory: 0,
            policy: Policy::NoEviction,
            sweep_interval_ms: 0,
            persist: None,
            ..cfg.clone()
        };
        let mut store = Store::build(&replay_cfg, Arc::new(|| 0));
        let recovered = recover(&pc.dir, &store)?;

        let wal = Wal::open(&pc.dir, recovered.active_gen)?;
        {
            // Only this handle exists, so `get_mut` always succeeds.
            let inner = Arc::get_mut(&mut store.inner).expect("store is not shared yet");
            inner.clock = Arc::new(system_now_ms);
            inner.max_memory = cfg.max_memory;
            inner.policy = cfg.policy;
            inner.durable = Some(Persist {
                dir: pc.dir.clone(),
                fsync: pc.fsync,
                wal: Mutex::new(wal),
                save_serial: Mutex::new(()),
                meta: Mutex::new(Meta {
                    last_save_ms: 0,
                    snapshot_bytes: recovered.snapshot_bytes,
                    loaded_keys: 0,
                    last_error: recovered.warning,
                }),
                threads: Mutex::new(Vec::new()),
                lock: Mutex::new(Some(lock)),
            });
        }

        // Keys that expired while the store was closed are removed now, against the real
        // clock. The removals are logged, so the log stays in step with memory.
        store.inner.purge_expired();
        let loaded = store.dbsize();
        if let Some(p) = store.inner.durable.as_ref() {
            p.meta.lock().loaded_keys = loaded;
        }

        if cfg.sweep_interval_ms > 0 {
            expiry::spawn(
                Arc::downgrade(&store.inner),
                Duration::from_millis(cfg.sweep_interval_ms),
            );
        }
        store.start_background(&pc)?;
        Ok(store)
    }

    fn start_background(&self, pc: &PersistConfig) -> io::Result<()> {
        let mut handles = Vec::new();
        if pc.fsync == FsyncPolicy::EverySec {
            handles.push(spawn_ticker(
                Arc::downgrade(&self.inner),
                "tinycache-fsync",
                FSYNC_PERIOD,
                |s| {
                    if let Some(p) = s.inner.durable.as_ref() {
                        if let Err(e) = p.wal.lock().sync() {
                            p.set_error(format!("fsync failed: {e}"));
                        }
                    }
                },
            )?);
        }
        if pc.snapshot_interval_secs > 0 {
            handles.push(spawn_ticker(
                Arc::downgrade(&self.inner),
                "tinycache-snapshot",
                Duration::from_secs(pc.snapshot_interval_secs),
                |s| {
                    if s.inner.durable.as_ref().is_some_and(Persist::dirty) {
                        let _ = s.save();
                    }
                },
            )?);
        }
        if let Some(p) = self.inner.durable.as_ref() {
            p.threads.lock().extend(handles);
        }
        Ok(())
    }

    /// Writes a snapshot and starts a new WAL generation. Writers are blocked only while the
    /// log is rotated and while each shard is serialised. Errors if persistence is off.
    pub fn save(&self) -> io::Result<()> {
        self.save_inner(None)
    }

    /// Persistence counters, or `None` if persistence is not enabled.
    pub fn persist_stats(&self) -> Option<PersistStats> {
        let p = self.inner.durable.as_ref()?;
        let wal_bytes = p.wal.lock().bytes();
        let m = p.meta.lock();
        Some(PersistStats {
            last_save_ms: m.last_save_ms,
            snapshot_bytes: m.snapshot_bytes,
            wal_bytes,
            loaded_keys: m.loaded_keys,
            last_error: m.last_error.clone(),
        })
    }

    /// Stops the sweeper and background threads, writes a final snapshot, syncs the WAL and
    /// releases the data directory's lock, so another process may open it. Without persistence
    /// it only stops the sweeper. Idempotent. The store stays readable afterwards, but a
    /// persistent store no longer logs writes, so later mutations are not durable.
    pub fn close(&self) {
        if self.inner.closed.swap(true, SeqCst) {
            return;
        }
        if let Some(p) = self.inner.durable.as_ref() {
            p.join_threads();
            if let Err(e) = self.save() {
                p.set_error(format!("final save failed: {e}"));
            }
            if let Err(e) = p.wal.lock().close() {
                p.set_error(format!("WAL close failed: {e}"));
            }
            // Dropping the lock removes the LOCK file and frees the in-process registration.
            drop(p.lock.lock().take());
        }
    }

    /// Test hook that simulates SIGKILL. Stops background work and logging, without a final
    /// snapshot or sync. Data the OS already holds stays on disk. The LOCK file is left
    /// behind, as after a crash, and the next `open` takes it over.
    #[doc(hidden)]
    pub fn abandon(&self) {
        self.inner.closed.store(true, SeqCst);
        if let Some(p) = self.inner.durable.as_ref() {
            p.join_threads();
            p.wal.lock().abandon();
            if let Some(lock) = p.lock.lock().as_ref() {
                lock.abandon();
            }
        }
    }

    /// Test hook: runs `save` but returns early at `step`, as if the process died there.
    #[doc(hidden)]
    pub fn save_stopping_after(&self, step: SaveStep) -> io::Result<()> {
        self.save_inner(Some(step))
    }

    fn save_inner(&self, stop: Option<SaveStep>) -> io::Result<()> {
        let p = self.inner.durable.as_ref().ok_or_else(not_enabled)?;
        let _serial = p.save_serial.lock();
        let result = self.save_locked(p, stop);
        match &result {
            Ok(()) if stop.is_none() => p.meta.lock().last_error = None,
            Err(e) => p.set_error(format!("save failed: {e}")),
            Ok(()) => {}
        }
        result
    }

    fn save_locked(&self, p: &Persist, stop: Option<SaveStep>) -> io::Result<()> {
        let inner: &Inner = &self.inner;
        // Read-lock every shard, then rotate. Writers take a shard lock before they touch the
        // log, so none can append between the rotation and the snapshot of its shard.
        let guards: Vec<_> = inner.shards.iter().map(|s| s.read()).collect();
        let gen = p.wal.lock().rotate(&p.dir)?;
        if stop == Some(SaveStep::AfterRotate) {
            return Ok(());
        }

        let tmp = p.dir.join(SNAP_TMP);
        let mut w = snapshot::SnapWriter::create(&tmp, gen)?;
        let now = inner.now();
        for shard in guards {
            for e in shard.iter() {
                if !e.is_expired(now) {
                    w.entry(e)?;
                }
            }
            // Each shard's guard is dropped at the end of its iteration.
        }
        let bytes = w.finish()?;
        if stop == Some(SaveStep::AfterTmpWrite) {
            return Ok(());
        }

        fs::rename(&tmp, p.dir.join(SNAP_FILE))?;
        sync_dir(&p.dir)?;
        if stop == Some(SaveStep::AfterRename) {
            return Ok(());
        }

        for g in wal::list(&p.dir)? {
            if g < gen {
                match fs::remove_file(wal::path(&p.dir, g)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e),
                }
            }
        }
        if stop == Some(SaveStep::AfterWalDelete) {
            return Ok(());
        }
        sync_dir(&p.dir)?;

        let mut m = p.meta.lock();
        m.last_save_ms = system_now_ms();
        m.snapshot_bytes = bytes;
        Ok(())
    }
}

/// Runs `work` every `period` until the store closes or is dropped. Between ticks it holds
/// only a `Weak`, so it never keeps the store alive.
fn spawn_ticker(
    weak: Weak<Inner>,
    name: &str,
    period: Duration,
    work: impl Fn(&Store) + Send + 'static,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new().name(name.into()).spawn(move || {
        let mut since = Duration::ZERO;
        loop {
            thread::sleep(TICK);
            since += TICK;
            let Some(inner) = weak.upgrade() else {
                break;
            };
            if inner.closed.load(SeqCst) {
                break;
            }
            if since >= period {
                since = Duration::ZERO;
                work(&Store { inner });
            }
        }
    })
}

/// Result of recovering a data directory.
struct Recovered {
    /// Generation the store appends to.
    active_gen: u64,
    snapshot_bytes: u64,
    /// Set when a torn WAL tail was cut off.
    warning: Option<String>,
}

fn recover(dir: &Path, store: &Store) -> io::Result<Recovered> {
    // A temporary snapshot is an unfinished save. It was never renamed into place, so the
    // logs still hold everything it would have contained.
    let _ = fs::remove_file(dir.join(SNAP_TMP));

    let snap = dir.join(SNAP_FILE);
    let (snap_gen, snapshot_bytes) = if snap.exists() {
        let len = fs::metadata(&snap)?.len();
        let gen = snapshot::load(&snap, |rec| apply_snapshot_record(store, rec))?;
        (gen, len)
    } else {
        (0, 0)
    };

    let mut active = snap_gen;
    let mut warning = None;
    for g in wal::list(dir)? {
        let path = wal::path(dir, g);
        if g < snap_gen {
            // Superseded: the snapshot already contains everything in this file.
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            continue;
        }
        let res = wal::replay(&path, |rec| apply_wal_record(store, rec))?;
        if res.torn {
            wal::truncate(&path, res.valid_len)?;
            warning = Some(format!(
                "truncated torn WAL tail in wal-{g}.log at byte {}",
                res.valid_len
            ));
        }
        active = active.max(g);
    }
    Ok(Recovered {
        active_gen: active,
        snapshot_bytes,
        warning,
    })
}

fn apply_snapshot_record(store: &Store, rec: Record) {
    match rec {
        Record::Str {
            key,
            value,
            expires_at,
        } => store.apply_set(&key, &value, expires_at),
        Record::List {
            key,
            items,
            expires_at,
        } => {
            let bytes = items.iter().map(|i| elem_size(i.len())).sum();
            let data = Collection::List(items.into());
            store.apply_coll(&key, data, bytes, expires_at);
        }
        Record::Hash {
            key,
            pairs,
            expires_at,
        } => {
            let bytes = pairs.iter().map(|(f, v)| pair_size(f.len(), v.len())).sum();
            let map: Map<Vec<u8>, Vec<u8>> = pairs.into_iter().collect();
            store.apply_coll(&key, Collection::Hash(map), bytes, expires_at);
        }
        Record::Set {
            key,
            members,
            expires_at,
        } => {
            let bytes = members.iter().map(|m| elem_size(m.len())).sum();
            let set: Set = members.into_iter().collect();
            store.apply_coll(&key, Collection::Set(set), bytes, expires_at);
        }
    }
}

/// Re-applies one logged mutation through the store's own methods, which are deterministic
/// given the state they run on.
fn apply_wal_record(store: &Store, rec: &[u8]) -> io::Result<()> {
    let mut r = Rd::new(rec);
    let op = need(r.u8())?;
    // A logged command succeeded when it ran, so a replay error means the state already
    // matches and nothing needs to change. Errors are therefore ignored.
    match op {
        OP_SET => {
            let exp = need(r.u64())?;
            let k = need(r.bytes())?;
            store.apply_set(k, r.rest(), exp);
        }
        OP_DEL => {
            let k = need(r.bytes())?;
            store.apply_del(k);
        }
        OP_EXPIRE => {
            let exp = need(r.u64())?;
            let k = need(r.bytes())?;
            store.apply_expire(k, exp);
        }
        OP_HSET => {
            let k = need(r.bytes())?;
            let f = need(r.bytes())?;
            let _ = store.hset(k, f, r.rest().to_vec());
        }
        OP_HDEL => {
            let k = need(r.bytes())?;
            let fields = need(r.list())?;
            let _ = store.hdel(k, &fields);
        }
        OP_LPUSH | OP_RPUSH => {
            let k = need(r.bytes())?;
            let items = owned(need(r.list())?);
            let _ = if op == OP_LPUSH {
                store.lpush(k, items)
            } else {
                store.rpush(k, items)
            };
        }
        OP_LPOP | OP_RPOP => {
            let k = need(r.bytes())?;
            let _ = if op == OP_LPOP {
                store.lpop(k)
            } else {
                store.rpop(k)
            };
        }
        OP_SADD => {
            let k = need(r.bytes())?;
            let members = owned(need(r.list())?);
            let _ = store.sadd(k, members);
        }
        OP_SREM => {
            let k = need(r.bytes())?;
            let members = need(r.list())?;
            let _ = store.srem(k, &members);
        }
        OP_FLUSH => store.flush(),
        _ => return Err(corrupt("unknown operation")),
    }
    Ok(())
}

fn owned(items: Vec<&[u8]>) -> Vec<Vec<u8>> {
    items.into_iter().map(<[u8]>::to_vec).collect()
}

impl Store {
    /// Raw string insert with an absolute expiry. Skips TTL arithmetic and the memory limit.
    pub(crate) fn apply_set(&self, k: &[u8], v: &[u8], expires_at: u64) {
        let inner = &*self.inner;
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        let entry = inner.new_entry(k, v, expires_at);
        inner.put_locked(&mut shard, idx, h, k, entry);
    }

    /// Raw collection insert with an absolute expiry and its precomputed element bytes.
    pub(crate) fn apply_coll(&self, k: &[u8], data: Collection, bytes: usize, expires_at: u64) {
        let inner = &*self.inner;
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        let mut entry = Entry::new_coll(k, data);
        if let Some(c) = entry.coll_mut() {
            c.bytes = bytes;
        }
        entry.expires_at = expires_at;
        inner.put_locked(&mut shard, idx, h, k, entry);
    }

    pub(crate) fn apply_expire(&self, k: &[u8], expires_at: u64) {
        let inner = &*self.inner;
        let (_, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        if let Some(e) = shard.find_mut(h, |e| e.key() == k) {
            e.expires_at = expires_at;
        }
    }

    pub(crate) fn apply_del(&self, k: &[u8]) {
        let inner = &*self.inner;
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.remove_hashed(&mut shard, idx, h, k);
    }
}

impl Inner {
    /// Removes every expired key, logging each removal. Used when a store is opened.
    pub(crate) fn purge_expired(&self) {
        let now = self.now();
        for (idx, lock) in self.shards.iter().enumerate() {
            let mut shard = lock.write();
            let dead: Vec<Vec<u8>> = shard
                .iter()
                .filter(|e| e.is_expired(now))
                .map(|e| e.key().to_vec())
                .collect();
            for k in &dead {
                self.remove_locked(&mut shard, idx, k);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persist_config_is_comparable() {
        let a = PersistConfig {
            dir: PathBuf::from("/x"),
            fsync: FsyncPolicy::EverySec,
            snapshot_interval_secs: 0,
        };
        assert_eq!(a.clone(), a);
        assert_ne!(FsyncPolicy::Always, FsyncPolicy::Never);
    }
}
