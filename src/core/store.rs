//! Sharded key-value store and the Redis-style command surface.

use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;

use super::expiry;
use super::value::{
    alloc_size, coll_base, deadline, elem_size, pair_size, str_entry_size, table_bytes,
    table_growth, Coll, Collection, Entry, Map, Set, ShardMap,
};

/// Tables smaller than this are never shrunk; the saving would be negligible.
const MIN_SHRINK_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Policy {
    NoEviction,
    AllKeysLru,
    VolatileLru,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub shards: usize,
    /// Approximate byte budget; 0 means unlimited.
    pub max_memory: usize,
    pub policy: Policy,
    /// Sweeper period in ms; 0 disables the background thread.
    pub sweep_interval_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            shards: 64,
            max_memory: 0,
            policy: Policy::NoEviction,
            sweep_interval_ms: 100,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreError {
    WrongType,
    Oom,
    NotInteger,
    Overflow,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let msg = match self {
            StoreError::WrongType => {
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            }
            StoreError::Oom => "OOM command not allowed when used memory > 'maxmemory'",
            StoreError::NotInteger => "value is not an integer or out of range",
            StoreError::Overflow => "increment or decrement would overflow",
        };
        f.write_str(msg)
    }
}

impl std::error::Error for StoreError {}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Clock returning milliseconds since the epoch.
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

/// Shared state behind every `Store` handle.
pub(crate) struct Inner {
    pub(crate) shards: Box<[RwLock<ShardMap>]>,
    /// Bytes currently charged to `used` for each shard's bucket array.
    table_charge: Box<[AtomicUsize]>,
    pub(crate) mask: usize,
    hasher: ahash::RandomState,
    pub(crate) max_memory: usize,
    pub(crate) policy: Policy,
    pub(crate) clock: Clock,
    pub(crate) used: AtomicUsize,
    pub(crate) tick: AtomicU32,
    pub(crate) rng: AtomicU64,
    pub(crate) closed: AtomicBool,
}

/// Cheaply cloneable handle to a sharded store. The sweeper thread stops once
/// `close()` is called or the last handle is dropped.
#[derive(Clone)]
pub struct Store {
    pub(crate) inner: Arc<Inner>,
}

fn system_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Typed view of a non-string entry, or `WrongType` for strings.
fn collection(e: &Entry) -> Result<&Collection> {
    e.coll().map(|c| &c.data).ok_or(StoreError::WrongType)
}

/// Mutable access to a collection entry's container and cached byte count.
fn coll_mut(e: &mut Entry) -> Result<&mut Coll> {
    e.coll_mut().ok_or(StoreError::WrongType)
}

fn str_value(e: &Entry) -> Result<&[u8]> {
    e.as_str().ok_or(StoreError::WrongType)
}

impl Store {
    pub fn new(cfg: Config) -> Store {
        Self::with_clock(cfg, Arc::new(system_now_ms))
    }

    pub fn with_clock(cfg: Config, clock: Clock) -> Store {
        let n = cfg.shards.max(1).next_power_of_two();
        let shards = (0..n)
            .map(|_| RwLock::new(ShardMap::default()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let table_charge = (0..n)
            .map(|_| AtomicUsize::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let inner = Arc::new(Inner {
            shards,
            table_charge,
            mask: n - 1,
            hasher: ahash::RandomState::with_seeds(
                0x243f_6a88_85a3_08d3,
                0x1319_8a2e_0370_7344,
                0xa409_3822_299f_31d0,
                0x082e_fa98_ec4e_6c89,
            ),
            max_memory: cfg.max_memory,
            policy: cfg.policy,
            clock,
            used: AtomicUsize::new(0),
            tick: AtomicU32::new(0),
            rng: AtomicU64::new(0x853c_49e6_748f_ea9b),
            closed: AtomicBool::new(false),
        });
        if cfg.sweep_interval_ms > 0 {
            expiry::spawn(
                Arc::downgrade(&inner),
                Duration::from_millis(cfg.sweep_interval_ms),
            );
        }
        Store { inner }
    }

    /// Stops the background sweeper. The store remains usable for reads and writes.
    pub fn close(&self) {
        self.inner.closed.store(true, Relaxed);
    }

    // ---- strings -------------------------------------------------------

    pub fn set(
        &self,
        k: &[u8],
        v: Vec<u8>,
        ttl_ms: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> Result<bool> {
        let inner = &self.inner;
        let now = inner.now();
        let (_, lock, _) = inner.locate(k);
        let new_size = str_entry_size(k.len(), v.len()) + inner.insert_growth(lock, k);
        let old_size = inner.live_size(k, now);
        if (nx && old_size.is_some()) || (xx && old_size.is_none()) {
            return Ok(false);
        }
        inner.reserve(new_size.saturating_sub(old_size.unwrap_or(0)))?;

        // The NX/XX condition is re-checked under the write lock so it stays atomic.
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        let exists = shard.find(h, |e| e.key() == k).is_some();
        if (nx && exists) || (xx && !exists) {
            return Ok(false);
        }
        let expires_at = ttl_ms.map_or(0, |t| deadline(now, t));
        let entry = inner.new_entry(k, &v, expires_at);
        inner.put_locked(&mut shard, idx, h, k, entry);
        Ok(true)
    }

    pub fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.read_key(k, |e| str_value(e).map(<[u8]>::to_vec))
    }

    pub fn mget(&self, ks: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        ks.iter().map(|&k| self.get(k).ok().flatten()).collect()
    }

    pub fn mset(&self, kv: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        // Not atomic: pairs are applied in order and an OOM stops the batch.
        for (k, v) in kv {
            self.set(&k, v, None, false, false)?;
        }
        Ok(())
    }

    pub fn incr_by(&self, k: &[u8], delta: i64) -> Result<i64> {
        let inner = &self.inner;
        let now = inner.now();
        // Longest i64 text is 20 bytes.
        let (_, lock, _) = inner.locate(k);
        let growth = (str_entry_size(k.len(), 20) + inner.insert_growth(lock, k))
            .saturating_sub(inner.live_size(k, now).unwrap_or(0));
        inner.reserve(growth)?;

        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        let (current, expires_at) = match shard.find(h, |e| e.key() == k) {
            None => (0, 0),
            Some(e) => (parse_i64(str_value(e)?)?, e.expires_at),
        };
        let next = current.checked_add(delta).ok_or(StoreError::Overflow)?;
        let entry = inner.new_entry(k, next.to_string().as_bytes(), expires_at);
        inner.put_locked(&mut shard, idx, h, k, entry);
        Ok(next)
    }

    pub fn append(&self, k: &[u8], v: &[u8]) -> Result<usize> {
        let inner = &self.inner;
        let now = inner.now();
        let (_, lock, _) = inner.locate(k);
        let growth = match inner.live_size(k, now) {
            Some(_) => v.len(),
            None => str_entry_size(k.len(), v.len()) + inner.insert_growth(lock, k),
        };
        inner.reserve(growth)?;

        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        if let Some(e) = shard.find_mut(h, |e| e.key() == k) {
            str_value(e)?;
            inner.stamp(e);
            let old = e.heap_size();
            e.append_str(v);
            let len = str_value(e)?.len();
            let new = e.heap_size();
            inner.resize(old, new);
            inner.settle(idx, &shard);
            Ok(len)
        } else {
            let entry = inner.new_entry(k, v, 0);
            inner.put_locked(&mut shard, idx, h, k, entry);
            Ok(v.len())
        }
    }

    // ---- keys ----------------------------------------------------------

    pub fn delete(&self, ks: &[&[u8]]) -> usize {
        let now = self.inner.now();
        let mut removed = 0;
        for &k in ks {
            let (idx, lock, h) = self.inner.locate(k);
            let mut shard = lock.write();
            self.inner.purge_locked(&mut shard, idx, h, k, now);
            if self.inner.remove_hashed(&mut shard, idx, h, k) {
                removed += 1;
            }
        }
        removed
    }

    pub fn exists(&self, k: &[u8]) -> bool {
        let now = self.inner.now();
        self.inner.live_size(k, now).is_some()
    }

    pub fn expire_ms(&self, k: &[u8], ttl_ms: u64) -> bool {
        let now = self.inner.now();
        let (idx, lock, h) = self.inner.locate(k);
        let mut shard = lock.write();
        self.inner.purge_locked(&mut shard, idx, h, k, now);
        match shard.find_mut(h, |e| e.key() == k) {
            Some(e) => {
                e.expires_at = deadline(now, ttl_ms);
                true
            }
            None => false,
        }
    }

    /// Removes the expiry from `k`. Returns true only if a TTL was actually removed.
    pub fn persist(&self, k: &[u8]) -> bool {
        let now = self.inner.now();
        let (idx, lock, h) = self.inner.locate(k);
        let mut shard = lock.write();
        self.inner.purge_locked(&mut shard, idx, h, k, now);
        match shard.find_mut(h, |e| e.key() == k) {
            Some(e) if e.expires_at != 0 => {
                e.expires_at = 0;
                true
            }
            _ => false,
        }
    }

    /// Remaining time to live in ms: -2 if missing, -1 if the key has no expiry.
    pub fn pttl(&self, k: &[u8]) -> i64 {
        let now = self.inner.now();
        let (_, lock, h) = self.inner.locate(k);
        let shard = lock.read();
        match shard.find(h, |e| e.key() == k) {
            None => -2,
            Some(e) if e.is_expired(now) => -2,
            Some(e) if e.expires_at == 0 => -1,
            Some(e) => i64::try_from(e.expires_at - now).unwrap_or(i64::MAX),
        }
    }

    /// Live keys matching `glob` (supports `*`, `?`, `[abc]`, `[^a-z]`, and `\x` escapes).
    pub fn keys(&self, glob: &str) -> Vec<Vec<u8>> {
        let now = self.inner.now();
        let pattern = glob.as_bytes();
        let mut out = Vec::new();
        for lock in self.inner.shards.iter() {
            let shard = lock.read();
            out.extend(
                shard
                    .iter()
                    .filter(|e| !e.is_expired(now) && glob_match(pattern, e.key()))
                    .map(|e| e.key().to_vec()),
            );
        }
        out
    }

    pub fn dbsize(&self) -> usize {
        let now = self.inner.now();
        self.inner
            .shards
            .iter()
            .map(|lock| lock.read().iter().filter(|e| !e.is_expired(now)).count())
            .sum()
    }

    /// Removes every key. Each shard's table is swapped for an empty one and the old
    /// table is freed outside the lock, so the memory goes back to the allocator.
    pub fn flush(&self) {
        for (idx, lock) in self.inner.shards.iter().enumerate() {
            let old = {
                let mut shard = lock.write();
                let heap: usize = shard.iter().map(Entry::heap_size).sum();
                self.inner.refund(heap);
                let old = std::mem::take(&mut *shard);
                self.inner.settle(idx, &shard);
                old
            };
            // Free the old bucket array and entries outside the lock.
            drop(old);
        }
    }

    pub fn used_memory(&self) -> usize {
        self.inner.used.load(Relaxed)
    }

    // ---- hashes --------------------------------------------------------

    /// Returns true if the field was newly created.
    pub fn hset(&self, k: &[u8], f: &[u8], v: Vec<u8>) -> Result<bool> {
        let inner = &self.inner;
        let (_, lock, _) = inner.locate(k);
        inner.reserve(
            coll_base(k.len()) + pair_size(f.len(), v.len()) + inner.insert_growth(lock, k),
        )?;

        let now = inner.now();
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        let e = inner.coll_entry(&mut shard, h, k, || Collection::Hash(Map::default()));
        inner.stamp(e);
        let coll = coll_mut(e)?;
        let Collection::Hash(map) = &mut coll.data else {
            return Err(StoreError::WrongType);
        };
        let added = if let Some(old) = map.get_mut(f) {
            let (old_alloc, new_alloc) = (alloc_size(old.len()), alloc_size(v.len()));
            *old = v;
            inner.coll_resize(&mut coll.bytes, old_alloc, new_alloc);
            false
        } else {
            inner.coll_resize(&mut coll.bytes, 0, pair_size(f.len(), v.len()));
            map.insert(f.to_vec(), v);
            true
        };
        inner.settle(idx, &shard);
        Ok(added)
    }

    pub fn hget(&self, k: &[u8], f: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.read_with(k, |e| match collection(e)? {
            Collection::Hash(h) => Ok(h.get(f).cloned()),
            _ => Err(StoreError::WrongType),
        })
    }

    pub fn hdel(&self, k: &[u8], fields: &[&[u8]]) -> Result<usize> {
        let inner = &self.inner;
        let now = inner.now();
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        let Some(e) = shard.find_mut(h, |e| e.key() == k) else {
            return Ok(0);
        };
        inner.stamp(e);
        let coll = coll_mut(e)?;
        let Collection::Hash(map) = &mut coll.data else {
            return Err(StoreError::WrongType);
        };
        let mut removed = 0;
        for f in fields {
            if let Some((fk, v)) = map.remove_entry(*f) {
                inner.coll_resize(&mut coll.bytes, pair_size(fk.len(), v.len()), 0);
                removed += 1;
            }
        }
        inner.drop_if_empty(&mut shard, idx, h, k);
        Ok(removed)
    }

    pub fn hgetall(&self, k: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner.read_with(k, |e| match collection(e)? {
            Collection::Hash(h) => Ok(h.iter().map(|(f, v)| (f.clone(), v.clone())).collect()),
            _ => Err(StoreError::WrongType),
        })
    }

    pub fn hlen(&self, k: &[u8]) -> Result<usize> {
        self.inner.read_with(k, |e| match collection(e)? {
            Collection::Hash(h) => Ok(h.len()),
            _ => Err(StoreError::WrongType),
        })
    }

    // ---- lists ---------------------------------------------------------

    /// Pushes each value to the head in argument order; returns the new length.
    pub fn lpush(&self, k: &[u8], vs: Vec<Vec<u8>>) -> Result<usize> {
        self.push(k, vs, true)
    }

    pub fn rpush(&self, k: &[u8], vs: Vec<Vec<u8>>) -> Result<usize> {
        self.push(k, vs, false)
    }

    fn push(&self, k: &[u8], vs: Vec<Vec<u8>>, front: bool) -> Result<usize> {
        let inner = &self.inner;
        let (_, lock, _) = inner.locate(k);
        let growth = coll_base(k.len())
            + vs.iter().map(|v| elem_size(v.len())).sum::<usize>()
            + inner.insert_growth(lock, k);
        inner.reserve(growth)?;

        let now = inner.now();
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        let e = inner.coll_entry(&mut shard, h, k, || Collection::List(VecDeque::new()));
        inner.stamp(e);
        let coll = coll_mut(e)?;
        let Collection::List(list) = &mut coll.data else {
            return Err(StoreError::WrongType);
        };
        for v in vs {
            inner.coll_resize(&mut coll.bytes, 0, elem_size(v.len()));
            if front {
                list.push_front(v);
            } else {
                list.push_back(v);
            }
        }
        let len = list.len();
        inner.drop_if_empty(&mut shard, idx, h, k);
        Ok(len)
    }

    pub fn lpop(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        self.pop(k, true)
    }

    pub fn rpop(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        self.pop(k, false)
    }

    fn pop(&self, k: &[u8], front: bool) -> Result<Option<Vec<u8>>> {
        let inner = &self.inner;
        let now = inner.now();
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        let Some(e) = shard.find_mut(h, |e| e.key() == k) else {
            return Ok(None);
        };
        inner.stamp(e);
        let coll = coll_mut(e)?;
        let Collection::List(list) = &mut coll.data else {
            return Err(StoreError::WrongType);
        };
        let popped = if front {
            list.pop_front()
        } else {
            list.pop_back()
        };
        if let Some(v) = &popped {
            inner.coll_resize(&mut coll.bytes, elem_size(v.len()), 0);
        }
        inner.drop_if_empty(&mut shard, idx, h, k);
        Ok(popped)
    }

    /// Inclusive range with Redis semantics: negative indexes count from the tail,
    /// out-of-range bounds are clamped, and an empty result is returned when start > stop.
    pub fn lrange(&self, k: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        self.inner.read_with(k, |e| match collection(e)? {
            Collection::List(l) => Ok(match list_bounds(l.len(), start, stop) {
                Some((a, b)) => l.range(a..=b).cloned().collect(),
                None => Vec::new(),
            }),
            _ => Err(StoreError::WrongType),
        })
    }

    pub fn llen(&self, k: &[u8]) -> Result<usize> {
        self.inner.read_with(k, |e| match collection(e)? {
            Collection::List(l) => Ok(l.len()),
            _ => Err(StoreError::WrongType),
        })
    }

    // ---- sets ----------------------------------------------------------

    /// Returns the number of members newly added.
    pub fn sadd(&self, k: &[u8], ms: Vec<Vec<u8>>) -> Result<usize> {
        let inner = &self.inner;
        let (_, lock, _) = inner.locate(k);
        let growth = coll_base(k.len())
            + ms.iter().map(|m| elem_size(m.len())).sum::<usize>()
            + inner.insert_growth(lock, k);
        inner.reserve(growth)?;

        let now = inner.now();
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        let e = inner.coll_entry(&mut shard, h, k, || Collection::Set(Set::default()));
        inner.stamp(e);
        let coll = coll_mut(e)?;
        let Collection::Set(set) = &mut coll.data else {
            return Err(StoreError::WrongType);
        };
        let mut added = 0;
        for m in ms {
            let size = elem_size(m.len());
            if set.insert(m) {
                inner.coll_resize(&mut coll.bytes, 0, size);
                added += 1;
            }
        }
        inner.drop_if_empty(&mut shard, idx, h, k);
        Ok(added)
    }

    pub fn srem(&self, k: &[u8], ms: &[&[u8]]) -> Result<usize> {
        let inner = &self.inner;
        let now = inner.now();
        let (idx, lock, h) = inner.locate(k);
        let mut shard = lock.write();
        inner.purge_locked(&mut shard, idx, h, k, now);
        let Some(e) = shard.find_mut(h, |e| e.key() == k) else {
            return Ok(0);
        };
        inner.stamp(e);
        let coll = coll_mut(e)?;
        let Collection::Set(set) = &mut coll.data else {
            return Err(StoreError::WrongType);
        };
        let mut removed = 0;
        for m in ms {
            if set.remove(*m) {
                inner.coll_resize(&mut coll.bytes, elem_size(m.len()), 0);
                removed += 1;
            }
        }
        inner.drop_if_empty(&mut shard, idx, h, k);
        Ok(removed)
    }

    pub fn smembers(&self, k: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.inner.read_with(k, |e| match collection(e)? {
            Collection::Set(s) => Ok(s.iter().cloned().collect()),
            _ => Err(StoreError::WrongType),
        })
    }

    pub fn sismember(&self, k: &[u8], m: &[u8]) -> Result<bool> {
        self.inner.read_with(k, |e| match collection(e)? {
            Collection::Set(s) => Ok(s.contains(m)),
            _ => Err(StoreError::WrongType),
        })
    }

    pub fn scard(&self, k: &[u8]) -> Result<usize> {
        self.inner.read_with(k, |e| match collection(e)? {
            Collection::Set(s) => Ok(s.len()),
            _ => Err(StoreError::WrongType),
        })
    }
}

impl Inner {
    pub(crate) fn now(&self) -> u64 {
        (self.clock)()
    }

    /// Shard lock and full hash for `k`. The shard index is taken from hash bits 32 and up:
    /// hashbrown indexes slots with the low bits and tags with the top 7, so sharing those
    /// bits with the shard choice would leave every shard's table mostly empty.
    pub(crate) fn locate(&self, k: &[u8]) -> (usize, &RwLock<ShardMap>, u64) {
        let h = self.hasher.hash_one(k);
        let idx = ((h >> 32) as usize) & self.mask;
        (idx, &self.shards[idx], h)
    }

    pub(crate) fn charge(&self, n: usize) {
        self.used.fetch_add(n, Relaxed);
    }

    pub(crate) fn refund(&self, n: usize) {
        self.used.fetch_sub(n, Relaxed);
    }

    /// Replaces an `old`-byte charge with a `new`-byte one.
    pub(crate) fn resize(&self, old: usize, new: usize) {
        if new > old {
            self.charge(new - old);
        } else {
            self.refund(old - new);
        }
    }

    /// Like `resize`, also keeping a collection's cached element byte count in step.
    pub(crate) fn coll_resize(&self, bytes: &mut usize, old: usize, new: usize) {
        *bytes = (*bytes).saturating_sub(old) + new;
        self.resize(old, new);
    }

    /// Records an access so LRU eviction can order keys. Skipped when no budget is set.
    pub(crate) fn stamp(&self, e: &Entry) {
        if self.max_memory > 0 {
            e.last_access
                .store(self.tick.fetch_add(1, Relaxed), Relaxed);
        }
    }

    pub(crate) fn new_entry(&self, k: &[u8], v: &[u8], expires_at: u64) -> Entry {
        let e = Entry::new_str(k, v, expires_at);
        self.stamp(&e);
        e
    }

    /// Size of the live (unexpired) entry for `k`, if any. Takes only a read lock.
    pub(crate) fn live_size(&self, k: &[u8], now: u64) -> Option<usize> {
        let (_, lock, h) = self.locate(k);
        let shard = lock.read();
        shard
            .find(h, |e| e.key() == k)
            .filter(|e| !e.is_expired(now))
            .map(Entry::heap_size)
    }

    /// Removes `k` if it is expired. Caller holds the shard's write lock.
    pub(crate) fn purge_locked(
        &self,
        shard: &mut ShardMap,
        idx: usize,
        h: u64,
        k: &[u8],
        now: u64,
    ) {
        if shard
            .find(h, |e| e.key() == k)
            .is_some_and(|e| e.is_expired(now))
        {
            self.remove_hashed(shard, idx, h, k);
        }
    }

    /// Removes `k` (with precomputed hash `h`) and refunds its memory.
    pub(crate) fn remove_hashed(&self, shard: &mut ShardMap, idx: usize, h: u64, k: &[u8]) -> bool {
        let removed = match shard.find_entry(h, |e| e.key() == k) {
            Ok(occupied) => {
                let (e, _) = occupied.remove();
                self.refund(e.heap_size());
                true
            }
            Err(_) => false,
        };
        if removed {
            self.maybe_shrink(shard, idx);
        }
        removed
    }

    /// Removes `k` and refunds its memory. Returns whether it was present.
    pub(crate) fn remove_locked(&self, shard: &mut ShardMap, idx: usize, k: &[u8]) -> bool {
        let h = self.hasher.hash_one(k);
        self.remove_hashed(shard, idx, h, k)
    }

    /// Inserts or replaces `k`, keeping the memory counter exact.
    pub(crate) fn put_locked(&self, shard: &mut ShardMap, idx: usize, h: u64, k: &[u8], e: Entry) {
        let size = e.heap_size();
        if let Some(old) = shard.find_mut(h, |o| o.key() == k) {
            let old_size = old.heap_size();
            *old = e;
            self.refund(old_size);
        } else {
            shard.insert_unique(h, e, |o| self.hasher.hash_one(o.key()));
        }
        self.charge(size);
        self.settle(idx, shard);
    }

    /// Enforces the Redis rule that empty collections do not exist.
    pub(crate) fn drop_if_empty(&self, shard: &mut ShardMap, idx: usize, h: u64, k: &[u8]) {
        let empty = shard
            .find(h, |e| e.key() == k)
            .and_then(|e| e.coll())
            .is_some_and(|c| c.data.is_empty());
        if empty {
            self.remove_hashed(shard, idx, h, k);
        }
        self.settle(idx, shard);
    }

    /// Returns the entry for `k`, inserting an empty collection first if it is missing.
    /// The caller must have purged expired keys and must check the entry's type.
    pub(crate) fn coll_entry<'a>(
        &self,
        shard: &'a mut ShardMap,
        h: u64,
        k: &[u8],
        make: impl FnOnce() -> Collection,
    ) -> &'a mut Entry {
        let entry = shard
            .entry(h, |e| e.key() == k, |e| self.hasher.hash_one(e.key()))
            .or_insert_with(|| {
                let e = Entry::new_coll(k, make());
                self.charge(e.heap_size());
                e
            });
        // The caller reports table growth through `settle` once the write is done.
        entry.into_mut()
    }

    /// Shrinks a table that has become mostly empty, so its buckets go back to the allocator.
    pub(crate) fn maybe_shrink(&self, shard: &mut ShardMap, idx: usize) {
        let cap = shard.capacity();
        // An empty table gives back its buckets regardless of size; otherwise only large
        // tables are worth the rehash.
        let worth = if shard.is_empty() {
            cap > 0
        } else {
            cap >= MIN_SHRINK_CAPACITY && shard.len() * 4 < cap
        };
        if worth {
            shard.shrink_to_fit(|e| self.hasher.hash_one(e.key()));
        }
        self.settle(idx, shard);
    }

    /// Extra table bytes that inserting `k` will add to its shard, zero if the table has room.
    /// Read under a shared lock; another writer may race, so this is an estimate.
    pub(crate) fn insert_growth(&self, lock: &RwLock<ShardMap>, k: &[u8]) -> usize {
        let shard = lock.read();
        if shard
            .find(self.hasher.hash_one(k), |e| e.key() == k)
            .is_some()
        {
            return 0;
        }
        table_growth(shard.len(), shard.capacity())
    }

    /// Brings shard `idx`'s charged table bytes in line with its real bucket array. Called
    /// with the shard's write lock held, after every operation that can grow or shrink it.
    pub(crate) fn settle(&self, idx: usize, shard: &ShardMap) {
        let now = table_bytes(shard.capacity());
        let was = self.table_charge[idx].swap(now, Relaxed);
        self.resize(was, now);
    }

    /// Runs `f` on the live entry for `k`. Expired keys are removed on the way.
    pub(crate) fn read_key<R>(
        &self,
        k: &[u8],
        f: impl FnOnce(&Entry) -> Result<R>,
    ) -> Result<Option<R>> {
        let now = self.now();
        let (idx, lock, h) = self.locate(k);
        {
            let shard = lock.read();
            match shard.find(h, |e| e.key() == k) {
                None => return Ok(None),
                Some(e) if !e.is_expired(now) => {
                    self.stamp(e);
                    return f(e).map(Some);
                }
                Some(_) => {}
            }
        }
        // Expired: retake the lock exclusively and remove it.
        self.purge_locked(&mut lock.write(), idx, h, k, now);
        Ok(None)
    }

    /// Typed read that yields `R::default()` for a missing key.
    pub(crate) fn read_with<R: Default>(
        &self,
        k: &[u8],
        f: impl FnOnce(&Entry) -> Result<R>,
    ) -> Result<R> {
        Ok(self.read_key(k, f)?.unwrap_or_default())
    }
}

fn parse_i64(b: &[u8]) -> Result<i64> {
    let digits = b.strip_prefix(b"-").unwrap_or(b);
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(StoreError::NotInteger);
    }
    std::str::from_utf8(b)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(StoreError::NotInteger)
}

fn list_bounds(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    if len == 0 {
        return None;
    }
    let len = len as i64;
    let s = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    let e = if stop < 0 {
        len + stop
    } else {
        stop.min(len - 1)
    };
    if s > e || s >= len {
        None
    } else {
        Some((s as usize, e as usize))
    }
}

/// Glob matching with `*`, `?`, `[...]` classes (ranges and `^` negation) and `\` escapes.
/// Unclosed `[` is treated as a literal.
pub(crate) fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    enum Tok {
        Star,
        Any,
        Lit(u8),
        Class { ranges: Vec<(u8, u8)>, negate: bool },
    }

    fn parse_class(p: &[u8], start: usize) -> Option<(Tok, usize)> {
        let mut i = start + 1;
        let negate = p.get(i) == Some(&b'^');
        if negate {
            i += 1;
        }
        let mut ranges = Vec::new();
        while i < p.len() {
            match p[i] {
                b']' => return Some((Tok::Class { ranges, negate }, i + 1)),
                b'\\' if i + 1 < p.len() => {
                    let c = p[i + 1];
                    ranges.push((c, c));
                    i += 2;
                }
                lo if i + 2 < p.len() && p[i + 1] == b'-' && p[i + 2] != b']' => {
                    let hi = p[i + 2];
                    ranges.push((lo.min(hi), lo.max(hi)));
                    i += 3;
                }
                c => {
                    ranges.push((c, c));
                    i += 1;
                }
            }
        }
        None
    }

    let mut toks = Vec::new();
    let mut i = 0;
    while i < pattern.len() {
        match pattern[i] {
            b'*' => {
                if !matches!(toks.last(), Some(Tok::Star)) {
                    toks.push(Tok::Star);
                }
                i += 1;
            }
            b'?' => {
                toks.push(Tok::Any);
                i += 1;
            }
            b'\\' if i + 1 < pattern.len() => {
                toks.push(Tok::Lit(pattern[i + 1]));
                i += 2;
            }
            b'[' => match parse_class(pattern, i) {
                Some((tok, next)) => {
                    toks.push(tok);
                    i = next;
                }
                None => {
                    toks.push(Tok::Lit(b'['));
                    i += 1;
                }
            },
            c => {
                toks.push(Tok::Lit(c));
                i += 1;
            }
        }
    }

    let matches_one = |t: &Tok, c: u8| match t {
        Tok::Star => false,
        Tok::Any => true,
        Tok::Lit(l) => *l == c,
        Tok::Class { ranges, negate } => {
            ranges.iter().any(|&(lo, hi)| (lo..=hi).contains(&c)) != *negate
        }
    };

    // Single-star backtracking: remember the last `*` and let it absorb one more byte on mismatch.
    let (mut ti, mut si) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while si < text.len() {
        match toks.get(ti) {
            Some(Tok::Star) => {
                star = Some((ti + 1, si));
                ti += 1;
                continue;
            }
            Some(t) if matches_one(t, text[si]) => {
                ti += 1;
                si += 1;
                continue;
            }
            _ => {}
        }
        let Some((next_ti, star_si)) = star else {
            return false;
        };
        star = Some((next_ti, star_si + 1));
        ti = next_ti;
        si = star_si + 1;
    }
    toks[ti..].iter().all(|t| matches!(t, Tok::Star))
}

#[cfg(test)]
mod tests {
    use super::super::value::{alloc_size, str_entry_size, table_bytes};
    use super::*;
    use std::thread;

    /// Shared, manually advanced clock for deterministic TTL tests.
    #[derive(Clone)]
    struct FakeClock(Arc<AtomicU64>);

    impl FakeClock {
        fn new(start: u64) -> Self {
            FakeClock(Arc::new(AtomicU64::new(start)))
        }

        fn advance(&self, ms: u64) {
            self.0.fetch_add(ms, Relaxed);
        }

        fn clock(&self) -> Clock {
            let c = self.0.clone();
            Arc::new(move || c.load(Relaxed))
        }
    }

    fn cfg() -> Config {
        Config {
            sweep_interval_ms: 0,
            ..Config::default()
        }
    }

    fn fake(c: Config) -> (Store, FakeClock) {
        let fc = FakeClock::new(1_000);
        (Store::with_clock(c, fc.clock()), fc)
    }

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn set(s: &Store, k: &str, v: &str) {
        assert!(s.set(k.as_bytes(), b(v), None, false, false).unwrap());
    }

    fn rpush(s: &Store, k: &str, items: &[&str]) {
        s.rpush(k.as_bytes(), items.iter().map(|i| b(i)).collect())
            .unwrap();
    }

    fn strs(v: Vec<Vec<u8>>) -> Vec<String> {
        v.into_iter()
            .map(|x| String::from_utf8(x).unwrap())
            .collect()
    }

    // ---- strings -------------------------------------------------------

    #[test]
    fn set_get_roundtrip() {
        let s = Store::new(cfg());
        assert_eq!(s.get(b"missing").unwrap(), None);
        set(&s, "k", "v");
        assert_eq!(s.get(b"k").unwrap(), Some(b("v")));
    }

    #[test]
    fn set_overwrites_value() {
        let s = Store::new(cfg());
        set(&s, "k", "one");
        set(&s, "k", "two");
        assert_eq!(s.get(b"k").unwrap(), Some(b("two")));
        assert_eq!(s.dbsize(), 1);
    }

    #[test]
    fn set_nx_only_when_absent() {
        let s = Store::new(cfg());
        assert!(s.set(b"k", b("a"), None, true, false).unwrap());
        assert!(!s.set(b"k", b("b"), None, true, false).unwrap());
        assert_eq!(s.get(b"k").unwrap(), Some(b("a")));
    }

    #[test]
    fn set_xx_only_when_present() {
        let s = Store::new(cfg());
        assert!(!s.set(b"k", b("a"), None, false, true).unwrap());
        assert_eq!(s.get(b"k").unwrap(), None);
        set(&s, "k", "old");
        assert!(s.set(b"k", b("new"), None, false, true).unwrap());
        assert_eq!(s.get(b"k").unwrap(), Some(b("new")));
    }

    #[test]
    fn set_with_ttl_expires() {
        let (s, clock) = fake(cfg());
        s.set(b"k", b("v"), Some(500), false, false).unwrap();
        clock.advance(499);
        assert_eq!(s.get(b"k").unwrap(), Some(b("v")));
        clock.advance(1);
        assert_eq!(s.get(b"k").unwrap(), None);
    }

    #[test]
    fn set_nx_succeeds_over_expired_key() {
        let (s, clock) = fake(cfg());
        s.set(b"k", b("old"), Some(10), false, false).unwrap();
        clock.advance(10);
        assert!(s.set(b"k", b("new"), None, true, false).unwrap());
        assert_eq!(s.get(b"k").unwrap(), Some(b("new")));
    }

    #[test]
    fn set_xx_fails_over_expired_key() {
        let (s, clock) = fake(cfg());
        s.set(b"k", b("old"), Some(10), false, false).unwrap();
        clock.advance(10);
        assert!(!s.set(b"k", b("new"), None, false, true).unwrap());
    }

    #[test]
    fn set_replaces_ttl_when_ttl_is_none() {
        let (s, clock) = fake(cfg());
        s.set(b"k", b("a"), Some(10), false, false).unwrap();
        s.set(b"k", b("b"), None, false, false).unwrap();
        clock.advance(1_000);
        assert_eq!(s.get(b"k").unwrap(), Some(b("b")));
        assert_eq!(s.pttl(b"k"), -1);
    }

    #[test]
    fn get_wrongtype_on_list() {
        let s = Store::new(cfg());
        rpush(&s, "l", &["x"]);
        assert_eq!(s.get(b"l"), Err(StoreError::WrongType));
    }

    #[test]
    fn mget_returns_none_for_missing_and_wrongtype() {
        let s = Store::new(cfg());
        set(&s, "a", "1");
        set(&s, "c", "3");
        rpush(&s, "l", &["x"]);
        let got = s.mget(&[b"a", b"b", b"l", b"c"]);
        assert_eq!(got, vec![Some(b("1")), None, None, Some(b("3"))]);
    }

    #[test]
    fn mset_sets_all_pairs() {
        let s = Store::new(cfg());
        s.mset(vec![(b("a"), b("1")), (b("b"), b("2"))]).unwrap();
        assert_eq!(s.get(b"a").unwrap(), Some(b("1")));
        assert_eq!(s.get(b"b").unwrap(), Some(b("2")));
        assert_eq!(s.dbsize(), 2);
    }

    #[test]
    fn incr_from_missing_and_by_delta() {
        let s = Store::new(cfg());
        assert_eq!(s.incr_by(b"n", 1).unwrap(), 1);
        assert_eq!(s.incr_by(b"n", 41).unwrap(), 42);
        assert_eq!(s.incr_by(b"n", -50).unwrap(), -8);
        assert_eq!(s.get(b"n").unwrap(), Some(b("-8")));
    }

    #[test]
    fn incr_keeps_ttl() {
        let (s, clock) = fake(cfg());
        s.set(b"n", b("1"), Some(100), false, false).unwrap();
        assert_eq!(s.incr_by(b"n", 1).unwrap(), 2);
        assert!(s.pttl(b"n") > 0);
        clock.advance(100);
        assert_eq!(s.incr_by(b"n", 1).unwrap(), 1);
    }

    #[test]
    fn incr_not_integer() {
        let s = Store::new(cfg());
        for bad in [
            "abc",
            "1.5",
            " 5",
            "5 ",
            "",
            "-",
            "+5",
            "9223372036854775808",
        ] {
            set(&s, "n", bad);
            assert_eq!(s.incr_by(b"n", 1), Err(StoreError::NotInteger), "{bad:?}");
        }
    }

    #[test]
    fn incr_overflow() {
        let s = Store::new(cfg());
        set(&s, "n", "9223372036854775807");
        assert_eq!(s.incr_by(b"n", 1), Err(StoreError::Overflow));
        assert_eq!(s.get(b"n").unwrap(), Some(b("9223372036854775807")));
        set(&s, "m", "-9223372036854775808");
        assert_eq!(s.incr_by(b"m", -1), Err(StoreError::Overflow));
        assert_eq!(s.incr_by(b"m", i64::MAX), Ok(-1));
    }

    #[test]
    fn incr_wrongtype() {
        let s = Store::new(cfg());
        rpush(&s, "l", &["x"]);
        assert_eq!(s.incr_by(b"l", 1), Err(StoreError::WrongType));
    }

    #[test]
    fn append_creates_and_extends() {
        let s = Store::new(cfg());
        assert_eq!(s.append(b"k", b"ab").unwrap(), 2);
        assert_eq!(s.append(b"k", b"cde").unwrap(), 5);
        assert_eq!(s.get(b"k").unwrap(), Some(b("abcde")));
    }

    #[test]
    fn append_wrongtype() {
        let s = Store::new(cfg());
        s.hset(b"h", b"f", b("v")).unwrap();
        assert_eq!(s.append(b"h", b"x"), Err(StoreError::WrongType));
    }

    // ---- keys ----------------------------------------------------------

    #[test]
    fn delete_counts_removed_keys() {
        let s = Store::new(cfg());
        set(&s, "a", "1");
        set(&s, "b", "2");
        assert_eq!(s.delete(&[b"a", b"b", b"zzz"]), 2);
        assert_eq!(s.dbsize(), 0);
        assert_eq!(s.delete(&[b"a"]), 0);
    }

    #[test]
    fn delete_expired_key_is_not_counted() {
        let (s, clock) = fake(cfg());
        s.set(b"k", b("v"), Some(5), false, false).unwrap();
        clock.advance(5);
        assert_eq!(s.delete(&[b"k"]), 0);
    }

    #[test]
    fn exists_respects_expiry() {
        let (s, clock) = fake(cfg());
        assert!(!s.exists(b"k"));
        s.set(b"k", b("v"), Some(5), false, false).unwrap();
        assert!(s.exists(b"k"));
        clock.advance(5);
        assert!(!s.exists(b"k"));
    }

    #[test]
    fn expire_ms_sets_and_reports_missing() {
        let (s, clock) = fake(cfg());
        assert!(!s.expire_ms(b"k", 100));
        set(&s, "k", "v");
        assert!(s.expire_ms(b"k", 100));
        assert_eq!(s.pttl(b"k"), 100);
        clock.advance(100);
        assert_eq!(s.get(b"k").unwrap(), None);
        assert!(!s.expire_ms(b"k", 100));
    }

    #[test]
    fn expire_ms_on_list_key() {
        let (s, clock) = fake(cfg());
        rpush(&s, "l", &["a"]);
        assert!(s.expire_ms(b"l", 50));
        clock.advance(50);
        assert_eq!(s.llen(b"l").unwrap(), 0);
        assert!(!s.exists(b"l"));
    }

    #[test]
    fn persist_removes_ttl() {
        let (s, clock) = fake(cfg());
        set(&s, "k", "v");
        assert!(!s.persist(b"k"), "no TTL to remove");
        s.expire_ms(b"k", 10);
        assert!(s.persist(b"k"));
        clock.advance(1_000);
        assert_eq!(s.get(b"k").unwrap(), Some(b("v")));
        assert!(!s.persist(b"missing"));
    }

    #[test]
    fn pttl_values() {
        let (s, clock) = fake(cfg());
        assert_eq!(s.pttl(b"missing"), -2);
        set(&s, "k", "v");
        assert_eq!(s.pttl(b"k"), -1);
        s.expire_ms(b"k", 300);
        clock.advance(100);
        assert_eq!(s.pttl(b"k"), 200);
        clock.advance(200);
        assert_eq!(s.pttl(b"k"), -2);
    }

    #[test]
    fn keys_filters_by_glob_and_skips_expired() {
        let (s, clock) = fake(cfg());
        set(&s, "user:1", "a");
        set(&s, "user:2", "b");
        set(&s, "item:1", "c");
        s.set(b"user:dead", b("x"), Some(1), false, false).unwrap();
        clock.advance(1);
        let mut got = strs(s.keys("user:*"));
        got.sort();
        assert_eq!(got, vec!["user:1", "user:2"]);
        assert_eq!(s.keys("*").len(), 3);
    }

    #[test]
    fn dbsize_excludes_expired() {
        let (s, clock) = fake(cfg());
        set(&s, "a", "1");
        s.set(b"b", b("2"), Some(10), false, false).unwrap();
        assert_eq!(s.dbsize(), 2);
        clock.advance(10);
        assert_eq!(s.dbsize(), 1);
    }

    #[test]
    fn flush_clears_everything_and_memory() {
        let s = Store::new(cfg());
        set(&s, "a", "1");
        rpush(&s, "l", &["x", "y"]);
        s.hset(b"h", b"f", b("v")).unwrap();
        s.sadd(b"s", vec![b("m")]).unwrap();
        assert!(s.used_memory() > 0);
        s.flush();
        assert_eq!(s.dbsize(), 0);
        assert_eq!(s.used_memory(), 0);
    }

    #[test]
    fn used_memory_tracks_insert_overwrite_and_delete() {
        let s = Store::new(cfg());
        assert_eq!(s.used_memory(), 0);
        s.set(b"k", vec![0; 100], None, false, false).unwrap();
        let first = s.used_memory();
        // Entry heap plus the bucket array of the shard it landed in.
        assert_eq!(first, entry_size_of(b"k", 100) + table_bytes(3));
        s.set(b"k", vec![0; 10], None, false, false).unwrap();
        assert_eq!(s.used_memory(), entry_size_of(b"k", 10) + table_bytes(3));
        s.delete(&[b"k"]);
        assert_eq!(s.used_memory(), 0, "emptied shard gives its table back");
    }

    fn entry_size_of(key: &[u8], value_len: usize) -> usize {
        str_entry_size(key.len(), value_len)
    }

    #[test]
    fn flush_returns_table_buckets_to_the_allocator() {
        let s = Store::new(cfg());
        for i in 0..5_000 {
            s.set(format!("k{i}").as_bytes(), vec![0; 64], None, false, false)
                .unwrap();
        }
        let filled = s.used_memory();
        let table_cap: usize = s.inner.shards.iter().map(|l| l.read().capacity()).sum();
        assert!(table_cap >= 5_000);
        s.flush();
        assert_eq!(s.used_memory(), 0);
        assert!(s.inner.shards.iter().all(|l| l.read().capacity() == 0));
        assert!(filled > 5_000 * 64);
    }

    #[test]
    fn mass_delete_shrinks_shard_tables() {
        let s = Store::new(cfg());
        for i in 0..20_000 {
            s.set(format!("k{i}").as_bytes(), vec![0; 8], None, false, false)
                .unwrap();
        }
        let full = s.used_memory();
        let keys: Vec<String> = (0..19_000).map(|i| format!("k{i}")).collect();
        for k in &keys {
            s.delete(&[k.as_bytes()]);
        }
        assert_eq!(s.dbsize(), 1_000);
        // 1k live keys in 64 shards must not keep the tables sized for 20k.
        assert!(
            s.used_memory() * 3 < full,
            "tables kept: {} of {full}",
            s.used_memory()
        );
        // The charge matches what the tables hold right now.
        let heap: usize = s
            .inner
            .shards
            .iter()
            .map(|l| l.read().iter().map(Entry::heap_size).sum::<usize>())
            .sum();
        let tables: usize = s
            .inner
            .shards
            .iter()
            .map(|l| table_bytes(l.read().capacity()))
            .sum();
        assert_eq!(s.used_memory(), heap + tables);
    }

    #[test]
    fn accounting_is_exact_for_100k_string_keys() {
        // Deterministic size check: the charge equals the sum of per-entry heap blocks plus
        // the bucket arrays actually allocated. Every term is integer arithmetic on fixed
        // sizes, so this cannot flake. The probe in examples/mem_probe.rs checks it against RSS.
        let s = Store::new(cfg());
        for i in 0..100_000 {
            s.set(
                format!("k{i:07}").as_bytes(),
                vec![b'x'; 64],
                None,
                false,
                false,
            )
            .unwrap();
        }
        assert_eq!(s.dbsize(), 100_000);
        let heap: usize = s
            .inner
            .shards
            .iter()
            .map(|l| l.read().iter().map(Entry::heap_size).sum::<usize>())
            .sum();
        let tables: usize = s
            .inner
            .shards
            .iter()
            .map(|l| table_bytes(l.read().capacity()))
            .sum();
        assert_eq!(s.used_memory(), heap + tables);
        // Per key: a 72 B heap block (key and value, rounded) plus a 41 B bucket, at this
        // table size about 0.6 of the way between powers of two. Measured at 133 B/key. The
        // bound catches gross mis-accounting in either direction, not the last byte.
        let per_key = s.used_memory() / 100_000;
        assert!((110..=160).contains(&per_key), "per key {per_key}");
    }

    // ---- hashes --------------------------------------------------------

    #[test]
    fn hash_set_get_and_overwrite() {
        let s = Store::new(cfg());
        assert!(s.hset(b"h", b"f", b("1")).unwrap());
        assert!(!s.hset(b"h", b"f", b("2")).unwrap());
        assert_eq!(s.hget(b"h", b"f").unwrap(), Some(b("2")));
        assert_eq!(s.hget(b"h", b"nope").unwrap(), None);
        assert_eq!(s.hget(b"missing", b"f").unwrap(), None);
    }

    #[test]
    fn hash_hdel_hlen_hgetall() {
        let s = Store::new(cfg());
        s.hset(b"h", b"a", b("1")).unwrap();
        s.hset(b"h", b"b", b("2")).unwrap();
        s.hset(b"h", b"c", b("3")).unwrap();
        assert_eq!(s.hlen(b"h").unwrap(), 3);
        assert_eq!(s.hdel(b"h", &[b"a", b"zz"]).unwrap(), 1);
        assert_eq!(s.hlen(b"h").unwrap(), 2);
        let mut all = s.hgetall(b"h").unwrap();
        all.sort();
        assert_eq!(all, vec![(b("b"), b("2")), (b("c"), b("3"))]);
        assert_eq!(s.hdel(b"missing", &[b"a"]).unwrap(), 0);
    }

    #[test]
    fn hash_removed_when_empty() {
        let s = Store::new(cfg());
        s.hset(b"h", b"a", b("1")).unwrap();
        assert_eq!(s.hdel(b"h", &[b"a"]).unwrap(), 1);
        assert!(!s.exists(b"h"));
        assert_eq!(s.used_memory(), 0);
        assert_eq!(s.hlen(b"h").unwrap(), 0);
    }

    #[test]
    fn hash_wrongtype() {
        let s = Store::new(cfg());
        set(&s, "k", "v");
        assert_eq!(s.hset(b"k", b"f", b("1")), Err(StoreError::WrongType));
        assert_eq!(s.hget(b"k", b"f"), Err(StoreError::WrongType));
        assert_eq!(s.hdel(b"k", &[b"f"]), Err(StoreError::WrongType));
        assert_eq!(s.hgetall(b"k"), Err(StoreError::WrongType));
        assert_eq!(s.hlen(b"k"), Err(StoreError::WrongType));
    }

    #[test]
    fn hash_memory_accounting_is_exact() {
        let s = Store::new(cfg());
        s.hset(b"h", b"f", vec![0; 10]).unwrap();
        let with_one = s.used_memory();
        // 10 and 4 byte values both round up to one 16-byte allocation, so the charge is unchanged.
        s.hset(b"h", b"f", vec![0; 4]).unwrap();
        assert_eq!(s.used_memory(), with_one);
        // Growing past a granule is charged for the larger allocation.
        s.hset(b"h", b"f", vec![0; 40]).unwrap();
        assert_eq!(s.used_memory(), with_one + alloc_size(40) - alloc_size(10));
        s.hdel(b"h", &[b"f"]).unwrap();
        assert_eq!(s.used_memory(), 0);
    }

    // ---- lists ---------------------------------------------------------

    #[test]
    fn lpush_reverses_argument_order() {
        let s = Store::new(cfg());
        assert_eq!(s.lpush(b"l", vec![b("a"), b("b"), b("c")]).unwrap(), 3);
        assert_eq!(strs(s.lrange(b"l", 0, -1).unwrap()), vec!["c", "b", "a"]);
    }

    #[test]
    fn rpush_keeps_order_and_returns_len() {
        let s = Store::new(cfg());
        assert_eq!(s.rpush(b"l", vec![b("a"), b("b")]).unwrap(), 2);
        assert_eq!(s.rpush(b"l", vec![b("c")]).unwrap(), 3);
        assert_eq!(strs(s.lrange(b"l", 0, -1).unwrap()), vec!["a", "b", "c"]);
        assert_eq!(s.llen(b"l").unwrap(), 3);
    }

    #[test]
    fn pops_from_both_ends() {
        let s = Store::new(cfg());
        rpush(&s, "l", &["a", "b", "c"]);
        assert_eq!(s.lpop(b"l").unwrap(), Some(b("a")));
        assert_eq!(s.rpop(b"l").unwrap(), Some(b("c")));
        assert_eq!(s.llen(b"l").unwrap(), 1);
        assert_eq!(s.rpop(b"l").unwrap(), Some(b("b")));
        assert_eq!(s.rpop(b"l").unwrap(), None);
        assert_eq!(s.lpop(b"missing").unwrap(), None);
    }

    #[test]
    fn list_removed_when_emptied() {
        let s = Store::new(cfg());
        rpush(&s, "l", &["a"]);
        s.lpop(b"l").unwrap();
        assert!(!s.exists(b"l"));
        assert_eq!(s.used_memory(), 0);
    }

    #[test]
    fn lrange_redis_semantics() {
        let s = Store::new(cfg());
        rpush(&s, "l", &["1", "2", "3", "4", "5"]);
        let r = |a, z| strs(s.lrange(b"l", a, z).unwrap()).join("");
        assert_eq!(r(0, -1), "12345");
        assert_eq!(r(-2, -1), "45");
        assert_eq!(r(-100, 2), "123");
        assert_eq!(r(0, 100), "12345");
        assert_eq!(r(1, 1), "2");
        assert_eq!(r(-1, -1), "5");
        assert_eq!(r(3, 1), "");
        assert_eq!(r(10, 20), "");
        assert_eq!(r(0, -10), "");
        assert_eq!(r(-100, -50), "");
    }

    #[test]
    fn lrange_missing_key_is_empty() {
        let s = Store::new(cfg());
        assert!(s.lrange(b"nope", 0, -1).unwrap().is_empty());
    }

    #[test]
    fn list_wrongtype() {
        let s = Store::new(cfg());
        set(&s, "k", "v");
        assert_eq!(s.lpush(b"k", vec![b("x")]), Err(StoreError::WrongType));
        assert_eq!(s.rpush(b"k", vec![b("x")]), Err(StoreError::WrongType));
        assert_eq!(s.lpop(b"k"), Err(StoreError::WrongType));
        assert_eq!(s.rpop(b"k"), Err(StoreError::WrongType));
        assert_eq!(s.lrange(b"k", 0, -1), Err(StoreError::WrongType));
        assert_eq!(s.llen(b"k"), Err(StoreError::WrongType));
    }

    // ---- sets ----------------------------------------------------------

    #[test]
    fn set_add_remove_members() {
        let s = Store::new(cfg());
        assert_eq!(s.sadd(b"s", vec![b("a"), b("b"), b("a")]).unwrap(), 2);
        assert_eq!(s.sadd(b"s", vec![b("b"), b("c")]).unwrap(), 1);
        assert_eq!(s.scard(b"s").unwrap(), 3);
        assert!(s.sismember(b"s", b"a").unwrap());
        assert!(!s.sismember(b"s", b"z").unwrap());
        assert_eq!(s.srem(b"s", &[b"a", b"zz"]).unwrap(), 1);
        let mut m = strs(s.smembers(b"s").unwrap());
        m.sort();
        assert_eq!(m, vec!["b", "c"]);
    }

    #[test]
    fn set_removed_when_emptied() {
        let s = Store::new(cfg());
        s.sadd(b"s", vec![b("a")]).unwrap();
        assert_eq!(s.srem(b"s", &[b"a"]).unwrap(), 1);
        assert!(!s.exists(b"s"));
        assert_eq!(s.used_memory(), 0);
        assert_eq!(s.scard(b"s").unwrap(), 0);
        assert!(!s.sismember(b"s", b"a").unwrap());
        assert!(s.smembers(b"s").unwrap().is_empty());
        assert_eq!(s.srem(b"missing", &[b"a"]).unwrap(), 0);
    }

    #[test]
    fn set_wrongtype() {
        let s = Store::new(cfg());
        set(&s, "k", "v");
        assert_eq!(s.sadd(b"k", vec![b("m")]), Err(StoreError::WrongType));
        assert_eq!(s.srem(b"k", &[b"m"]), Err(StoreError::WrongType));
        assert_eq!(s.smembers(b"k"), Err(StoreError::WrongType));
        assert_eq!(s.sismember(b"k", b"m"), Err(StoreError::WrongType));
        assert_eq!(s.scard(b"k"), Err(StoreError::WrongType));
    }

    #[test]
    fn expired_collection_reads_as_missing_and_is_removed() {
        let (s, clock) = fake(cfg());
        s.sadd(b"s", vec![b("a")]).unwrap();
        s.expire_ms(b"s", 10);
        clock.advance(10);
        assert_eq!(s.scard(b"s").unwrap(), 0);
        assert_eq!(s.used_memory(), 0);
    }

    #[test]
    fn wrong_type_writes_do_not_change_memory() {
        let s = Store::new(cfg());
        set(&s, "k", "v");
        let before = s.used_memory();
        let _ = s.hset(b"k", b"f", vec![0; 100]);
        let _ = s.sadd(b"k", vec![vec![0; 100]]);
        assert_eq!(s.used_memory(), before);
    }

    // ---- TTL, expiry and sweeper ---------------------------------------

    #[test]
    fn sweeper_removes_expired_keys_without_access() {
        let c = Config {
            sweep_interval_ms: 5,
            ..Config::default()
        };
        let (s, clock) = fake(c);
        for i in 0..200 {
            s.set(
                format!("k{i}").as_bytes(),
                vec![0; 8],
                Some(10),
                false,
                false,
            )
            .unwrap();
        }
        set(&s, "live", "v");
        let before = s.used_memory();
        clock.advance(10);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while s.used_memory() >= before && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(s.used_memory() < before);
        assert_eq!(
            s.inner.shards.iter().map(|l| l.read().len()).sum::<usize>(),
            1
        );
        assert_eq!(s.get(b"live").unwrap(), Some(b("v")));
    }

    #[test]
    fn sweep_once_is_bounded_and_keeps_live_keys() {
        let (s, clock) = fake(cfg());
        for i in 0..50 {
            s.set(format!("d{i}").as_bytes(), vec![], Some(1), false, false)
                .unwrap();
        }
        set(&s, "live", "v");
        clock.advance(1);
        s.inner.sweep_once();
        assert_eq!(
            s.inner.shards.iter().map(|l| l.read().len()).sum::<usize>(),
            1
        );
        assert!(s.exists(b"live"));
    }

    #[test]
    fn sweeper_thread_stops_when_last_handle_dropped() {
        let c = Config {
            sweep_interval_ms: 1,
            ..Config::default()
        };
        let s = Store::new(c);
        let weak = Arc::downgrade(&s.inner);
        let s2 = s.clone();
        drop(s);
        assert!(weak.upgrade().is_some(), "clone keeps the store alive");
        drop(s2);
        thread::sleep(Duration::from_millis(20));
        assert!(
            weak.upgrade().is_none(),
            "sweeper must not hold a strong ref"
        );
    }

    #[test]
    fn close_keeps_store_usable() {
        let c = Config {
            sweep_interval_ms: 1,
            ..Config::default()
        };
        let s = Store::new(c);
        s.close();
        set(&s, "k", "v");
        assert_eq!(s.get(b"k").unwrap(), Some(b("v")));
    }

    // ---- eviction ------------------------------------------------------

    fn budgeted(policy: Policy, max: usize) -> Store {
        Store::new(Config {
            shards: 4,
            max_memory: max,
            policy,
            sweep_interval_ms: 0,
        })
    }

    #[test]
    fn noeviction_returns_oom_when_full() {
        let s = budgeted(Policy::NoEviction, 1_000);
        s.set(b"a", vec![0; 500], None, false, false).unwrap();
        assert_eq!(
            s.set(b"b", vec![0; 500], None, false, false),
            Err(StoreError::Oom)
        );
        assert!(!s.exists(b"b"));
        assert!(s.exists(b"a"));
        assert!(s.used_memory() <= 1_000);
    }

    #[test]
    fn noeviction_oom_on_each_write_kind() {
        // The budget leaves room for the 200-byte value and its shard's first bucket array.
        let s = budgeted(Policy::NoEviction, 300 + table_bytes(3));
        s.set(b"fill", vec![0; 200], None, false, false).unwrap();
        assert_eq!(s.incr_by(b"n", 1), Err(StoreError::Oom));
        assert_eq!(s.append(b"x", &[0; 500]), Err(StoreError::Oom));
        assert_eq!(s.lpush(b"l", vec![vec![0; 500]]), Err(StoreError::Oom));
        assert_eq!(s.hset(b"h", b"f", vec![0; 500]), Err(StoreError::Oom));
        assert_eq!(s.sadd(b"s", vec![vec![0; 500]]), Err(StoreError::Oom));
    }

    #[test]
    fn noeviction_allows_overwrite_that_fits() {
        let s = budgeted(Policy::NoEviction, 400);
        s.set(b"a", vec![0; 200], None, false, false).unwrap();
        s.set(b"a", vec![0; 100], None, false, false).unwrap();
        assert!(s.used_memory() <= 400);
    }

    #[test]
    fn allkeys_lru_evicts_to_stay_under_budget() {
        let max = 2_000;
        let s = budgeted(Policy::AllKeysLru, max);
        for i in 0..500 {
            s.set(format!("k{i}").as_bytes(), vec![0; 50], None, false, false)
                .expect("allkeys-lru never refuses");
            assert!(s.used_memory() <= max);
        }
        assert!(s.dbsize() < 500);
        assert!(s.dbsize() > 0);
    }

    #[test]
    fn allkeys_lru_keeps_recently_read_key() {
        let s = budgeted(Policy::AllKeysLru, 1_500);
        s.set(b"hot", vec![0; 50], None, false, false).unwrap();
        for i in 0..300 {
            s.get(b"hot").unwrap();
            s.set(
                format!("cold{i}").as_bytes(),
                vec![0; 50],
                None,
                false,
                false,
            )
            .unwrap();
        }
        assert_eq!(s.get(b"hot").unwrap(), Some(vec![0; 50]));
    }

    #[test]
    fn allkeys_lru_evicts_plain_keys_without_ttl() {
        let s = budgeted(Policy::AllKeysLru, 600);
        s.set(b"plain", vec![0; 100], None, false, false).unwrap();
        for i in 0..20 {
            s.set(
                format!("x{i}").as_bytes(),
                vec![0; 100],
                Some(60_000),
                false,
                false,
            )
            .unwrap();
        }
        assert!(s.used_memory() <= 600);
        assert!(
            !s.exists(b"plain"),
            "the oldest persistent key is the first victim"
        );
    }

    #[test]
    fn allkeys_lru_oom_when_single_value_exceeds_budget() {
        let s = budgeted(Policy::AllKeysLru, 100);
        assert_eq!(
            s.set(b"big", vec![0; 1_000], None, false, false),
            Err(StoreError::Oom)
        );
    }

    #[test]
    fn volatile_lru_never_evicts_persistent_keys() {
        let s = budgeted(Policy::VolatileLru, 1_000);
        s.set(b"forever", vec![0; 50], None, false, false).unwrap();
        for i in 0..200 {
            s.set(
                format!("t{i}").as_bytes(),
                vec![0; 50],
                Some(60_000),
                false,
                false,
            )
            .unwrap();
        }
        assert_eq!(s.get(b"forever").unwrap(), Some(vec![0; 50]));
        assert!(s.used_memory() <= 1_000);
    }

    #[test]
    fn volatile_lru_oom_without_volatile_keys() {
        // Two 150-byte values (176 B each after rounding) plus their table fit; a third does not.
        let s = budgeted(Policy::VolatileLru, 704);
        s.set(b"p1", vec![0; 150], None, false, false).unwrap();
        s.set(b"p2", vec![0; 150], None, false, false).unwrap();
        assert_eq!(
            s.set(b"p3", vec![0; 150], None, false, false),
            Err(StoreError::Oom)
        );
        assert!(s.exists(b"p1") && s.exists(b"p2"));
    }

    #[test]
    fn volatile_lru_reports_oom_when_only_persistent_keys_left() {
        let s = budgeted(Policy::VolatileLru, 500 + table_bytes(3));
        s.set(b"p", vec![0; 100], None, false, false).unwrap();
        s.set(b"v", vec![0; 10], Some(60_000), false, false)
            .unwrap();
        assert_eq!(
            s.set(b"huge", vec![0; 400], None, false, false),
            Err(StoreError::Oom)
        );
        assert!(s.exists(b"p"));
    }

    #[test]
    fn budget_zero_means_unlimited() {
        let s = Store::new(cfg());
        for i in 0..1_000 {
            s.set(
                format!("k{i}").as_bytes(),
                vec![0; 1_000],
                None,
                false,
                false,
            )
            .unwrap();
        }
        assert_eq!(s.dbsize(), 1_000);
    }

    // ---- glob ----------------------------------------------------------

    #[test]
    fn glob_literals_and_star() {
        assert!(glob_match(b"abc", b"abc"));
        assert!(!glob_match(b"abc", b"abd"));
        assert!(!glob_match(b"abc", b"ab"));
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"a*", b"a"));
        assert!(glob_match(b"a*c", b"abbbc"));
        assert!(glob_match(b"a**c", b"ac"));
        assert!(glob_match(b"*a*b*", b"xxaxxbxx"));
        assert!(!glob_match(b"*a*b*", b"xxbxxaxx"));
        assert!(glob_match(b"*:*", b"user:42"));
    }

    #[test]
    fn glob_question_mark() {
        assert!(glob_match(b"a?c", b"abc"));
        assert!(!glob_match(b"a?c", b"ac"));
        assert!(!glob_match(b"a?c", b"abbc"));
        assert!(glob_match(b"???", b"xyz"));
    }

    #[test]
    fn glob_classes_and_ranges() {
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
        assert!(glob_match(b"k[0-9]", b"k7"));
        assert!(!glob_match(b"k[0-9]", b"ka"));
        assert!(
            glob_match(b"k[9-0]", b"k5"),
            "reversed ranges are normalised"
        );
        assert!(glob_match(b"[^a]x", b"bx"));
        assert!(!glob_match(b"[^a]x", b"ax"));
    }

    #[test]
    fn glob_escapes() {
        assert!(glob_match(br"a\*c", b"a*c"));
        assert!(!glob_match(br"a\*c", b"abc"));
        assert!(glob_match(br"a\?", b"a?"));
        assert!(glob_match(br"\[x\]", b"[x]"));
        assert!(glob_match(br"[\]]", b"]"));
        assert!(glob_match(br"a\\b", br"a\b"));
    }

    #[test]
    fn glob_unclosed_bracket_is_literal() {
        assert!(glob_match(b"a[b", b"a[b"));
        assert!(!glob_match(b"a[b", b"ab"));
    }

    #[test]
    fn glob_empty_pattern() {
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"x"));
    }

    #[test]
    fn glob_backtracking_heavy() {
        assert!(!glob_match(b"*aaaaaaaaab", &b"a".repeat(5000)));
        let text = format!("{}b", "a".repeat(5000));
        assert!(glob_match(b"*a*a*b", text.as_bytes()));
    }

    // ---- commands through the public surface ---------------------------

    #[test]
    fn keys_glob_with_classes() {
        let s = Store::new(cfg());
        for k in ["a1", "a2", "b1", "ab"] {
            set(&s, k, "v");
        }
        let mut got = strs(s.keys("a[12]"));
        got.sort();
        assert_eq!(got, vec!["a1", "a2"]);
    }

    // ---- error text and concurrency ------------------------------------

    #[test]
    fn error_messages_are_redis_like() {
        assert!(StoreError::WrongType.to_string().starts_with("WRONGTYPE"));
        assert!(StoreError::Oom.to_string().starts_with("OOM"));
        assert!(StoreError::NotInteger.to_string().contains("integer"));
        assert!(StoreError::Overflow.to_string().contains("overflow"));
    }

    #[test]
    fn concurrent_incr_is_atomic() {
        let s = Store::new(cfg());
        thread::scope(|scope| {
            for _ in 0..8 {
                let s = s.clone();
                scope.spawn(move || {
                    for _ in 0..1_000 {
                        s.incr_by(b"counter", 1).unwrap();
                    }
                });
            }
        });
        assert_eq!(s.get(b"counter").unwrap(), Some(b("8000")));
    }

    #[test]
    fn eight_threads_mixed_workload_stays_consistent() {
        const THREADS: usize = 8;
        const OPS: usize = 2_000;
        let s = Store::new(Config {
            shards: 16,
            max_memory: 0,
            policy: Policy::NoEviction,
            sweep_interval_ms: 1,
        });
        thread::scope(|scope| {
            for t in 0..THREADS {
                let s = s.clone();
                scope.spawn(move || {
                    for n in 0..OPS {
                        let own = format!("t{t}:{n}");
                        s.set(own.as_bytes(), own.clone().into_bytes(), None, false, false)
                            .unwrap();
                        assert_eq!(
                            s.get(own.as_bytes()).unwrap(),
                            Some(own.clone().into_bytes())
                        );
                        s.rpush(format!("list:{t}").as_bytes(), vec![b("x")])
                            .unwrap();
                        s.sadd(b"shared-set", vec![format!("{t}").into_bytes()])
                            .unwrap();
                        s.hset(b"shared-hash", format!("{t}:{n}").as_bytes(), b("v"))
                            .unwrap();
                        s.incr_by(b"total", 1).unwrap();
                        if n.is_multiple_of(2) {
                            s.delete(&[own.as_bytes()]);
                        }
                    }
                    assert_eq!(s.llen(format!("list:{t}").as_bytes()).unwrap(), OPS);
                });
            }
        });
        assert_eq!(s.get(b"total").unwrap(), Some(b("16000")));
        assert_eq!(s.scard(b"shared-set").unwrap(), THREADS);
        assert_eq!(s.hlen(b"shared-hash").unwrap(), THREADS * OPS);
        // total, shared-set, shared-hash, one list per thread, and the odd-numbered own keys.
        assert_eq!(s.dbsize(), 3 + THREADS + THREADS * OPS / 2);
    }

    #[test]
    fn concurrent_eviction_respects_budget_roughly() {
        let s = Store::new(Config {
            shards: 8,
            max_memory: 50_000,
            policy: Policy::AllKeysLru,
            sweep_interval_ms: 0,
        });
        thread::scope(|scope| {
            for t in 0..8 {
                let s = s.clone();
                scope.spawn(move || {
                    for n in 0..1_000 {
                        s.set(
                            format!("{t}-{n}").as_bytes(),
                            vec![0; 40],
                            None,
                            false,
                            false,
                        )
                        .unwrap();
                    }
                });
            }
        });
        // Accounting is approximate under concurrency; allow modest overshoot.
        assert!(s.used_memory() <= 50_000 + 8 * 200);
        assert!(s.dbsize() > 0);
    }
}
