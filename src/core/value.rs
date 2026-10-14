//! Stored values and per-key entries.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicU32;

pub(crate) type Map<K, V> = HashMap<K, V, ahash::RandomState>;
pub(crate) type Set = HashSet<Vec<u8>, ahash::RandomState>;
pub(crate) type ShardMap = Map<Box<[u8]>, Entry>;

/// Approximate bookkeeping cost of one key (map slot plus entry header).
pub(crate) const ENTRY_OVERHEAD: usize = 64;
/// Approximate bookkeeping cost of one collection element.
pub(crate) const ELEM_OVERHEAD: usize = 24;

pub(crate) enum Value {
    Bytes(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(Map<Vec<u8>, Vec<u8>>),
    Set(Set),
}

impl Value {
    /// Approximate heap footprint of the value, excluding the key and entry header.
    pub(crate) fn approx_size(&self) -> usize {
        match self {
            Value::Bytes(b) => b.len(),
            Value::List(l) => l.iter().map(|e| e.len() + ELEM_OVERHEAD).sum(),
            Value::Hash(h) => h
                .iter()
                .map(|(f, v)| f.len() + v.len() + ELEM_OVERHEAD)
                .sum(),
            Value::Set(s) => s.iter().map(|m| m.len() + ELEM_OVERHEAD).sum(),
        }
    }
}

pub(crate) struct Entry {
    pub(crate) value: Value,
    /// Absolute expiry in ms since epoch (clock domain of the store).
    pub(crate) expires_at: Option<u64>,
    /// Logical access stamp; larger is more recent, compared with wrapping arithmetic.
    pub(crate) last_access: AtomicU32,
}

impl Entry {
    pub(crate) fn new(value: Value, expires_at: Option<u64>) -> Self {
        Entry {
            value,
            expires_at,
            last_access: AtomicU32::new(0),
        }
    }

    pub(crate) fn is_expired(&self, now: u64) -> bool {
        self.expires_at.is_some_and(|t| t <= now)
    }
}

/// Approximate total bytes charged for a key/entry pair.
pub(crate) fn entry_size(key: &[u8], e: &Entry) -> usize {
    ENTRY_OVERHEAD + key.len() + e.value.approx_size()
}
