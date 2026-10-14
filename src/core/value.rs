//! Stored values and per-key entries.
//!
//! Layout (see PLAN.md 4c): a table slot is one [`Entry`] of 40 bytes. A string key and
//! its value share one heap buffer (`key ++ value`, with `key_len` marking the split).
//! Collections are boxed so they do not inflate the slot for the common string case.
//!
//! Accounting has two parts, both sized for the allocator's 16-byte granule:
//! - heap blocks owned by entries (`Entry::heap_size`);
//! - each shard's bucket array, charged from its real capacity after every write
//!   (`table_bytes`, settled by `Inner::settle`), so table growth and shrinkage are exact.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicU32;

pub(crate) type Map<K, V> = HashMap<K, V, ahash::RandomState>;
pub(crate) type Set = HashSet<Vec<u8>, ahash::RandomState>;
/// One shard: an open-addressing table whose slots are the entries themselves.
pub(crate) type ShardMap = hashbrown::HashTable<Entry>;

/// Bytes of one bucket: the 40-byte entry plus its control byte.
pub(crate) const BUCKET_BYTES: usize = std::mem::size_of::<Entry>() + 1;
/// Trailing control bytes hashbrown allocates after the buckets for group probing.
const CTRL_PAD: usize = 16;
/// Heap cost of a boxed collection beyond its elements: the `Coll` box, the collection
/// header and its element table. Calibrated with `examples/mem_probe.rs`.
pub(crate) const COLL_OVERHEAD: usize = 80;
/// Amortised cost of one collection element beyond its byte payload: container slot,
/// control byte and load-factor slack. Calibrated with `examples/mem_probe.rs`.
pub(crate) const ELEM_OVERHEAD: usize = 56;
/// The system allocator hands out blocks in 16-byte granules.
pub(crate) const ALLOC_GRANULE: usize = 16;

/// Bytes charged for one heap block of `n` payload bytes, including allocator rounding.
pub(crate) const fn alloc_size(n: usize) -> usize {
    (n + ALLOC_GRANULE - 1) & !(ALLOC_GRANULE - 1)
}

/// Heap bytes of a string entry with a key of `klen` bytes and a value of `vlen` bytes.
pub(crate) const fn str_entry_size(klen: usize, vlen: usize) -> usize {
    alloc_size(klen + vlen)
}

/// Heap bytes of a new, empty collection key of `klen` bytes, before any elements.
pub(crate) const fn coll_base(klen: usize) -> usize {
    COLL_OVERHEAD + alloc_size(klen)
}

/// Heap bytes of one list element or set member of `n` bytes.
pub(crate) const fn elem_size(n: usize) -> usize {
    alloc_size(n) + ELEM_OVERHEAD
}

/// Heap bytes of one hash field/value pair.
pub(crate) const fn pair_size(field: usize, value: usize) -> usize {
    alloc_size(field) + alloc_size(value) + ELEM_OVERHEAD
}

/// Absolute expiry for `ttl_ms` from `now`. Zero is reserved for "no expiry".
pub(crate) fn deadline(now: u64, ttl_ms: u64) -> u64 {
    now.saturating_add(ttl_ms).max(1)
}

/// Bucket count hashbrown allocates for a table whose usable capacity is `cap`.
const fn buckets_for(cap: usize) -> usize {
    if cap <= 3 {
        4
    } else {
        cap / 7 * 8
    }
}

/// Usable capacity hashbrown provides for `buckets` buckets.
const fn capacity_for(buckets: usize) -> usize {
    if buckets < 8 {
        buckets - 1
    } else {
        buckets / 8 * 7
    }
}

/// Bytes of the bucket array for a table with usable capacity `cap`. Zero when the table
/// has never allocated (capacity 0). Matches hashbrown's layout: buckets, then control bytes.
pub(crate) const fn table_bytes(cap: usize) -> usize {
    if cap == 0 {
        return 0;
    }
    alloc_size(buckets_for(cap) * BUCKET_BYTES + CTRL_PAD)
}

/// Bytes the next insert adds to a table with `len` items and usable capacity `cap`.
/// Nonzero only when the table is full, because hashbrown then reallocates to double.
pub(crate) const fn table_growth(len: usize, cap: usize) -> usize {
    if len < cap {
        return 0;
    }
    let next = if cap == 0 {
        capacity_for(4)
    } else {
        capacity_for(buckets_for(cap) * 2)
    };
    table_bytes(next) - table_bytes(cap)
}

pub(crate) enum Collection {
    List(VecDeque<Vec<u8>>),
    Hash(Map<Vec<u8>, Vec<u8>>),
    Set(Set),
}

impl Collection {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Collection::List(l) => l.is_empty(),
            Collection::Hash(h) => h.is_empty(),
            Collection::Set(s) => s.is_empty(),
        }
    }
}

/// A boxed collection together with its key and cached element footprint.
pub(crate) struct Coll {
    pub(crate) key: Box<[u8]>,
    pub(crate) data: Collection,
    /// Sum of `elem_size`/`pair_size` over all elements. Kept in step with every mutation
    /// so that removing the entry can refund it without walking the collection.
    pub(crate) bytes: usize,
}

pub(crate) enum Body {
    /// `key ++ value` for strings; the key is `buf[..key_len]`.
    Str(Vec<u8>),
    Coll(Box<Coll>),
}

pub(crate) struct Entry {
    body: Body,
    /// Length of the key prefix of a `Body::Str`. Zero for collections.
    key_len: u32,
    /// Absolute expiry in ms since epoch (store clock domain); 0 means no expiry.
    pub(crate) expires_at: u64,
    /// Logical access stamp; larger is more recent, compared with wrapping arithmetic.
    pub(crate) last_access: AtomicU32,
}

impl Entry {
    pub(crate) fn new_str(k: &[u8], v: &[u8], expires_at: u64) -> Self {
        let key_len = u32::try_from(k.len()).expect("key longer than 4 GiB");
        let mut buf = Vec::with_capacity(k.len() + v.len());
        buf.extend_from_slice(k);
        buf.extend_from_slice(v);
        Entry {
            body: Body::Str(buf),
            key_len,
            expires_at,
            last_access: AtomicU32::new(0),
        }
    }

    pub(crate) fn new_coll(k: &[u8], data: Collection) -> Self {
        Entry {
            body: Body::Coll(Box::new(Coll {
                key: k.into(),
                data,
                bytes: 0,
            })),
            key_len: 0,
            expires_at: 0,
            last_access: AtomicU32::new(0),
        }
    }

    pub(crate) fn key(&self) -> &[u8] {
        match &self.body {
            Body::Str(b) => &b[..self.key_len as usize],
            Body::Coll(c) => &c.key,
        }
    }

    /// The string value, or `None` if this entry holds a collection.
    pub(crate) fn as_str(&self) -> Option<&[u8]> {
        match &self.body {
            Body::Str(b) => Some(&b[self.key_len as usize..]),
            Body::Coll(_) => None,
        }
    }

    /// Appends to a string value. Returns false (and changes nothing) for collections.
    pub(crate) fn append_str(&mut self, v: &[u8]) -> bool {
        match &mut self.body {
            Body::Str(b) => {
                b.extend_from_slice(v);
                true
            }
            Body::Coll(_) => false,
        }
    }

    pub(crate) fn coll(&self) -> Option<&Coll> {
        match &self.body {
            Body::Coll(c) => Some(c),
            Body::Str(_) => None,
        }
    }

    pub(crate) fn coll_mut(&mut self) -> Option<&mut Coll> {
        match &mut self.body {
            Body::Coll(c) => Some(c),
            Body::Str(_) => None,
        }
    }

    pub(crate) fn is_expired(&self, now: u64) -> bool {
        self.expires_at != 0 && self.expires_at <= now
    }

    /// Heap bytes this entry owns, excluding its table slot (charged per table bucket).
    /// O(1): collection element bytes are cached on the `Coll`.
    pub(crate) fn heap_size(&self) -> usize {
        match &self.body {
            Body::Str(b) => alloc_size(b.capacity()),
            Body::Coll(c) => COLL_OVERHEAD + alloc_size(c.key.len()) + c.bytes,
        }
    }
}
