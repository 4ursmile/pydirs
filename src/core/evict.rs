//! Approximate memory budget enforcement and sampled-LRU eviction.
//!
//! Eviction samples a few keys from distinct shards and removes the least recently
//! used of them. Within a shard, the sample is a window of consecutive entries starting
//! at a random offset, so each eviction costs O(shard size) in the worst case.

use std::sync::atomic::Ordering::Relaxed;

use super::store::{Inner, Policy, Result, StoreError};
use super::value::{Entry, ShardMap};

/// Number of candidate keys considered per eviction.
const SAMPLE_SIZE: usize = 5;

impl Inner {
    /// Ensures `growth` more bytes fit under `max_memory`, evicting as the policy allows.
    /// Accounting is approximate under concurrency: a few writers may overshoot slightly.
    pub(crate) fn reserve(&self, growth: usize) -> Result<()> {
        if self.max_memory == 0 || growth == 0 {
            return Ok(());
        }
        loop {
            if self.used.load(Relaxed).saturating_add(growth) <= self.max_memory {
                return Ok(());
            }
            match self.policy {
                Policy::NoEviction => return Err(StoreError::Oom),
                Policy::AllKeysLru => {
                    if !self.evict_one(false) {
                        return Err(StoreError::Oom);
                    }
                }
                Policy::VolatileLru => {
                    if !self.evict_one(true) {
                        return Err(StoreError::Oom);
                    }
                }
            }
        }
    }

    /// Removes one sampled-LRU victim. Returns false if no eligible key exists.
    pub(crate) fn evict_one(&self, volatile_only: bool) -> bool {
        let tick = self.tick.load(Relaxed);
        let start = self.next_rand() as usize;
        let mut best: Option<(u32, usize, Vec<u8>)> = None;
        let mut found = 0;
        for i in 0..self.shards.len() {
            let idx = start.wrapping_add(i) & self.mask;
            let shard = self.shards[idx].read();
            if let Some((age, key)) = pick_victim(&shard, volatile_only, tick, self.next_rand()) {
                found += 1;
                if best.as_ref().is_none_or(|b| age > b.0) {
                    best = Some((age, idx, key));
                }
                if found == SAMPLE_SIZE {
                    break;
                }
            }
        }
        let Some((_, idx, key)) = best else {
            return false;
        };
        let mut shard = self.shards[idx].write();
        self.remove_locked(&mut shard, idx, &key);
        true
    }

    /// splitmix64 over an atomic counter. Good enough for sampling; not for security.
    pub(crate) fn next_rand(&self) -> u64 {
        let mut z = self
            .rng
            .fetch_add(0x9E37_79B9_7F4A_7C15, Relaxed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Picks the least recently used key from a window of up to `SAMPLE_SIZE` eligible entries
/// that starts at a random position in the shard. Returns the key's age and a copy of it.
fn pick_victim(
    shard: &ShardMap,
    volatile_only: bool,
    tick: u32,
    rnd: u64,
) -> Option<(u32, Vec<u8>)> {
    let len = shard.len();
    if len == 0 {
        return None;
    }
    let offset = (rnd % len as u64) as usize;
    let mut best: Option<(u32, &Entry)> = None;
    let window = shard
        .iter()
        .skip(offset)
        .chain(shard.iter())
        .filter(|e| !volatile_only || e.expires_at != 0)
        .take(SAMPLE_SIZE);
    for e in window {
        let age = tick.wrapping_sub(e.last_access.load(Relaxed));
        if best.is_none_or(|(a, _)| age > a) {
            best = Some((age, e));
        }
    }
    best.map(|(age, e)| (age, e.key().to_vec()))
}
