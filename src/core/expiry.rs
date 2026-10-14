//! Background sweeper that actively removes expired keys. Lazy expiry on access
//! remains the correctness mechanism; the sweeper only bounds memory held by dead keys.

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Weak;
use std::thread;
use std::time::Duration;

use super::store::Inner;

const SAMPLE_SIZE: usize = 20;
const MAX_ROUNDS: usize = 16;

/// Spawns the sweeper. It holds only a `Weak` reference, so it exits on its own once
/// every handle is dropped, and it checks the `closed` flag on each tick.
pub(crate) fn spawn(inner: Weak<Inner>, interval: Duration) {
    // If the OS refuses the thread, expired keys are still removed lazily on access.
    let _ = thread::Builder::new()
        .name("pydirs-sweeper".into())
        .spawn(move || loop {
            thread::sleep(interval);
            let Some(inner) = inner.upgrade() else {
                break;
            };
            if inner.closed.load(Relaxed) {
                break;
            }
            inner.sweep_once();
        });
}

impl Inner {
    /// One sweep pass over every shard. Samples keys and repeats while many are expired.
    pub(crate) fn sweep_once(&self) {
        let now = self.now();
        for (idx, lock) in self.shards.iter().enumerate() {
            let mut shard = lock.write();
            for _ in 0..MAX_ROUNDS {
                if shard.is_empty() {
                    break;
                }
                // A window that wraps past the end of the shard, so every key can be sampled.
                let offset = (self.next_rand() % shard.len() as u64) as usize;
                let dead: Vec<Vec<u8>> = shard
                    .iter()
                    .skip(offset)
                    .chain(shard.iter())
                    .take(SAMPLE_SIZE)
                    .filter(|e| e.is_expired(now))
                    .map(|e| e.key().to_vec())
                    .collect();
                if dead.is_empty() {
                    break;
                }
                for k in &dead {
                    self.remove_locked(&mut shard, idx, k);
                }
            }
        }
    }
}
