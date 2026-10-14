# Architecture

This document describes how `tinycache` is put together and why. The public
Python API is documented in [api.md](api.md).

## Layers

```
Python (tinycache package)        python/tinycache/
  Cache, Server, memoize          thin wrappers, type stubs (.pyi)
        |
PyO3 binding (_tinycache)         src/lib.rs            (feature "python")
        |
Rust core (crate::core)           src/core/             (no Python dependency)
  Store -> shards -> HashMap
  expiry sweeper, eviction
        |
RESP2 server (crate::server)      src/server/           (std::net, thread per connection)
```

The core is plain Rust with no dependency on Python. The `python` Cargo feature
(on by default) gates the PyO3 dependency. This lets the core be tested with
`cargo test --no-default-features`, which is what CI's lint job runs.

## Sharding

The keyspace is split across `N` shards (default 64, must be a power of two).

- A key is mapped to a shard with `hash(key) & (N - 1)`, using `ahash`.
- Each shard is a `parking_lot::RwLock` around its own `HashMap<Box<[u8]>, Entry>`.
- Operations lock only the shard that owns the key. There is no global lock, so
  independent keys rarely contend.
- Multi-key operations (`mget`, `mset`, `delete`, `keys`, `dbsize`) lock the
  shards they touch one at a time. They are not atomic across shards.

Read-only operations take a read lock; anything that mutates takes a write lock.

## Data model

Each entry is:

```
Entry { value: Value, expires_at: Option<u64 /* ms since epoch */>, last_access: u32 }

enum Value {
    Bytes(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Hash(HashMap<..>),
    Set(HashSet<..>),
}
```

- The `Value` enum mirrors the Redis types the cache supports.
- `last_access` is a coarse timestamp used by LRU eviction. It is updated on
  reads and writes.
- Empty lists, hashes and sets are removed automatically, as in Redis.
- Wrong-type access (for example `get` on a list key) is reported as a
  `WRONGTYPE` error, which surfaces in Python as `TypeError`.

## GIL release

Every call from Python into the core is wrapped in `py.allow_threads(...)`. The
GIL is released for the duration of the Rust work, so:

- Python threads calling the cache run in parallel, limited only by shard
  contention.
- The background sweeper and the RESP server do not block Python code.

Arguments are converted to owned Rust values while the GIL is held. Only the
core work runs without it.

## Expiry

Expiry uses two complementary mechanisms, the same strategy Redis uses:

1. **Lazy expiry.** Every read checks `expires_at` against the current time. An
   expired key is removed and reported as missing.
2. **Active expiry.** A sweeper thread wakes every 100 ms, samples 20 keys per
   shard, and removes those that have expired.

The sweeper is a plain `std::thread`. It stops when `close()` is called or when
the last `Store` handle is dropped. Setting `sweep_interval_ms` to 0 disables
the thread entirely; lazy expiry still applies.

Time is read through an injectable clock (`Store::with_clock`) so that tests can
advance time deterministically without sleeping.

## Eviction

Memory accounting is approximate. Each entry is charged an estimate based on
its key and value sizes plus fixed overhead. `used_memory` reports the running
total.

When `max_memory` is set and a write pushes usage over the limit:

- `noeviction`: the write fails with `MemoryError`.
- `allkeys-lru`: sampled LRU. Sample 5 keys from across the keyspace, evict the
  one with the oldest `last_access`, repeat until usage is under the limit.
- `volatile-lru`: same as above, but only keys with an expiry are candidates.

Sampled LRU avoids maintaining a global linked list, which would need a lock
shared across all shards. The trade-off is that eviction is approximate, as it
is in Redis.

## Server mode

`tinycache.Server` runs a RESP2 listener on its own thread. It accepts
connections and spawns one thread per client. Each command is parsed and
dispatched to the same `Store` the Python `Cache` uses, so a value written
through Python is visible to a `redis-py` client and vice versa.

The server uses only the standard library. An async runtime such as tokio was
left out to keep the wheel small and the dependency tree short. The cost is
one OS thread per connection, which is fine for development and local use.

## Why abi3

The extension is built with PyO3's `abi3-py39` feature, which targets the
CPython stable ABI for Python 3.9 and later.

- One wheel per platform covers CPython 3.9 through 3.14, and any later 3.x
  release that keeps the stable ABI.
- The release pipeline builds one set of wheels, not one per Python minor
  version. This cuts the build matrix from roughly 6 × platforms to 1 × platforms.
- The trade-off is that abi3 builds cannot use CPython APIs that are excluded
  from the stable ABI, and they can be slightly slower in hot paths. This
  project does not use such APIs.
- Free-threaded CPython builds (3.13t and later) are not covered by the abi3
  wheel; they fall back to the source distribution.

## Build and packaging

- `maturin` builds the extension and packages `python/` as the `tinycache`
  package. The native module is `tinycache._tinycache`.
- The release profile uses fat LTO, `codegen-units = 1`, and `strip = true` for
  speed and size. `panic` stays `unwind` because PyO3 requires it.
- CI runs the CPU matrix described in [releasing.md](releasing.md#what-the-workflows-do).

## Testing for leaks

Leak tests run a workload many times and check that memory stays flat after
warm-up. Growth that scales with the number of iterations is a failure. No test
asserts on a single run.

- **Rust** (`tests/leaks.rs` and the lib tests): a test-only counting
  `#[global_allocator]` in an integration test file measures live heap. The
  test asserts that bytes in use after N cycles stay within a baseline plus a
  tolerance (about 1 MiB). Cycles cover set/delete, TTL expiry through the
  sweeper, flush, collection auto-removal, eviction churn, and repeated
  `Store` create/close/drop.
- **Python** (`tests/python/test_leaks.py`, marker `leak`): RSS is read from
  `/proc/self/statm`, after `gc.collect()` and a warm-up. The test fails if RSS
  grows by more than a few MB over the second half of the run. Open file
  descriptors are compared by listing `/dev/fd`.
- **CI** (`leaks` job, Linux only): the Rust test binaries run under
  `valgrind --leak-check=full --errors-for-leak-kinds=definite --error-exitcode=1`.
  The binaries come from cargo's JSON build output. `TINYCACHE_VALGRIND=1` is set
  so the tests can lower their iteration counts. The Python `-m leak` tests run
  against a release build of the extension.

Sanitizer-based checks (`-Zsanitizer=leak`) are not used, because they need a
nightly toolchain.
