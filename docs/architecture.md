# Architecture

This document describes how `tinycache` is built and why. The public Python API
is in [api.md](api.md).

## Layers

```
Python (tinycache package)          python/tinycache/
  Cache, Client, Server, CLI          public API, decoding, serializers, pydantic, memoize
        |  TinyCacheAPI mixin (_api.py) is shared by Cache and Client
        |
PyO3 binding (_tinycache)           src/lib.rs            (feature "python")
        |
Rust core (crate::core)             src/core/             (no Python dependency)
  Store -> shards -> hashbrown tables
  expiry sweeper, memory accounting, eviction
        |
RESP2 server (crate::server)        src/server/           (std::net, thread per connection)
```

The core is plain Rust with no Python dependency. The `python` Cargo feature,
on by default, gates PyO3. The core is tested with
`cargo test --no-default-features`, which is what CI's lint job runs.

Two Python classes serve the same API. `Cache` calls the native extension
in-process. `Client` speaks RESP2 to a running `Server` (or `tinycache-server`)
over a socket, with a small pure-Python RESP encoder and decoder. Both inherit
decoding, serializers, JSON, pydantic, dict sugar and `memoize` from one mixin,
`TinyCacheAPI`, which validates arguments and encodes values before calling a
small bytes-level backend. This keeps the two backends from drifting apart.

## Sharing across processes

A `Cache` is private to its process. To share one cache between several
processes, run the server (`tinycache-server` or `python -m tinycache`) once and
have each process use a `Client`. The server is the same RESP2 server that
`tinycache.Server` runs in-process, with a command-line front end.

## Sharding

The keyspace is split across `N` shards (default 64, a power of two).

- A key maps to a shard with `hash & (N - 1)`, using `ahash`.
- Each shard is a `parking_lot::RwLock` around a `hashbrown::HashTable<Entry>`.
- Operations lock only the shard that owns the key, so independent keys rarely
  contend. There is no global lock.
- Multi-key operations (`mget`, `mset`, `delete`, `keys`, `dbsize`) lock shards
  one at a time, so they are not atomic across shards.

## Memory layout

Each table slot is one `Entry`, and the key lives inside the entry. Layout as
built (see `src/core/value.rs`):

```
Entry (40 bytes) {
    body:        Body,            // 24 bytes: Str(Vec<u8>) holds key ++ value,
                                  //           or Coll(Box<Coll>) for list/hash/set
    key_len:     u32,             // length of the key prefix of a Str body
    expires_at:  u64,             // absolute ms since epoch; 0 means no expiry
    last_access: AtomicU32,       // logical access stamp for LRU
}
```

- **One allocation per string entry.** A string key and its value share one heap
  block (`key ++ value`), so the key is not stored in a second allocation.
- **Collections are boxed.** A list, hash or set sits behind `Box<Coll>`, so the
  slot stays at 40 bytes for the common string case.
- **Empty collections are removed**, as in Redis.
- The 40-byte size is checked by reasoning about the field layout. It is not a
  compile-time assertion in the crate, so a field added later would change it
  silently.

### Memory accounting

`used_memory` and `max_memory` track bytes the store owns. Accounting follows
the real allocation sizes, not the logical payload:

- Heap blocks are charged after rounding up to 16 bytes, the allocator's granule
  (`alloc_size`).
- Each shard's bucket array is charged from its real capacity after every write
  (`table_bytes`, settled in `Inner::settle`). That covers table growth and
  shrinkage, plus the control bytes hashbrown allocates for probing.
- Collections add a fixed overhead for the box and header, plus an amortised cost
  per element for container slots and load-factor slack. Both constants are
  calibrated with `examples/mem_probe.rs`.

`used_memory` is therefore an estimate that tracks RSS growth, not an exact byte
count. On macOS, freed memory is not always returned to the OS, so RSS can stay
high after `flush()` while the store holds nothing live.

## GIL release

Each call from Python into the core runs inside `py.allow_threads(...)`. The GIL
is released while the Rust work runs, so:

- Python threads calling the cache run in parallel, limited only by shard
  contention.
- The sweeper thread and the RESP server threads never block Python code.

Arguments are converted to owned Rust values while the GIL is held. Only the core
work runs without it.

## Expiry

Expiry uses two mechanisms, as Redis does:

1. **Lazy expiry.** Every read checks `expires_at` against the clock. An expired
   key is removed and reported as missing.
2. **Active expiry.** A sweeper thread wakes every 100 ms, samples 20 keys per
   shard, and removes the ones that have expired.

The sweeper is a `std::thread`. It stops on `close()` or when the last `Store`
handle is dropped. `sweep_interval_ms = 0` disables the thread, and lazy expiry
still applies.

Time comes from an injectable clock (`Store::with_clock`), so tests can advance
time without sleeping.

## Eviction

When `max_memory` is set and a write pushes usage over the limit:

- `noeviction`: the write fails with `MemoryError` (`OOM` on the wire).
- `allkeys-lru`: sampled LRU. Sample 5 keys, evict the one with the oldest
  `last_access`, and repeat until usage is under the limit.
- `volatile-lru`: the same, but only keys with an expiry are candidates.

Sampled LRU avoids a global list shared across shards. Eviction is approximate,
as it is in Redis.

## Server mode

The RESP2 server runs on its own thread and accepts connections, with one thread
per client. Each command is parsed and dispatched to the same `Store` the Python
`Cache` uses, so a value written in one place is visible through the other.
The server uses only the standard library. An async runtime was left out to keep
the wheel and dependency tree small.

Supported commands are listed in [api.md](api.md#supported-commands). `AUTH`
gates every command except `AUTH` and `QUIT` when a password is configured.
Each connection authenticates separately, and the password is compared in
constant time. `INFO`, `SAVE`, `BGSAVE` and `LASTSAVE` expose statistics and
snapshots.

## Persistence

Persistence is opt-in. When a directory is given, the core writes every
mutation to a write-ahead log and periodically writes a full snapshot. Recovery
loads the snapshot and replays the log. The file layout, the snapshot and log
generations, the lock file, the fsync policies and the exact guarantees are in
[persistence.md](persistence.md).

Two design points matter for the rest of the core:

- Logging happens under the same shard write lock that applies the mutation, so
  the log order matches the per-key order.
- A snapshot read-locks every shard only while it rotates the log, then streams
  each shard. Writers are blocked only for the duration of that rotation and
  of each shard's serialisation.

Persistence adds nothing to the in-memory layout. With it off, a log call is a
single `Option` check.

## Why abi3

The extension is built with PyO3's `abi3-py39` feature, which targets the CPython
stable ABI for Python 3.9 and later.

- One wheel per platform covers CPython 3.9 through 3.14, and later 3.x releases
  that keep the stable ABI.
- The release pipeline builds one set of wheels, not one per Python minor
  version, so the build matrix is roughly 1 × platforms instead of 6 × platforms.
- The cost is that abi3 builds cannot use CPython APIs outside the stable ABI,
  and can be a little slower in hot paths. The project uses no such APIs.
- Free-threaded CPython builds (3.13t and later) are not covered by the abi3
  wheel. They fall back to the source distribution.

## Build and packaging

- `maturin` builds the extension and packages `python/` as the `tinycache`
  package. The native module is `tinycache._tinycache`.
- `tinycache-server` is a console script that points at `tinycache._cli:main`.
  `python -m tinycache` runs the same command.
- The release profile uses fat LTO, `codegen-units = 1`, and `strip = true`.
  `panic` stays `unwind` because PyO3 requires it.

## Testing

- Rust unit tests cover the core and server parser and dispatch. Fake-clock
  tests cover TTL, and eviction and glob tests are included. An 8-thread test
  covers concurrency.
- Python tests (`tests/python`) cover the API, the client's RESP codec and retry
  rules against a fake server, the server over a real socket, and the CLI in a
  subprocess.
- CI runs `cargo fmt --check`, `clippy -D warnings`, and `cargo test
  --no-default-features`. It runs the Python suite on Ubuntu, macOS and Windows
  for Python 3.9 to 3.14.

## Testing for leaks

Leak tests run a workload many times and check that memory stays flat after
warm-up. Growth that scales with the number of iterations is a failure. No test
asserts on a single run.

- **Rust** (`tests/leaks.rs`): a test-only counting `#[global_allocator]` in an
  integration test file measures live heap. The test asserts that bytes in use
  after N cycles stay within a baseline plus a tolerance (about 1 MiB). Cycles
  cover set/delete, TTL expiry through the sweeper, flush, collection
  auto-removal, eviction churn, and repeated `Store` create, close and drop.
  Server start/stop and connection cycles are covered too.
- **Python** (`tests/python/test_leaks.py`, marker `leak`): RSS is read from
  `/proc/self/statm` after `gc.collect()` and a warm-up. The test fails if RSS
  grows by more than a few MB over the second half of the run. Open file
  descriptors are compared by listing `/dev/fd`.
- **CI** (`leaks` job, Linux only): the Rust test binaries run under
  `valgrind --leak-check=full --errors-for-leak-kinds=definite --error-exitcode=1`.
  The binaries come from cargo's JSON build output. `TINYCACHE_VALGRIND=1` is set
  so the tests can lower their iteration counts. The Python `-m leak` tests run
  against a release build of the extension.

Sanitizer-based checks (`-Zsanitizer=leak`) are not used, because they need a
nightly toolchain.
