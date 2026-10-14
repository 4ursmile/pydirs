# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-10-15

### Changed

- Renamed the project to `pydirs` (PyPI distribution, import name, native module
  `_pydirs`, `pydirs-server` command, `PYDIRS_PASSWORD`), because `tinycache-rs`
  could not be published under the old name.

### Fixed

- Reopening a data directory right after dropping a store no longer fails with a
  spurious "locked" error while the sweeper thread releases its last reference.
- `Server.stop` keeps waking the accept thread until it exits, so shutdown cannot hang.
- Release and CI workflows: `id-token: write` for PyPI publishing, wheel-based CI
  install that works on Windows.

## [0.1.0] - 2026-10-14

### Added

- Rust core: sharded in-memory store (64 shards by default, `parking_lot` locks)
  with string, hash, list and set values.
- Key expiry with `ex` / `px` on `set`, `expire`, `pexpire`, `persist`, `ttl`
  and `pttl`. Lazy expiry on read plus a background sweeper thread.
- Optional memory limit (`max_memory`) with `noeviction`, `allkeys-lru` and
  `volatile-lru` policies using sampled LRU.
- Python package `pydirs` (distribution `pydirs`) built with PyO3 and
  maturin against the `abi3-py39` stable ABI, supporting Python 3.9 through 3.14.
- `Cache` class with dict-style access, context-manager support and a `decode`
  option for `str` reads.
- `memoize(ttl=...)` decorator on `Cache` instances.
- JSON support using the standard library `json` module only (no pickle):
  `set_json`, `get_json`, `mget_json`, `mset_json`, `hset_json`, `hget_json`,
  `lpush_json`, `rpush_json` and `lrange_json`.
- `Cache(serializer="json")` routes `set`, `get`, `mget`, `mset`, dict-style
  access and `memoize` through JSON, so dicts, lists and other JSON-compatible
  values round-trip. The default remains `serializer="raw"`.
- Rejection of `NaN` and `Infinity` so stored values are strictly valid JSON.

### Changed

- The core stores each string key and value in one allocation, with 40-byte table
  entries backed by `hashbrown`. Memory accounting rounds to 16-byte allocation
  granules and counts shard bucket arrays, so `used_memory` tracks RSS growth more
  closely. Constants are calibrated with `examples/mem_probe.rs`.
- `Cache` and `Client` share one API mixin, so both support the same
  serializers, JSON, pydantic, decoding and `memoize`.
- Optional pydantic support (`pip install "pydirs[pydantic]"`, pydantic
  v2 only): `set_model`, `get_model`, `mget_model`, `hset_model` and
  `hget_model`. The package works without pydantic, which is imported lazily.
- `memoize` returns real pydantic model instances when the return annotation
  names a pydantic type. It accepts a `model=` override, supports async
  functions, keeps a cached `None` distinct from a miss, and keys
  pydantic-model arguments by `model_dump_json()`.
- `Server` class: RESP2 TCP server compatible with `redis-py`. Supports
  strings, keys, hashes, lists, sets, `PING`, `ECHO`, `SETEX`, `DECRBY`,
  `FLUSHDB`, `SELECT` (database 0), `COMMAND`, `AUTH`, `INFO`, `SAVE`, `BGSAVE`,
  `LASTSAVE` and `QUIT`. `Server` is a context manager, and `port` reports the
  port actually bound.
- Password authentication: `Server(password=...)` and `Client(password=...)`.
  Until a connection sends a correct `AUTH`, only `AUTH` and `QUIT` are accepted,
  and other commands get `NOAUTH`. A wrong password gets `WRONGPASS`.
- Persistence: `Cache(persist_dir=..., fsync="everysec"|"always"|"never",
  snapshot_interval=300)`, with `save()`, `persistent` and `persist_stats`. The
  design is a snapshot plus a write-ahead log in one directory, with a `LOCK`
  file. `close()` (and leaving a `with` block) writes a final snapshot. See
  `docs/persistence.md` for the guarantees and their limits.
- Persistence exceptions: `CacheLockedError` (a subclass of `OSError`) when
  another live process holds the directory, and `CorruptDataError` (a subclass of
  `ValueError`) when the snapshot fails its checksum or structure check.
- `AuthenticationError` (a subclass of `ConnectionError`) for missing or wrong
  passwords on a `Client`.
- `Client` administration: `info()`, `save()`, `bgsave()` and `lastsave()`.
  `used_memory` on a `Client` reads `INFO`.
- `Client`: thread-safe, pure-Python RESP2 client for a running server, with the
  same API as `Cache`. It has a connection pool (`pool_size`), per-socket
  `timeout`, `AUTH` via `password`, and retries an idempotent command once after
  a dropped connection. Non-idempotent commands are never retried.
- `Pipeline` (`client.pipeline()`) sends several commands in one write. Results
  are in `p.results` and `execute()`.
- Serializer protocol: `Cache(serializer=...)` accepts `"raw"`, `"json"`, or any
  object with `dumps` and `loads` methods. `JsonSerializer` and
  `PydanticSerializer` are provided.
- Command-line server: `pydirs-server` and `python -m pydirs`, with
  `--host`, `--port`, `--max-memory`, `--policy`, `--shards`, `--dir`, `--fsync`,
  `--snapshot-interval`, `--requirepass` (or `PYDIRS_PASSWORD`),
  `--allow-insecure` and `--version`. It refuses non-loopback binds without a
  password (exit code 2). A locked or corrupt data directory exits with code 1.
  On SIGINT or SIGTERM it stops and, with `--dir`, writes a final snapshot.
- Memory-leak tests: a Rust counting-allocator suite (`tests/leaks.rs`), Python
  RSS and fd tests under the `leak` marker, and a Linux `leaks` CI job running
  the Rust tests under valgrind.
- The GIL is released around every core call so Python threads run in parallel.
- Prebuilt wheels for Linux (manylinux and musllinux, x86_64 and aarch64),
  macOS (x86_64 and arm64) and Windows (x64), plus a source distribution.
- CI workflow: Rust lint and tests, and a Python test matrix over Ubuntu, macOS
  and Windows.
- Release workflow: publishes to PyPI when a GitHub Release is published, after
  verifying that the tag matches the `Cargo.toml` version.

[Unreleased]: https://github.com/4ursmile/pydirs/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/4ursmile/pydirs/releases/tag/v0.1.1
[0.1.0]: https://github.com/4ursmile/pydirs/releases/tag/v0.1.0
