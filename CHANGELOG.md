# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-14

### Added

- Rust core: sharded in-memory store (64 shards by default, `parking_lot` locks)
  with string, hash, list and set values.
- Key expiry with `ex` / `px` on `set`, `expire`, `pexpire`, `persist`, `ttl`
  and `pttl`. Lazy expiry on read plus a background sweeper thread.
- Optional memory limit (`max_memory`) with `noeviction`, `allkeys-lru` and
  `volatile-lru` policies using sampled LRU.
- Python package `tinycache` (distribution `tinycache-rs`) built with PyO3 and
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
- Optional pydantic support (`pip install "tinycache-rs[pydantic]"`, pydantic
  v2 only): `set_model`, `get_model`, `mget_model`, `hset_model` and
  `hget_model`. The package works without pydantic, which is imported lazily.
- `memoize` returns real pydantic model instances when the return annotation
  names a pydantic type. It accepts a `model=` override, supports async
  functions, keeps a cached `None` distinct from a miss, and keys
  pydantic-model arguments by `model_dump_json()`.
- `Server` class: RESP2 TCP server compatible with `redis-py`, supporting
  strings, keys, hashes, lists, sets and `PING` / `QUIT`.
- The GIL is released around every core call so Python threads run in parallel.
- Prebuilt wheels for Linux (manylinux and musllinux, x86_64 and aarch64),
  macOS (x86_64 and arm64) and Windows (x64), plus a source distribution.
- CI workflow: Rust lint and tests, and a Python test matrix over Ubuntu, macOS
  and Windows.
- Release workflow: publishes to PyPI when a GitHub Release is published, after
  verifying that the tag matches the `Cargo.toml` version.

[Unreleased]: https://github.com/4ursmile/tinycache/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/4ursmile/tinycache/releases/tag/v0.1.0
