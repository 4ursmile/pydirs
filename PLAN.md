# tinycache — lightweight in-memory cache (Rust core, Python package)

Goal: a Redis-like, in-process cache engine written in Rust, installable with
`pip install tinycache-rs`, working on CPython 3.9–3.14, auto-published to PyPI
when a GitHub Release is published.

PyPI distribution name: `tinycache-rs` (check availability before first release).
Import name: `tinycache`. Rust lib name: `_tinycache` (the native module).

## 1. Design decisions

| Concern | Decision | Why |
|---|---|---|
| Binding | PyO3 + maturin, `abi3-py39` | One wheel per OS/arch covers 3.9–3.14 (stable ABI); no per-version builds |
| Concurrency | N shards (default 64, power of 2), each `parking_lot::RwLock<Shard>`; shard = hash(key) & mask | Low contention, no global lock |
| Hashing | `ahash` for key->shard and inside maps | Fast, non-cryptographic |
| Storage | `HashMap<Box<[u8]>, Entry>` per shard; `Entry { value: Value, expires_at: Option<u64 ms>, last_access: u32 }` | Compact |
| Values | `enum Value { Bytes(Vec<u8>), List(VecDeque<Vec<u8>>), Hash(HashMap<..>), Set(HashSet<..>) }` | Redis-style types |
| GIL | Release GIL (`py.allow_threads`) around every core call | True parallel access from Python threads |
| Expiry | Lazy (checked on read) + background sweeper thread sampling 20 keys/shard every 100 ms, stops on `close()`/drop | Same strategy as Redis; no timer wheel overhead |
| Eviction | Optional `max_memory` (bytes, approximate accounting per entry). When exceeded: sampled-LRU (sample 5 keys, evict oldest `last_access`) until under limit. Policies: `noeviction`, `allkeys-lru`, `volatile-lru` | Redis-compatible semantics |
| Errors | `WRONGTYPE` -> Python `TypeError`; OOM with noeviction -> `MemoryError`; non-integer INCR -> `ValueError` | Pythonic |
| Optional server | Thread-per-connection RESP2 TCP server (`tinycache.Server`) supporting PING, GET, SET (EX/PX/NX/XX), DEL, EXISTS, EXPIRE, PEXPIRE, TTL, PTTL, INCR, INCRBY, DECR, MGET, MSET, KEYS, DBSIZE, FLUSHALL, HSET/HGET/HDEL/HGETALL, LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN, SADD/SREM/SMEMBERS/SISMEMBER, QUIT | Makes it usable by `redis-py` clients as a mini-Redis; std only, no tokio (keeps wheel small) |
| Release profile | `opt-level=3, lto="fat", codegen-units=1, panic="abort"?? NO` — keep `panic="unwind"` (PyO3 needs it); `strip=true` | Fast + small |

## 2. Repo layout

```
Cargo.toml                 (given)
pyproject.toml             (given)
src/lib.rs                 PyO3 module `_tinycache`  (Task B)
src/core/mod.rs            re-exports                 (Task A)
src/core/store.rs          sharded Store + all commands (Task A)
src/core/value.rs          Value / Entry               (Task A)
src/core/expiry.rs         sweeper thread              (Task A)
src/core/evict.rs          memory accounting + LRU     (Task A)
src/server/mod.rs          RESP server (std::net)      (Task A2)
python/tinycache/__init__.py   re-exports, __version__  (Task B)
python/tinycache/_tinycache.pyi type stubs             (Task B)
python/tinycache/py.typed                              (Task B)
tests/python/test_*.py     pytest suite                (Task B)
benches/store_bench.rs     criterion bench             (Task A)
.github/workflows/ci.yml   test matrix                 (Task C)
.github/workflows/release.yml  build + publish        (Task C)
README.md, docs/*.md, CHANGELOG.md, LICENSE            (Task C)
```

## 3. Core Rust API contract (Task A implements, Task B consumes)

All in `crate::core`. Keys/values are `&[u8]` / `Vec<u8>`. Time is injectable for tests
via `Store::with_clock(Arc<dyn Fn() -> u64 + Send + Sync>)` (ms since epoch).

```rust
pub struct Config { pub shards: usize /*64*/, pub max_memory: usize /*0=unlimited*/, pub policy: Policy, pub sweep_interval_ms: u64 /*100; 0 disables thread*/ }
pub enum Policy { NoEviction, AllKeysLru, VolatileLru }
pub enum StoreError { WrongType, Oom, NotInteger, Overflow }   // impl Display + std::error::Error
pub type Result<T> = std::result::Result<T, StoreError>;

pub struct Store; // Clone = Arc handle; sweeper stops when last handle dropped or close() called
impl Store {
  pub fn new(cfg: Config) -> Store;
  pub fn close(&self);
  // strings
  pub fn set(&self, k:&[u8], v:Vec<u8>, ttl_ms:Option<u64>, nx:bool, xx:bool) -> Result<bool>; // false if NX/XX condition failed
  pub fn get(&self, k:&[u8]) -> Result<Option<Vec<u8>>>;
  pub fn mget(&self, ks:&[&[u8]]) -> Vec<Option<Vec<u8>>>;     // wrong-type => None
  pub fn mset(&self, kv: Vec<(Vec<u8>,Vec<u8>)>) -> Result<()>;
  pub fn incr_by(&self, k:&[u8], delta:i64) -> Result<i64>;
  pub fn append(&self, k:&[u8], v:&[u8]) -> Result<usize>;
  // keys
  pub fn delete(&self, ks:&[&[u8]]) -> usize;
  pub fn exists(&self, k:&[u8]) -> bool;
  pub fn expire_ms(&self, k:&[u8], ttl_ms:u64) -> bool;
  pub fn persist(&self, k:&[u8]) -> bool;
  pub fn pttl(&self, k:&[u8]) -> i64;          // -2 missing, -1 no expiry
  pub fn keys(&self, glob:&str) -> Vec<Vec<u8>>; // glob: * ? [abc] \x
  pub fn dbsize(&self) -> usize;               // excludes expired
  pub fn flush(&self);
  pub fn used_memory(&self) -> usize;
  // hash
  pub fn hset(&self,k:&[u8],f:&[u8],v:Vec<u8>)->Result<bool>; // true if new field
  pub fn hget(&self,k:&[u8],f:&[u8])->Result<Option<Vec<u8>>>;
  pub fn hdel(&self,k:&[u8],f:&[&[u8]])->Result<usize>;
  pub fn hgetall(&self,k:&[u8])->Result<Vec<(Vec<u8>,Vec<u8>)>>;
  pub fn hlen(&self,k:&[u8])->Result<usize>;
  // list
  pub fn lpush(&self,k:&[u8],vs:Vec<Vec<u8>>)->Result<usize>;
  pub fn rpush(&self,k:&[u8],vs:Vec<Vec<u8>>)->Result<usize>;
  pub fn lpop(&self,k:&[u8])->Result<Option<Vec<u8>>>;
  pub fn rpop(&self,k:&[u8])->Result<Option<Vec<u8>>>;
  pub fn lrange(&self,k:&[u8],start:i64,stop:i64)->Result<Vec<Vec<u8>>>; // Redis negative-index semantics
  pub fn llen(&self,k:&[u8])->Result<usize>;
  // set
  pub fn sadd(&self,k:&[u8],ms:Vec<Vec<u8>>)->Result<usize>;
  pub fn srem(&self,k:&[u8],ms:&[&[u8]])->Result<usize>;
  pub fn smembers(&self,k:&[u8])->Result<Vec<Vec<u8>>>;
  pub fn sismember(&self,k:&[u8],m:&[u8])->Result<bool>;
  pub fn scard(&self,k:&[u8])->Result<usize>;
}
```
Empty list/hash/set is removed automatically (Redis behaviour).

## 4. Python API contract (Task B)

```python
from tinycache import Cache, Server
c = Cache(max_memory=0, policy="noeviction", shards=64)   # context-manager; .close()
# values: bytes | str | int | float accepted on write (str->utf-8). Reads return bytes,
# unless Cache(decode=True) -> str.
c.set(key, value, ex=None, px=None, nx=False, xx=False) -> bool
c.get(key) -> bytes|str|None ;  c.mget([keys]) ; c.mset({k: v})
c.delete(*keys)->int ; c.exists(key)->bool ; c.expire(key, seconds)->bool ; c.pexpire ; c.persist
c.ttl(key)->int ; c.pttl(key)->int ; c.incr(key, 1) ; c.decr(key, 1) ; c.append
c.keys(pattern="*") ; len(c) ; c.flush() ; c.used_memory
c.hset/hget/hdel/hgetall/hlen ; c.lpush/rpush/lpop/rpop/lrange/llen ; c.sadd/srem/smembers/sismember/scard
# dict-style sugar: c[k]=v ; c[k] ; del c[k] ; k in c
@c.memoize(ttl=60)  # decorator, key = module.qualname + repr(args, sorted kwargs)
s = Server(cache=c, host="127.0.0.1", port=6380); s.start() (background thread); s.stop(); s.port
```
`tinycache.__version__` read from package metadata (`importlib.metadata`, with fallback).

### 4a. JSON support (Task B, Python layer only; core stays bytes-only)

- Standard-library `json` only, no extra dependencies (pure JSON, no pickle).
- `c.set_json(key, obj, ex=None, px=None, nx=False, xx=False) -> bool`: serialises with compact separators and `ensure_ascii=False`, stores UTF-8 bytes.
- `c.get_json(key, default=None)`: returns the decoded object, or `default` if the key is missing. Invalid JSON raises `ValueError`.
- `c.mget_json(keys)` returns a list with `None` for missing keys. `c.mset_json({k: obj})`.
- `Cache(serializer="raw" | "json")`: with `"json"`, `set`/`get`/`mget`/`mset`, dict sugar (`c[k]`), and `memoize` results all go through JSON, so any JSON-compatible value round-trips (dict, list, str, int, float, bool, None). Default stays `"raw"`.
- Non-serialisable objects raise `TypeError` (the standard `json` behaviour). `NaN`/`Infinity` are rejected (`allow_nan=False`) so the output is strictly valid JSON.
- Hash fields and list/set members can also take JSON: `hset_json`, `hget_json`, `lpush_json`, `rpush_json`, `lrange_json`.
- Tests: round-trip nested structures, unicode, big ints, TTL with JSON, corrupt-bytes `ValueError`, NaN rejection, `serializer="json"` mode, memoize returning dicts.
- The RESP server is unaffected: it serves raw bytes, so a client can read a JSON value as a string.

### 4b. Pydantic support (Task B, Python layer only, optional extra)

- Optional dependency: `pip install tinycache-rs[pydantic]` (pydantic>=2.5; v2 API only, via `TypeAdapter`). The package must import and work fully without pydantic installed; pydantic is imported lazily, and using a pydantic feature without it raises `ImportError` with an install hint.
- `c.set_model(key, obj, ex=None, px=None, nx=False, xx=False)`: `obj` is any type pydantic can serialise (BaseModel, dataclass, `list[Model]`, `dict[str, Model]`, TypedDict...). Stored as JSON bytes via `TypeAdapter(type(obj)).dump_json(obj)`.
- `c.get_model(key, model, default=None)`: validates with `TypeAdapter(model).validate_json(data)`. `model` can be any type annotation (`User`, `list[User]`). Validation failure raises `pydantic.ValidationError` (it subclasses `ValueError`). Adapters are cached per type.
- `c.mget_model(keys, model)`, `c.hset_model`, `c.hget_model(key, field, model)`.
- `Cache(serializer=...)` also accepts a `tinycache.PydanticSerializer(model=None)`-style object. Simpler contract: the serializer interface is `dumps(obj)->bytes`, `loads(data)->obj`; users may pass any object implementing it, and `"json"` / `"raw"` are built in.
- **memoize with pydantic**: `@c.memoize(ttl=60)` inspects the decorated function's return annotation (`typing.get_type_hints`, falling back to no model on failure). If the annotation is a type pydantic can handle and is not trivially JSON-native (BaseModel, dataclass, `list[Model]`, `dict[str, Model]`, `Optional[Model]`, etc.), the result is stored with `TypeAdapter(ret).dump_json` and a hit is returned as `validate_json` -> real model instances, not dicts. `@c.memoize(ttl=60, model=User)` overrides the annotation. Async functions (`async def`) must work too: `memoize` detects coroutine functions and returns an async wrapper (stores the awaited result). A cached `None` is distinguished from a miss. Cache key includes args that are pydantic models via their `model_dump_json()` instead of `repr`.
- Without pydantic installed or without a model annotation, memoize falls back to the section 4a JSON behaviour.
- `pyproject.toml`: add `pydantic = ["pydantic>=2.5"]` under optional-dependencies, and add pydantic to `test` extra (CI installs it). Python 3.9–3.14: pydantic >= 2.12 supports 3.14; CI pip resolves a supporting version, no pin.
- Tests (skip with `pytest.importorskip` where pydantic is required, plus one test file that blocks pydantic import to prove the no-pydantic path works): model round trip, nested model, `list[Model]`, `Optional`, validation error on corrupt/mismatched data, TTL, `mget_model`, hash model fields, memoize with annotation returning model instance (assert `isinstance`), memoize with `model=` override, memoize of `list[Model]`, memoize with a pydantic model argument, async memoize, cached `None`, ImportError hint when pydantic missing.

### 4c. Memory diet (Task D, src/core only, public API unchanged)

Measured problem: 1M keys (~8 B key, 64 B value) take 449 MB RSS in a pure-Rust probe (449 B/key; Redis ~128 B/key) while `used_memory()` says 131 MB. `flush()` also returns little memory. Causes: fat `Entry` (enum with a HashMap variant, `Option<u64>`, atomic, `Box<[u8]>` key) in `std::HashMap` slots that double and run at 44-87% load; key and value in two separate allocations (malloc rounds each to 16 B); accounting ignores all of it.
Targets (measure with a probe example, `examples/mem_probe.rs`, release, `--no-default-features`):
- 1M keys x 64 B values: **<= 200 MB RSS growth** (stretch 160 MB), and `used_memory()` within **+-25 %** of measured RSS growth.
- `flush()` / deleting most keys must actually release table memory (replace or shrink maps).
- No throughput regression beyond 10 % in `cargo bench`.
Direction (agent may deviate if measurements say so): `hashbrown::HashTable<Entry>` (add dep `hashbrown = "0.15"`) so the key lives inside the entry; string key+value in ONE `Box<[u8]>` (`key_len` stored in entry); collections behind `Option<Box<Collection>>` so the slot stays <= 40 B; `expires_at: u64` with 0 = none; `last_access: AtomicU32`. Accounting must charge slot size amortised for load factor, 16-byte allocation rounding, and element overheads for collections.

### 4d. Persistence (Task E, src/core/persist/**, hooks in store.rs, server commands)

Opt-in. `Config.persist: Option<PersistConfig>` (Config loses `Copy`, keeps `Clone`; `..Config::default()` usage stays valid).
```rust
pub struct PersistConfig { pub dir: PathBuf, pub fsync: FsyncPolicy, pub snapshot_interval_secs: u64 /*0 = only on save()/close()*/ }
pub enum FsyncPolicy { Always, EverySec, Never }
Store::open(cfg: Config) -> io::Result<Store>   // loads snapshot, replays WAL; Store::new(cfg) = open without persistence (panics if cfg.persist is Some: use open)
Store::save(&self) -> io::Result<()>            // synchronous snapshot + WAL rotation
Store::persist_stats(&self) -> Option<PersistStats>  // { last_save_ms, snapshot_bytes, wal_bytes, loaded_keys, last_error: Option<String> }
Store::close()  // stops threads, fsyncs WAL, writes a final snapshot when persistence is on
```
Design (snapshot + WAL):
- Files in `dir`: `snapshot.tcs` (+ `snapshot.tmp` during write), `wal-<gen>.log`, a `LOCK` file (advisory, `fs2`-free: create_new + pid; refuse to open if held by a live process, error `io::ErrorKind::WouldBlock`).
- Snapshot format: magic `TCSNAP01`, version u32, generation u64, then records `[type u8][key len varint][key][expires_at_ms u64][payload]` for Bytes/List/Hash/Set, terminated by an EOF marker and a CRC32 of everything (implement CRC32 locally, ~20 lines, no dep). Written to `snapshot.tmp`, fsynced, atomically renamed, then the directory fsynced. Expired keys are skipped when writing and when loading. TTLs are stored as ABSOLUTE ms since epoch (so downtime counts).
- WAL: append-only records `[len u32][crc32 u32][payload]` where payload is one logical mutation (Set with exp, Del, Expire/Persist, Incr/Append logged as the RESULTING Set so replay is idempotent, Hash/List/Set ops logged as the op). A torn/corrupt tail record is detected by CRC/length and the file is truncated there on load (log a warning in `last_error`, never crash). Appends happen under the shard write lock so per-key order is preserved; a single `Mutex<BufWriter>` WAL writer is fine. `EverySec` fsyncs from a background thread; `Always` fsyncs per write; `Never` leaves it to the OS.
- Consistent snapshot: acquire read locks on ALL shards (in index order), rotate the WAL to generation g+1 while still holding them (writers are blocked, so no append can interleave), then stream the snapshot with `generation = g+1`, release locks as soon as serialisation of each shard completes. After the snapshot is renamed, delete `wal-<g>.log` and older. Load = snapshot(gen n) then replay `wal-n.log`. If the process dies between rotation and rename, the old snapshot + old WAL + new WAL are all still present: load must pick the newest valid snapshot, then replay every WAL with generation >= snapshot generation in order... (agent: write this precisely and test crash points with fault injection: kill between each step by calling internal step functions).
- Background snapshotter thread every `snapshot_interval_secs` (only if dirty since last save). Stops with the store.
- Memory limits and eviction must not run while replaying (restore fully, then enforce limit lazily on next write).
- Server (src/server): new commands `SAVE`, `BGSAVE` (runs save on a thread, replies "Background saving started"), `LASTSAVE`, `INFO` (text with `used_memory`, `keys`, `uptime_seconds`, `persistence` lines), `AUTH password` and `ServerOptions { password: Option<String> }` via new `pub fn start_with_options(store, host, port, opts) -> io::Result<ServerHandle>` (keep `start` unchanged); when a password is set every command except AUTH, PING?no: except AUTH and QUIT returns `-NOAUTH Authentication required.`; compare in constant time.
- Tests: round trip every type + TTL across reopen; expired keys not restored; torn WAL tail; corrupt snapshot CRC falls back (error, empty or previous gen, never panic); crash-point tests; fsync policies; concurrent writers during `save()` then reopen and compare with an in-memory oracle; lock file contention; SIGKILL-style test by dropping without close (use an internal `abandon()` test hook).

### 4e. Shared server and client (Task F, Python layer; F2 wires persistence)

Per-process caches are not shared between workers. Solution: run `tinycache-server` once and let every process use `tinycache.Client`, which has the SAME API as `Cache`.
- `tinycache.Client(host="127.0.0.1", port=6380, password=None, timeout=5.0, pool_size=8, decode=False, serializer="raw")`: pure-Python, thread-safe (small connection pool), RESP2 over sockets, reconnect once on a broken connection, pipelining via `with client.pipeline() as p:` (optional, nice-to-have). Implements the same methods as Cache (strings, keys, hash, list, set, JSON, pydantic, dict sugar, memoize, context manager). Errors map the same: `-WRONGTYPE` -> TypeError, `-OOM` -> MemoryError, not-an-integer -> ValueError, connection failure -> `ConnectionError`.
- Refactor so `Cache` and `Client` share one mixin (`_api.py`: decode, serializer, JSON, pydantic, memoize, sugar) over a tiny bytes-level backend interface; do not duplicate those features.
- CLI `tinycache-server` (also `python -m tinycache`): `--host 127.0.0.1 --port 6380 --max-memory 0 --policy noeviction --shards 64 --dir PATH --fsync everysec|always|never --snapshot-interval 300 --requirepass PASS` (or env `TINYCACHE_PASSWORD`); handles SIGINT/SIGTERM with a clean shutdown that saves; prints a one-line startup banner; refuses to bind a non-loopback host without `--requirepass` unless `--allow-insecure`. `[project.scripts] tinycache-server = "tinycache._cli:main"` (already added to pyproject).
- F1 (now): Client, mixin refactor, CLI, tests against `Server` + CLI subprocess. F2 (after Task E): `Cache(persist_dir=None, fsync="everysec", snapshot_interval=300)`, `Cache.save()`, `Cache.persist_stats`, `Server(password=)`, lib.rs bindings, .pyi, and CLI `--dir/--fsync/--snapshot-interval/--requirepass` wiring, tests.

### 4f. Memory-leak testing (applies to every task)

A leak test runs a workload many times and asserts memory is flat after warm-up, so growth proportional to iterations fails. Never assert on a single run.
- **Rust** (`src/core`, `src/server`, tests marked `#[ignore]`-free but fast, < 10 s total): a helper measures live heap with a counting `#[global_allocator]` wrapper (test-only, in a `tests/` integration file so it does not affect the library) and asserts `bytes_in_use` after N cycles <= baseline + tolerance (e.g. 1 MiB). Cycles: set/delete 100k keys x 10 rounds; set with TTL then let the sweeper expire them; flush after fill; collection create/empty/auto-remove; eviction churn under `max_memory`; `Store` create + `close()` + drop x 200 (sweeper thread joined, no thread count growth: check `/proc/self/task` on Linux or skip elsewhere); server start/stop x 50 and 200 connect/disconnect cycles (thread and fd counts return to baseline).
- **Python** (`tests/python/test_leaks.py`, marker `leak`, uses `resource`/`psutil`-free RSS via `/proc/self/statm` or `ps`): Cache create/close/del x 500; set/get/delete loops; `mget`/`hgetall`/`smembers` returning big results discarded x 1000; exceptions raised in a loop (`WRONGTYPE`, bad types) do not leak; JSON/pydantic round trips; `memoize` with unique args (cache bounded by TTL/flush); Server start/stop x 50 and Client connect/close x 200 (open fd count via `os.listdir('/dev/fd')` back to baseline); GIL-release paths under threads. Assert RSS growth over the last half of the run <= a few MB; use `gc.collect()` and warm-up first.
- **CI** (Linux only): extra job runs the Rust tests under `valgrind --leak-check=full --error-exitcode=1` on the core and server test binaries, and the Python `-m leak` tests; plus `RUSTFLAGS=-Zsanitizer=leak` is NOT used (needs nightly).

## 5. Tasks

- **A — Rust core** (`src/core/**`, `benches/**`, Rust unit tests incl. fake-clock TTL tests, eviction tests, glob tests, concurrency test with 8 threads). `cargo test --no-default-features` must pass (see Cargo features: `python` feature gates PyO3).
- **A2 — RESP server** (`src/server/**`) after A: parser + command dispatch + Rust integration test over a real `TcpStream`.
- **B — Python binding & tests** (`src/lib.rs`, `python/**`, `tests/python/**`) after A: wraps API contract plus section 4a JSON support, GIL release, `.pyi`, pytest suite (≥ 40 tests: types, TTL with real sleeps, eviction, threads, memoize, server via raw socket).
- **C — CI/CD & docs** (`.github/**`, `README.md`, `docs/**`, `CHANGELOG.md`, `LICENSE` MIT). No Rust/Python source edits.
  - `ci.yml`: on push/PR: `cargo fmt --check`, `clippy -D warnings`, `cargo test`; maturin develop + pytest on matrix {ubuntu, macos, windows} x python {3.9,3.10,3.11,3.12,3.13,3.14}.
  - `release.yml`: `on: release: types: [published]` ONLY. Jobs: build wheels (PyO3/maturin-action; linux x86_64+aarch64 manylinux & musllinux, macOS x86_64+arm64, windows x64; abi3 so one interpreter per target), sdist, smoke-test installed wheel on 3.9 and 3.14, then `publish` job with `needs:` all, using `pypa/gh-action-pypi-publish` and `password: ${{ secrets.PYPI_API_TOKEN }}` (repository secret named `PYPI_API_TOKEN`). Also verify the release tag equals the Cargo/pyproject version before publishing.
  - Docs: README (install, quickstart, API table, benchmarks placeholder, server mode), docs/architecture.md, docs/api.md, docs/releasing.md (how to add the secret, bump version, cut release).

Order: A → (A2 ∥ B ∥ C). Then lead verifies: `cargo test`, `maturin build`, `pytest`.
