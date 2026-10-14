# pydirs

[![CI](https://github.com/4ursmile/pydirs/actions/workflows/ci.yml/badge.svg)](https://github.com/4ursmile/pydirs/actions/workflows/ci.yml)
[![Release](https://github.com/4ursmile/pydirs/actions/workflows/release.yml/badge.svg)](https://github.com/4ursmile/pydirs/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/pydirs.svg)](https://pypi.org/project/pydirs/)
[![Python](https://img.shields.io/pypi/pyversions/pydirs.svg)](https://pypi.org/project/pydirs/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`pydirs` is a lightweight, in-process, Redis-like cache. The engine is written
in Rust and exposed to Python through PyO3. It supports strings, hashes, lists and
sets, key expiry, optional memory limits with LRU eviction, a `memoize` decorator,
and an optional RESP2 server that `redis-py` clients can talk to.


## Installation

```bash
pip install pydirs
```

The PyPI distribution is `pydirs`; the import name is `pydirs`.

## Quickstart

```python
from pydirs import Cache

with Cache() as c:
    c.set("greeting", "hello", ex=60)   # expires in 60 seconds
    print(c.get("greeting"))            # b'hello'

    c.incr("visits")                    # 1
    c.incr("visits")                    # 2

    c.hset("user:1", "name", "Ada")
    print(c.hgetall("user:1"))

    c.rpush("jobs", "a", "b", "c")
    print(c.lpop("jobs"))               # b'a'
```

Reads return `bytes` by default. Pass `Cache(decode=True)` to get `str` back.

## API overview

| Group | Methods |
|---|---|
| Strings | `set`, `get`, `mget`, `mset`, `incr`, `decr`, `append` |
| Keys | `delete`, `exists`, `expire`, `pexpire`, `persist`, `ttl`, `pttl`, `keys`, `len(c)`, `flush`, `used_memory` |
| Hashes | `hset`, `hget`, `hdel`, `hgetall`, `hlen` |
| Lists | `lpush`, `rpush`, `lpop`, `rpop`, `lrange`, `llen` |
| Sets | `sadd`, `srem`, `smembers`, `sismember`, `scard` |
| JSON | `set_json`, `get_json`, `mget_json`, `mset_json`, `hset_json`, `hget_json`, `lpush_json`, `rpush_json`, `lrange_json`, `Cache(serializer="json")` |
| Sugar | `c[k] = v`, `c[k]`, `del c[k]`, `k in c` |
| Decorator | `@c.memoize(ttl=...)` |
| Pydantic | `set_model`, `get_model`, `mget_model`, `hset_model`, `hget_model`, `@c.memoize` returning model instances |
| Persistence | `Cache(persist_dir=..., fsync=..., snapshot_interval=...)`, `save()`, `persistent`, `persist_stats` |
| Server | `Server(cache=c, host=..., port=..., password=...)`, `start()`, `stop()`, `port`, `running` |
| Client | `Client(host, port, password, timeout, pool_size)`, same methods as `Cache`, `pipeline()`, `ping()`, `info()`, `save()`, `bgsave()`, `lastsave()` |
| CLI | `pydirs-server` / `python -m pydirs` |

Full signatures, return values and error behaviour are in
[docs/api.md](docs/api.md).

## JSON values

Values can be stored as JSON using the standard library `json` module. No pickle
is involved, so stored data is plain, portable JSON.

```python
from pydirs import Cache

c = Cache()
c.set_json("user:1", {"name": "Ada", "tags": ["math", "engines"]}, ex=300)
user = c.get_json("user:1")            # {'name': 'Ada', 'tags': ['math', 'engines']}
c.get_json("missing", default={})      # {}

c.mset_json({"a": [1, 2], "b": None})
c.mget_json(["a", "b", "nope"])        # [[1, 2], None, None]

c.hset_json("profile", "prefs", {"theme": "dark"})
c.hget_json("profile", "prefs")        # {'theme': 'dark'}

c.rpush_json("events", {"n": 1}, {"n": 2})
c.lrange_json("events", 0, -1)         # [{'n': 1}, {'n': 2}]
```

To make a whole cache JSON-based, use `serializer="json"`. Then `set`, `get`,
`mget`, `mset`, `c[k]` and `memoize` all go through JSON:

```python
c = Cache(serializer="json")
c["config"] = {"retries": 3, "hosts": ["a", "b"]}
c["config"]                            # {'retries': 3, 'hosts': ['a', 'b']}
```

Only JSON-compatible values are accepted: `dict`, `list`, `str`, `int`, `float`,
`bool` and `None`. Other objects raise `TypeError`. `NaN` and `Infinity` raise
`ValueError`. A stored value that is not valid JSON raises `ValueError` on read.

## Expiry (TTL)

Keys can be given a time-to-live with `ex` (seconds) or `px` (milliseconds) on
`set`, or with `expire` / `pexpire` on an existing key.

```python
c.set("session", "abc", px=1500)
c.ttl("session")      # seconds remaining, -1 if no expiry, -2 if missing
c.pttl("session")     # milliseconds remaining
c.persist("session")  # remove the expiry
```

Expiry works in two ways:

- **Lazy:** an expired key is treated as missing the moment it is read.
- **Background sweep:** a sweeper thread samples keys every 100 ms and removes
  expired ones, so memory is reclaimed even for keys that are never read again.
  The sweeper stops when the cache is closed.

## Memory limits and eviction

```python
c = Cache(max_memory=64 * 1024 * 1024, policy="allkeys-lru")
```

- `max_memory` is in bytes. `0` (the default) means unlimited.
- Memory accounting is approximate and per entry.
- `policy` selects what happens when the limit is exceeded:
  - `"noeviction"` (default): writes that would exceed the limit raise `MemoryError`.
  - `"allkeys-lru"`: evict the least recently used keys, from any key.
  - `"volatile-lru"`: evict the least recently used keys, only among keys with a TTL.

Eviction uses sampled LRU (similar to Redis): a small random sample of keys is
checked and the oldest one is evicted, repeating until usage is under the limit.

## Pydantic models

Pydantic support is optional. Install it with the extra:

```bash
pip install "pydirs[pydantic]"
```

`pydirs` imports pydantic lazily. Without pydantic installed, everything else
works unchanged, and calling a pydantic method raises `ImportError` with an
install hint. The pydantic API used is v2 only (`TypeAdapter`), with
`pydantic>=2.5`.

```python
from pydantic import BaseModel
from pydirs import Cache

class User(BaseModel):
    id: int
    name: str
    tags: list[str] = []

c = Cache()
c.set_model("user:1", User(id=1, name="Ada", tags=["math"]), ex=300)
user = c.get_model("user:1", User)      # User(id=1, name='Ada', tags=['math'])

c.set_model("users", [User(id=1, name="Ada")])
c.get_model("users", list[User])        # [User(id=1, name='Ada', tags=[])]

c.mget_model(["user:1", "nope"], User)  # [User(...), None]

c.hset_model("accounts", "ada", User(id=1, name="Ada"))
c.hget_model("accounts", "ada", User)   # User(id=1, name='Ada', tags=[])
```

`get_model` returns `default` (`None` unless given) for a missing key. If the
stored JSON does not match the model, it raises `pydantic.ValidationError`,
which is a subclass of `ValueError`.

Memoized functions return real model instances when the return annotation
names a pydantic type:

```python
@c.memoize(ttl=60)
def load_user(user_id: int) -> User:
    return User(id=user_id, name="Ada")

load_user(1)   # User instance, computed
load_user(1)   # User instance, from cache

@c.memoize(ttl=60, model=User)         # explicit model overrides the annotation
def load_any(user_id: int):
    return {"id": user_id, "name": "Ada"}

@c.memoize(ttl=60)                     # async functions are supported
async def fetch_user(user_id: int) -> User:
    ...
```

## The `memoize` decorator

```python
from pydirs import Cache

cache = Cache(serializer="json")

@cache.memoize(ttl=60)
def fetch_user(user_id: int) -> dict:
    ...  # expensive call, returns a dict

fetch_user(42)   # computed
fetch_user(42)   # served from cache for 60 seconds, as a dict
```

The cache key is built from the function's module and qualified name plus the
`repr` of its positional arguments and sorted keyword arguments. Make sure
arguments have a stable `repr`.

With the default `serializer="raw"`, the return value must be `bytes`, `str`,
`int` or `float`. With `serializer="json"`, dicts and lists work too.

## Server mode (RESP2)

`pydirs` can also serve the cache over TCP using the Redis wire protocol,
so existing Redis clients can connect to it.

```python
from pydirs import Cache, Server

cache = Cache()
with Server(cache, host="127.0.0.1", port=6380) as srv:   # starts; stops on exit
    print("listening on", srv.port)
    ...
```

`srv.port` is the port actually bound, so `port=0` works. Connect with
`redis-py`:

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6380)
r.set("answer", 42, ex=30)
print(r.get("answer"))          # b'42'
r.hset("user:1", mapping={"name": "Ada"})
print(r.hgetall("user:1"))
```

Supported commands: `PING`, `ECHO`, `GET`, `SET` (with `EX`, `PX`, `NX`, `XX`),
`SETEX`, `DEL`, `EXISTS`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `TTL`, `PTTL`, `INCR`,
`INCRBY`, `DECR`, `DECRBY`, `APPEND`, `MGET`, `MSET`, `KEYS`, `DBSIZE`,
`FLUSHALL`, `FLUSHDB`, `HSET`, `HGET`, `HDEL`, `HGETALL`, `HLEN`, `LPUSH`,
`RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `SADD`, `SREM`, `SMEMBERS`,
`SISMEMBER`, `SCARD`, `SELECT` (database 0 only), `COMMAND`, `AUTH`, `INFO`,
`SAVE`, `BGSAVE`, `LASTSAVE`, `QUIT`. With a password set, every command except
`AUTH` and `QUIT` needs an authenticated connection (`NOAUTH` otherwise).

The server uses one thread per connection and the standard library only. It is
for development, testing and local tooling, not a hardened production network
service. Bind to `127.0.0.1`.

## Persistence

By default a cache lives only in memory. Pass `persist_dir` to keep it on disk:
the data is reloaded when the cache is opened again.

```python
from pydirs import Cache

with Cache(persist_dir="/tmp/tc-demo", fsync="everysec") as c:
    c.set("greeting", "hello")
    c.hset("user:1", "name", "Ada")
    c.save()                        # snapshot now, optional
    print(c.persist_stats["snapshot_bytes"])

# A later run (or process) sees the same data.
with Cache(persist_dir="/tmp/tc-demo") as c:
    print(c.get("greeting"))        # b'hello'
```

Leaving the `with` block writes a final snapshot. `fsync` picks when WAL writes
are flushed to disk (`always`, `everysec` or `never`). Background snapshots run
every `snapshot_interval` seconds (default 300) when there are unsaved writes.

Read [docs/persistence.md](docs/persistence.md) before relying on it. In brief:

- A process kill (SIGKILL) is recovered. Power-loss durability is **not
  verified** for any fsync policy.
- A damaged snapshot makes the open fail with `CorruptDataError`. The store does
  not fall back to older data.
- Only one process may own a persist directory. Others should use `Client`.
- Single node only. Persistence does not replicate the data.

### Run as a service

Run the server with a data directory and a password, and let clients connect:

```bash
export PYDIRS_PASSWORD='choose-a-long-password'
pydirs-server --host 0.0.0.0 --port 6380 --dir /var/lib/pydirs \
    --fsync everysec --snapshot-interval 300
```

A public bind needs a password, and the server refuses to start without one.
Clients then authenticate:

```python
from pydirs import Client

client = Client(host="10.0.0.5", port=6380, password="choose-a-long-password")
client.set("answer", 42)
print(client.get("answer"))         # b'42'
print(client.info("persistence"))   # dict of persistence stats
client.bgsave()
```

A `systemd` unit and a Dockerfile are in
[docs/persistence.md](docs/persistence.md#running-as-a-service-untested-examples).
They are untested examples.

## Sharing a cache between processes

A `Cache` lives inside one process, so workers in different processes do not
see each other's data. To share it, run a server once and connect each worker
with `pydirs.Client`, which has the same methods as `Cache`.

```bash
pydirs-server --port 6380 --max-memory 256mb --policy allkeys-lru
# or: python -m pydirs --port 6380
```

The command prints a banner with the bound port and runs until Ctrl-C or
SIGTERM. Run `pydirs-server --help` for all options. Binding a non-loopback
address is refused unless you pass `--allow-insecure`, because password
enforcement is not available in this release.

```python
from pydirs import Client

client = Client(host="127.0.0.1", port=6380, pool_size=8, timeout=5.0)
client.set("answer", 42, ex=30)
client.get("answer")              # b'42'

with client.pipeline() as p:      # several commands, one write
    p.set("a", 1).incr("n").hset("h", "f", "v")
print(p.results)                  # [True, 1, True]
```

A `Client` is thread-safe and pools its connections. Idempotent commands (reads,
plain `SET`, `EXPIRE`, `FLUSHALL`) are retried once after a dropped connection,
so a server restart is usually invisible. Commands that change data
non-idempotently (`INCR`, `APPEND`, `LPUSH`, `LPOP`, `DEL`, `HSET`, and so on)
are never retried, so a failed one raises `ConnectionError` and may or may not
have run. Full rules and error mapping are in [docs/api.md](docs/api.md#client).

### Gunicorn and uvicorn workers

Start the server first, then give each worker its own `Client`. Create the
client after the worker forks, so no socket is shared across processes.

```python
# gunicorn.conf.py or app module
from pydirs import Client

_client = None

def get_client():
    global _client
    if _client is None:
        _client = Client(host="127.0.0.1", port=6380)
    return _client
```

```bash
gunicorn -w 4 myapp:app
uvicorn myapp:app --workers 4
```

`myapp` must call `get_client()` from request handlers (or from a `post_fork`
hook in gunicorn), not at import time, so each worker opens its own
connections.

## Performance

Measured with `benchmarks/compare.py` on one macOS machine (x86_64 build,
CPython 3.13, Redis 8.6.1 on localhost, 64-byte values, 10k keys). Single run,
not averaged, so treat it as indicative. Re-run it on your own hardware.

**Python client (ops/s).** pydirs runs in-process, Redis over TCP, so this
shows the cost of the network hop, not a like-for-like engine comparison.

| | SET | GET |
|---|---:|---:|
| Python `dict` | 3,410,059 | 5,008,027 |
| `pydirs.Cache` (in-process) | 649,073 | 1,058,776 |
| redis-py to redis-server | 9,948 | 10,631 |

**Wire protocol** (`pydirs.Server` vs `redis-server`, `redis-benchmark`, 50 clients, req/s).

| | pipeline 1 | | pipeline 16 | |
|---|---:|---:|---:|---:|
| | pydirs | redis | pydirs | redis |
| SET | 69,348 | 72,411 | 163,265 | 560,224 |
| GET | 67,069 | 70,822 | 862,069 | 719,424 |
| INCR | 71,225 | 74,991 | 179,533 | 641,026 |

**Size.** Wheel 333 KB (extension module 646 KB, sdist 77 KB), against a 2.0 MB
`redis-server` binary.

**Memory, 1M keys x 64 B values.** Redis grew by 129 MB, pydirs by 187 MB
(1.45x Redis; it was 501 MB before the memory rework). `used_memory` reports
158 MB, within 20% of the real growth, so `max_memory` limits are now close to
honest. On macOS freed memory is not always returned to the OS, so RSS after
`flush()` stays high even though the allocator holds nothing live.

### Pros and cons

Pros: `pip install` with no server to run; about 100x faster than redis-py for
local calls because there is no network; GIL released so threads run in
parallel; one abi3 wheel covers Python 3.9 to 3.14; small binary; JSON and
pydantic helpers and `memoize` built in; optional RESP2 server for redis clients.

Cons: single node only (no replication, clustering, failover, pub/sub or Lua).
Persistence is opt-in and writes to one local directory, and its durability
limits are documented in [docs/persistence.md](docs/persistence.md). A `Cache`
is per process, so workers share data only through a running server.
Pipelined SET and INCR over the server are 3 to 4 times slower than Redis. Memory
use is about 1.5 times Redis for small keys. A plain `dict` is about 3 to 5 times
faster for a single-process cache with no TTL needs. The project is far less
mature and tested than Redis.

## Supported platforms

Prebuilt wheels are published for:

| OS | Architectures |
|---|---|
| Linux (manylinux) | x86_64, aarch64 |
| Linux (musllinux) | x86_64, aarch64 |
| macOS | x86_64, arm64 (aarch64) |
| Windows | x64 |

Python **3.9 through 3.14** are supported. The extension is built against the
CPython stable ABI (`abi3-py39`), so a single wheel per platform covers every
supported Python version. If no wheel matches your platform, `pip` will try to
build from the source distribution, which requires a Rust toolchain.

## Development

```bash
# Rust
cargo fmt --all -- --check
cargo clippy --no-default-features --all-targets -- -D warnings
cargo test --no-default-features

# Python
pip install maturin "pytest>=7" pytest-asyncio pydantic
maturin develop --release
pytest -m "not leak"
```

Memory-leak tests carry the `leak` pytest marker. The CI `test` job runs
`pytest -m "not leak"`, and a separate `leaks` job runs `pytest -m leak` on Linux.
A plain `pytest` run includes them as well. To run only the leak tests:

```bash
pytest -m leak
```

The Rust leak checks run under valgrind on Linux in CI. See
[docs/architecture.md](docs/architecture.md#testing-for-leaks).

## License

MIT. See [LICENSE](LICENSE).
