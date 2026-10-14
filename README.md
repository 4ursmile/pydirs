# tinycache

[![CI](https://github.com/4ursmile/tinycache/actions/workflows/ci.yml/badge.svg)](https://github.com/4ursmile/tinycache/actions/workflows/ci.yml)
[![Release](https://github.com/4ursmile/tinycache/actions/workflows/release.yml/badge.svg)](https://github.com/4ursmile/tinycache/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/tinycache-rs.svg)](https://pypi.org/project/tinycache-rs/)
[![Python](https://img.shields.io/pypi/pyversions/tinycache-rs.svg)](https://pypi.org/project/tinycache-rs/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`tinycache` is a lightweight, in-process, Redis-like cache. The engine is written
in Rust and exposed to Python through PyO3. It supports strings, hashes, lists and
sets, key expiry, optional memory limits with LRU eviction, a `memoize` decorator,
and an optional RESP2 server that `redis-py` clients can talk to.


## Installation

```bash
pip install tinycache-rs
```

The PyPI distribution is `tinycache-rs`; the import name is `tinycache`.

## Quickstart

```python
from tinycache import Cache

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
| Server | `Server(cache=c, host=..., port=...)`, `start()`, `stop()`, `port` |

Full signatures, return values and error behaviour are in
[docs/api.md](docs/api.md).

## JSON values

Values can be stored as JSON using the standard library `json` module. No pickle
is involved, so stored data is plain, portable JSON.

```python
from tinycache import Cache

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
pip install "tinycache-rs[pydantic]"
```

`tinycache` imports pydantic lazily. Without pydantic installed, everything else
works unchanged, and calling a pydantic method raises `ImportError` with an
install hint. The pydantic API used is v2 only (`TypeAdapter`), with
`pydantic>=2.5`.

```python
from pydantic import BaseModel
from tinycache import Cache

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
from tinycache import Cache

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

`tinycache` can also serve the cache over TCP using the Redis wire protocol,
so existing Redis clients can connect to it.

```python
from tinycache import Cache, Server

cache = Cache()
server = Server(cache=cache, host="127.0.0.1", port=6380)
server.start()          # runs in a background thread
print("listening on", server.port)

# ... later
server.stop()
```

Connect with `redis-py`:

```python
import redis

r = redis.Redis(host="127.0.0.1", port=6380)
r.set("answer", 42, ex=30)
print(r.get("answer"))          # b'42'
r.hset("user:1", mapping={"name": "Ada"})
print(r.hgetall("user:1"))
```

Supported commands: `PING`, `GET`, `SET` (with `EX`, `PX`, `NX`, `XX`), `DEL`,
`EXISTS`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `INCR`, `INCRBY`, `DECR`, `MGET`,
`MSET`, `KEYS`, `DBSIZE`, `FLUSHALL`, `HSET`, `HGET`, `HDEL`, `HGETALL`, `LPUSH`,
`RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`, `SADD`, `SREM`, `SMEMBERS`,
`SISMEMBER`, `QUIT`.

The server uses one thread per connection and the standard library only. It is
intended for development, testing and local tooling, not as a hardened
production network service. Bind to `127.0.0.1` unless you know what you are
doing.

## Performance

Measured with `benchmarks/compare.py` on one macOS machine (x86_64 build,
CPython 3.13, Redis 8.6.1 on localhost, 64-byte values, 10k keys). Single run,
not averaged, so treat it as indicative. Re-run it on your own hardware.

**Python client (ops/s).** tinycache runs in-process, Redis over TCP, so this
shows the cost of the network hop, not a like-for-like engine comparison.

| | SET | GET |
|---|---:|---:|
| Python `dict` | 3,410,059 | 5,008,027 |
| `tinycache.Cache` (in-process) | 649,073 | 1,058,776 |
| redis-py to redis-server | 9,948 | 10,631 |

**Wire protocol** (`tinycache.Server` vs `redis-server`, `redis-benchmark`, 50 clients, req/s).

| | pipeline 1 | | pipeline 16 | |
|---|---:|---:|---:|---:|
| | tinycache | redis | tinycache | redis |
| SET | 69,348 | 72,411 | 163,265 | 560,224 |
| GET | 67,069 | 70,822 | 862,069 | 719,424 |
| INCR | 71,225 | 74,991 | 179,533 | 641,026 |

**Size.** Wheel 333 KB (extension module 646 KB, sdist 77 KB), against a 2.0 MB
`redis-server` binary.

**Memory, 1M keys x 64 B values.** Redis grew by 128 MB. tinycache grew by
about 500 MB RSS, although its own `used_memory` estimate says 131 MB, so the
accounting undercounts and `max_memory` limits are not accurate yet.

### Pros and cons

Pros: `pip install` with no server to run; about 100x faster than redis-py for
local calls because there is no network; GIL released so threads run in
parallel; one abi3 wheel covers Python 3.9 to 3.14; small binary; JSON and
pydantic helpers and `memoize` built in; optional RESP2 server for redis clients.

Cons: not a distributed or persistent store (no replication, clustering, disk
persistence, pub/sub or Lua); the cache is per process, so workers do not share
it unless you run the server; pipelined SET and INCR over the server are 3 to 4
times slower than Redis; memory use is about 4x Redis for small keys and the
accounting is inaccurate; a plain `dict` is about 3 to 5 times faster for a
single-process cache with no TTL needs; far less mature and tested than Redis.

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
pip install maturin pytest
maturin develop --release
pytest
```

## License

MIT. See [LICENSE](LICENSE).
