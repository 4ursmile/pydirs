# Python API reference

```python
from pydirs import Cache, Client, Server, Pipeline, ResponseError
from pydirs import JsonSerializer, PydanticSerializer, Serializer
```

`Cache` (in-process) and `Client` (talks to a running server) share one API.
Every method below exists on both unless marked otherwise. Keys are `str`,
`bytes`, `int` or `float`. Values are `bytes`, `str`, `int` or `float` on the
raw path; JSON and pydantic methods accept richer objects.

Reads return `bytes` by default. With `decode=True` they return `str` instead,
for single values and for the items of `keys`, `hgetall`, `smembers`, `lrange`
and `hget`/`lpop`/`rpop`.

## Errors

| Condition | Python exception |
|---|---|
| Operation on a key holding a different type (`WRONGTYPE`) | `TypeError` |
| Write rejected under `policy="noeviction"` when `max_memory` is full (`OOM`) | `MemoryError` |
| `incr` / `decr` / `INCRBY` on a value that is not an integer | `ValueError` |
| Integer overflow in `incr` / `decr` | `ValueError` |
| Invalid argument (unknown `policy` or `serializer`, bad TTL, `nx` with `xx`, `ex` with `px`) | `ValueError` |
| Argument of an unsupported type (for example `bool` or `None` as a key) | `TypeError` |
| Non-serialisable object passed to a `*_json` method, or to `set`/`mset`/`memoize` with a JSON serializer | `TypeError` |
| `NaN` or `Infinity` passed as a JSON value | `ValueError` |
| Stored bytes that are not valid JSON, read through a JSON path | `ValueError` |
| Stored data that does not match the requested pydantic model | `pydantic.ValidationError` (subclass of `ValueError`) |
| A pydantic method or pydantic-typed `memoize` used without pydantic installed | `ImportError` with the install hint |
| Persistence directory held by another live process (`LOCK`) | `CacheLockedError` (subclass of `OSError`) |
| Snapshot in the persistence directory fails its checksum or structure check | `CorruptDataError` (subclass of `ValueError`) |
| I/O failure opening or writing the persistence directory | `OSError` |
| `save()` / `SAVE` when persistence is off | `ValueError` |
| `Client` cannot connect, or the connection drops and a retry is not allowed | `ConnectionError` |
| Missing or wrong password (`NOAUTH`, `WRONGPASS`) on a `Client` | `AuthenticationError` (subclass of `ConnectionError`) |
| Server replies with an error that has no more specific mapping (for example an unknown command) | `ResponseError` (on `Client`) |
| `used_memory` on a `Client` against a server without `INFO` | `NotImplementedError` |

## `Cache`

### `Cache(max_memory=0, policy="noeviction", shards=64, decode=False, serializer="raw", persist_dir=None, fsync="everysec", snapshot_interval=300)`

Creates an in-process cache. Without `persist_dir` the data lives only in memory.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `max_memory` | `int` | `0` | Approximate memory limit in bytes. `0` means unlimited. |
| `policy` | `str` | `"noeviction"` | One of `"noeviction"`, `"allkeys-lru"`, `"volatile-lru"`. |
| `shards` | `int` | `64` | Number of shards. Must be a power of two. |
| `decode` | `bool` | `False` | When `True`, reads return `str` instead of `bytes` (see the list at the top). |
| `serializer` | `str` or object | `"raw"` | See [Serializers](#serializers). |
| `persist_dir` | `str`, path, or `None` | `None` | Directory for snapshots and the write-ahead log. Loads any existing data on open. Off when `None`. See [docs/persistence.md](persistence.md). |
| `fsync` | `str` | `"everysec"` | WAL fsync policy: `"always"`, `"everysec"` or `"never"`. Any other value raises `ValueError`. Only used with `persist_dir`. |
| `snapshot_interval` | `int` | `300` | Seconds between background snapshots, only when there are unsaved writes. `0` disables the timer. Negative raises `ValueError`. |

Opening with `persist_dir` can raise `CacheLockedError` (another process owns
the directory) or `CorruptDataError` (the snapshot is damaged).

`Cache` is a context manager. Leaving the `with` block calls `close()`, which
writes a final snapshot when persistence is on.

### `close() -> None`

Stops the background threads, and, when persistence is on, writes a final
snapshot, syncs the log, and releases the directory so another `Cache` (or the
next process) can open it. Safe to call more than once.

The store stays readable after `close()`. Writes made after it are not logged,
so they are not durable.

### Persistence

Persistence is available on `Cache` only. `Client` reaches a persistent server
through `save`, `bgsave` and `lastsave` (see [Server administration](#server-administration)).

#### `save() -> None`

Writes a snapshot now and returns when it is complete. Raises `ValueError` if
the cache has no `persist_dir`, and `OSError` on an I/O failure.

#### `persistent -> bool` (property)

True when the cache was opened with `persist_dir`.

#### `persist_stats -> dict | None` (property)

`None` when persistence is off. Otherwise a dict with these keys:

| Key | Type | Meaning |
|---|---|---|
| `last_save_ms` | `int` | Wall-clock milliseconds of the last successful snapshot in this process, or `0`. |
| `snapshot_bytes` | `int` | Size of the current `snapshot.tcs` in bytes, or `0`. |
| `wal_bytes` | `int` | Bytes in the active WAL generation. |
| `loaded_keys` | `int` | Live keys present after recovery. |
| `last_error` | `str` or `None` | The latest persistence error or recovery warning. A successful save clears it. |

```python
with Cache(persist_dir="/tmp/tc-data") as c:
    c.set("k", "v")
    c.save()
    print(c.persist_stats["snapshot_bytes"] > 0)
```

### Serializers

`serializer` selects how values become bytes on write and back on read.

- `"raw"` (default): bytes, `str`, `int` and `float` are stored as they are.
  Reads return bytes (or `str` with `decode=True`).
- `"json"`: `set`, `get`, `mget`, `mset`, dict-style access and `memoize` go
  through the stdlib `json` module, so any JSON-compatible value round-trips.
  Values are compact and `ensure_ascii=False`, stored as UTF-8.
- An object with `dumps(obj) -> bytes | str | int | float` and
  `loads(data) -> obj` methods. `dumps` produces a value the core accepts, and
  `loads` receives the bytes read back. The `Serializer` protocol in
  `pydirs` describes this. `JsonSerializer` and `PydanticSerializer` are
  the built-in implementations. `PydanticSerializer(model=None)` needs pydantic.

Any other `serializer` value raises `ValueError`.

A serializer applies to `set`, `get`, `mget`, `mset`, `c[k]` and `memoize`. It
does not apply to the explicit `*_json` and `*_model` methods, which always use
their own format, and it does not apply to `append`, which always stores raw bytes.

### Strings

#### `set(key, value, ex=None, px=None, nx=False, xx=False) -> bool`

Stores `value` under `key`.

- `ex`: expiry in seconds. Must be positive. Mutually exclusive with `px`.
- `px`: expiry in milliseconds. Must be positive.
- `nx`: only set if the key does not exist.
- `xx`: only set if the key already exists. Mutually exclusive with `nx`.

Returns `True` when the value was stored, `False` when an `nx` or `xx`
condition was not met.

#### `get(key) -> Any`

Returns the value, or `None` if the key is missing or expired. Under
`serializer="json"`, a stored JSON `null` returns `None` too, which is not
distinguishable from a missing key through `get`. Use `exists` if that matters.

#### `mget(keys) -> list`

Returns one value per key, in order. Missing keys, and keys holding a
non-string type, give `None`.

#### `mset(mapping) -> None`

Stores every entry of `mapping`. All values are encoded before any write, so a
serialisation error writes nothing.

#### `incr(key, amount=1) -> int`

Adds `amount` to the integer stored at `key`. A missing key is treated as `0`.
Returns the new value. Raises `ValueError` if the stored value is not an
integer, or on overflow. Amount must fit in a signed 64-bit integer.

#### `decr(key, amount=1) -> int`

Subtracts `amount`. Same behaviour as `incr`.

#### `append(key, value) -> int`

Appends raw bytes to the string at `key`, creating it if missing. Returns the
new length. `append` bypasses the serializer.

### Keys

#### `delete(*keys) -> int`

Deletes the given keys. Returns how many existed.

#### `exists(key) -> bool`

Returns `True` if the key holds a live value.

#### `expire(key, seconds) -> bool`

Sets a time-to-live in whole seconds. Returns `False` for a missing key.

#### `pexpire(key, milliseconds) -> bool`

Same as `expire`, in milliseconds.

#### `persist(key) -> bool`

Removes any expiry. Returns `False` if the key is missing or has no expiry.

#### `ttl(key) -> int`

Remaining TTL in whole seconds, rounded up. `-2` if the key does not exist,
`-1` if it has no expiry.

#### `pttl(key) -> int`

Remaining TTL in milliseconds. Same sentinel values as `ttl`.

#### `keys(pattern="*") -> list`

Returns the live keys matching a glob pattern. Supported syntax: `*`, `?`,
`[abc]` and backslash escapes. This scans the whole keyspace, so avoid it on
large caches in hot paths.

#### `len(c) -> int`

Number of live keys.

#### `flush() -> None`

Removes all keys.

#### `used_memory -> int` (property)

Approximate bytes in use, as tracked for `max_memory`. See
[architecture.md](architecture.md#memory-accounting) for what is counted.

### Hashes

#### `hset(key, field, value) -> bool`

Sets a field. Returns `True` if the field is new.

#### `hget(key, field) -> Any`

Returns the field value, or `None`.

#### `hdel(key, *fields) -> int`

Deletes fields and returns how many were removed. The key is removed when its
last field is deleted.

#### `hgetall(key) -> dict`

Returns all fields and values as a `dict`. Empty for a missing key. With
`decode=True` both fields and values are `str`.

#### `hlen(key) -> int`

Number of fields.

### Lists

#### `lpush(key, *values) -> int`

Prepends each value in turn, as Redis does, so the last argument ends up first.
Returns the new length.

#### `rpush(key, *values) -> int`

Appends values. Returns the new length.

#### `lpop(key) -> Any`

Removes and returns the head, or `None` for a missing or empty list.

#### `rpop(key) -> Any`

Removes and returns the tail. Same empty behaviour as `lpop`.

#### `lrange(key, start, stop) -> list`

Returns elements from `start` to `stop`, inclusive. Negative indices count from
the end (`-1` is the last element).

#### `llen(key) -> int`

Number of elements.

### Sets

#### `sadd(key, *members) -> int`

Adds members and returns how many were new.

#### `srem(key, *members) -> int`

Removes members and returns how many were present.

#### `smembers(key) -> set`

Returns all members as a `set`.

#### `sismember(key, member) -> bool`

True if `member` is in the set.

#### `scard(key) -> int`

Number of members.

### Dict-style access

```python
c["k"] = "v"     # same as c.set("k", "v")
c["k"]           # same as c.get("k"); raises KeyError only when the key is missing
del c["k"]       # same as c.delete("k"); raises KeyError if nothing was deleted
"k" in c         # same as c.exists("k")
len(c)           # same as len of live keys
```

`c["k"]` treats a stored JSON `null` as present and returns `None`. It raises
`KeyError` only when the key is missing or expired.

### JSON methods

The `*_json` methods use the stdlib `json` module only (never pickle), with
compact separators, `ensure_ascii=False`, and `allow_nan=False`. JSON-compatible
values are `dict`, `list`, `str`, `int` (including big integers), `float`,
`bool` and `None`.

| Method | Returns |
|---|---|
| `set_json(key, obj, ex=None, px=None, nx=False, xx=False)` | `bool`, as `set` |
| `get_json(key, default=None)` | decoded object, or `default` if missing |
| `mget_json(keys)` | `list`, `None` for missing keys |
| `mset_json(mapping)` | `None` |
| `hset_json(key, field, obj)` | `bool`, `True` if the field is new |
| `hget_json(key, field, default=None)` | decoded object, or `default` if missing |
| `lpush_json(key, *objs)` | `int`, the new length (same order as `lpush`) |
| `rpush_json(key, *objs)` | `int`, the new length |
| `lrange_json(key, start, stop)` | `list` of decoded objects |

`set_json` and the other `*_json` methods always write JSON, whatever the
cache's serializer is. `get_json` reads whatever bytes are stored, so a value
written with `set("abc")` (the bytes `abc`) is not valid JSON and raises
`ValueError`.

```python
c = Cache()
c.set_json("user:1", {"name": "Ada", "tags": ["math"]}, ex=300)
c.get_json("user:1")              # {'name': 'Ada', 'tags': ['math']}
c.get_json("missing", {})         # {}
c.mset_json({"a": [1, 2], "b": None})
c.mget_json(["a", "b", "nope"])   # [[1, 2], None, None]
```

### Pydantic methods

Pydantic support is optional. Install it with
`pip install "pydirs[pydantic]"` (which requires `pydantic>=2.5`, v2 API
only). The package imports and works without pydantic. Pydantic is imported
only when one of these methods, or a pydantic-typed `memoize`, is used.
Without it they raise `ImportError` with the hint
`pydantic support requires pydantic; install it with: pip install 'pydirs[pydantic]'`.

Models are stored as the JSON from `TypeAdapter(type).dump_json`, and read back
with `TypeAdapter(model).validate_json`. Any type pydantic can serialise works:
`BaseModel`, dataclasses, `TypedDict`, `list[Model]`, `dict[str, Model]`,
`Optional[Model]`, and so on. Adapters are cached per type.

| Method | Returns |
|---|---|
| `set_model(key, obj, ex=None, px=None, nx=False, xx=False)` | `bool`. Serialises by the runtime type of `obj`. |
| `get_model(key, model, default=None)` | validated instance, or `default` if missing |
| `mget_model(keys, model)` | `list`, `None` for missing keys |
| `hset_model(key, field, obj)` | `bool`, `True` if the field is new |
| `hget_model(key, field, model, default=None)` | validated instance, or `default` if missing |

`get_model` and the other read methods raise `pydantic.ValidationError` if the
stored data does not match `model`. Corrupt bytes that are not JSON at all raise
`ValueError`.

```python
from pydantic import BaseModel
from pydirs import Cache

class User(BaseModel):
    id: int
    name: str

c = Cache()
c.set_model("user:1", User(id=1, name="Ada"))
c.get_model("user:1", User)                 # User(id=1, name='Ada')
c.get_model("missing", User)                # None
c.set_model("users", [User(id=1, name="Ada")])
c.get_model("users", list[User])            # [User(id=1, name='Ada')]
```

`PydanticSerializer(model)` provides the same behaviour through the serializer
protocol, so `Cache(serializer=PydanticSerializer(User))` routes `set`/`get`
through it.

### `memoize(ttl=None, model=None)`

Decorator that caches the results of a function in this cache. It works on sync
and async functions.

```python
@cache.memoize(ttl=60)
def compute(x, y=0):
    ...
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ttl` | `int \| float \| None` | `None` | Seconds to keep each result. `None` means no expiry. Must be positive. |
| `model` | type annotation or `None` | `None` | Type used to store and rebuild results. Overrides the return annotation. |

#### Cache key

The key is `f"{func.__module__}.{func.__qualname__}"` followed by the positional
arguments and the keyword arguments (sorted by name). Each argument is turned
into text with `repr`. A pydantic model instance is keyed by its
`model_dump_json()` instead, so two equal models always map to the same entry.
Make sure `repr` is stable for the other arguments you pass.

#### How results are stored and returned

Encoding is chosen once, when the decorator is applied:

1. **`model=` given.** Results are stored with `TypeAdapter(model)` and a hit is
   rebuilt through `validate_json`. A plain dict returned by the function is
   stored as the model's JSON, and a hit comes back as the model instance.
2. **No `model=`, and the return annotation is a type that is not plain JSON**
   (for example a `BaseModel`, dataclass, `list[Model]`, `dict[str, Model]` or
   `Optional[Model]`) **and pydantic is installed.** Results are stored with
   `TypeAdapter(annotation)`. A hit returns real model instances, not dicts.
   The annotation is read with `typing.get_type_hints`. If that fails, memoize
   behaves as in case 3.
3. **Otherwise.** On a `serializer="json"` cache (or a custom serializer), the
   result goes through the serializer, so dicts and lists round-trip. On a
   `serializer="raw"` cache, the stored value is `bytes`, `str`, `int` or
   `float`. A hit is decoded with the return annotation: `int` and `float`
   annotations coerce the stored text back to a number, `str` gives `str`, and
   with no annotation (or `bytes`) a hit returns `bytes`. Other types raise
   `TypeError` when stored.

With `decode=True` on a raw cache, hits return `str` unless the annotation says
otherwise.

#### Other behaviour

- **Async functions.** A coroutine function gets an async wrapper. The awaited
  result is stored, and a hit returns without awaiting the function.
- **Cached `None`.** A stored `None` is a hit, distinct from a miss, so the
  function is not called again until the entry expires.

#### Errors

- `ImportError` with the install hint if `model=` or the annotation needs
  pydantic and pydantic is not installed.
- `pydantic.ValidationError` (a `ValueError`) if a cached value does not match
  the model on a hit.
- `TypeError` if the result cannot be stored by the serializer.

## `Client`

```python
Client(host="127.0.0.1", port=6380, password=None, timeout=5.0,
       pool_size=8, decode=False, serializer="raw")
```

A thread-safe client for a running server. It implements the same methods as
`Cache` (strings, keys, hashes, lists, sets, JSON, pydantic, `memoize`,
dict-style access) over RESP2, with a small pure-Python RESP encoder and decoder
(no redis-py dependency).

| Parameter | Type | Default | Description |
|---|---|---|---|
| `host` | `str` | `"127.0.0.1"` | Server address. |
| `port` | `int` | `6380` | Server port, 1 to 65535. |
| `password` | `str` or `bytes` or `None` | `None` | Sent with `AUTH` on each new connection, when set. A server that requires a password and gets none, or a wrong one, raises `AuthenticationError`. |
| `timeout` | `float` or `None` | `5.0` | Socket timeout in seconds for connect and each read or write. `None` means no timeout. Must be positive. |
| `pool_size` | `int` | `8` | Maximum connections, and so the maximum number of commands in flight. |
| `decode`, `serializer` | | `False`, `"raw"` | As for `Cache`. |

### Connection pool and threads

Each call borrows an idle pooled connection, or opens a new one, and returns it
afterwards. A single `Client` can be shared by many threads. At most `pool_size`
commands run at once. If no slot frees within `timeout`, the call raises
`ConnectionError`.

### Retry policy

A broken connection (the peer closed it, or the socket failed) is retried once,
on a new connection, only for commands where repeating the command cannot change
the outcome. These are retried: reads (`GET`, `MGET`, `EXISTS`, `TTL`, `KEYS`,
`HGETALL`, `LRANGE`, and so on), plain `SET` and `MSET`, `EXPIRE`, `PEXPIRE`,
`FLUSHALL`, `PING`, and `INFO`.

These are never retried after a possible send: `INCRBY`/`DECRBY` (and so
`incr`/`decr`), `APPEND`, `LPUSH`/`RPUSH`, `LPOP`/`RPOP`, `DEL`, `HSET`, `HDEL`,
`SADD`, `SREM`, `PERSIST`, and `SET` with `NX` or `XX`. A repeated `INCRBY` or
`LPUSH` would change the data twice. A repeated `DEL` or `HSET` would return a
different reply.

Connections that the peer has already closed are discarded before any command
is sent, so a server restart is normally invisible to the caller. If a
non-retryable command fails after its bytes were sent, the caller sees
`ConnectionError`, and the command may or may not have taken effect.

### Errors

| Condition | Exception |
|---|---|
| Connect failure, auth failure, pool wait timeout, or a lost connection that cannot be retried | `ConnectionError` |
| `WRONGTYPE` | `TypeError` |
| `OOM` | `MemoryError` |
| `ERR value is not an integer` or an overflow | `ValueError` |
| Any other server error (for example `ERR unknown command`) | `ResponseError` |
| `used_memory` against a server without `INFO` | `NotImplementedError` |

`ResponseError` is a subclass of `Exception`. A `Client` used after `close()`
raises `ConnectionError`.

### `ping() -> bool`

True if the server answers `PING`.

### `close() -> None`

Closes every pooled connection. Later calls raise `ConnectionError`. Safe to call
more than once. `Client` is also a context manager.

### `pipeline() -> Pipeline`

Returns a `Pipeline` that sends several commands in one write. Only these methods
are available on a pipeline: `set`, `get`, `delete`, `incr`, `hset`, `hget`,
`lpush`, `rpush`. Each returns the pipeline, so calls chain. Pipelines are never
retried.

```python
with client.pipeline() as p:
    p.set("a", 1).incr("n").hset("h", "f", "v")
print(p.results)   # [True, 1, True]
```

`p.execute()` sends the queue and returns the results in order, and `p.results`
is filled when the `with` block exits cleanly. If any reply is an error, the first
error is raised after all replies are read. Leaving the block with an exception
discards the queue.

### Server administration

These methods exist on `Client`, and they talk to the server's commands. They
are not on `Cache`, which has the `persist_*` members instead.

| Method | Returns |
|---|---|
| `info(section=None)` | `dict` of `INFO` values. Numbers are converted to `int` or `float`. Section headers are dropped. |
| `save()` | `None`. Raises `ValueError` if the server has no persistence. |
| `bgsave()` | `None`. Returns once the background save has started. |
| `lastsave()` | `int`, the Unix time of the last successful snapshot, or `0`. |
| `used_memory` (property, via `INFO`) | `int`. |

## `Server`

```python
Server(cache, host="127.0.0.1", port=6380, password=None)
```

A RESP2 server backed by `cache`. Writes through the cache and commands from
clients see the same data.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache` | `Cache` | required | The cache to serve. |
| `host` | `str` | `"127.0.0.1"` | Bind address. |
| `port` | `int` | `6380` | Port to bind. `0` lets the OS pick a free port. |
| `password` | `str` or `None` | `None` | When set, every connection must send `AUTH` before other commands. Must be a non-empty `str`. |

### `start() -> None`

Binds the socket and starts accepting connections on a background thread. Calling
it on a running server does nothing. Raises `OSError` if the address cannot be
bound.

### `stop() -> None`

Stops accepting connections and closes open ones. Safe to call more than once.
`Server` is also a context manager: `with Server(cache, port=0) as srv:` starts it
on entry and stops it on exit.

### `port -> int` (property)

The port the server is bound to. After `start()` with `port=0`, this is the port
the OS picked. Before `start()` (or after `stop()`) it reports the last bound
port, or the configured port if the server never started.

### `running -> bool` (property)

True between a successful `start()` and the next `stop()`.

### Supported commands

`PING`, `ECHO`, `GET`, `SET` (with `EX`, `PX`, `NX`, `XX`), `SETEX`, `DEL`,
`EXISTS`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `TTL`, `PTTL`, `INCR`, `INCRBY`,
`DECR`, `DECRBY`, `APPEND`, `MGET`, `MSET`, `KEYS`, `DBSIZE`, `FLUSHALL`,
`FLUSHDB`, `HSET`, `HGET`, `HDEL`, `HGETALL`, `HLEN`, `LPUSH`, `RPUSH`, `LPOP`,
`RPOP`, `LRANGE`, `LLEN`, `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`,
`SELECT` (only database 0), `COMMAND` (returns an empty list), `AUTH`, `INFO`,
`SAVE`, `BGSAVE`, `LASTSAVE`, and `QUIT`.

#### Authentication

With a `password`, a new connection is unauthenticated. Until it sends
`AUTH <password>` successfully, only `AUTH` and `QUIT` are accepted. Anything
else gets `NOAUTH Authentication required.`. A wrong password gets
`WRONGPASS invalid password` and leaves the connection unauthenticated. Each
connection authenticates separately.

Without a `password`, `AUTH` is refused with an `ERR` reply, as in Redis.
The password is compared in constant time with respect to its contents.

#### `INFO [section]`

Returns the server's statistics as text, in `key:value` lines under
`# Section` headers. The sections are `Server` (`uptime_in_seconds`), `Memory`
(`used_memory`), `Keyspace` (`keys`), and `Persistence`
(`persistence_enabled`, and when it is 1, `last_save_time`, `wal_bytes`,
`snapshot_bytes`, `last_error`).

#### Persistence commands

- `SAVE` writes a snapshot now and replies `OK`. Refused with an error when the
  server has no `--dir`.
- `BGSAVE` starts a snapshot on a background thread and replies at once. Refused
  with `ERR Background save already in progress` while another one is running.
- `LASTSAVE` replies with the Unix time of the last successful snapshot, or `0`.

`Client` treats a missing `INFO` as `NotImplementedError` for `used_memory`.

## Command-line server

```bash
pydirs-server [options]
python -m pydirs [options]
```

Both run the same server. It prints a banner with the bound port and runs until
SIGINT or SIGTERM. On either signal it stops the server and closes the cache,
which writes a final snapshot when `--dir` is set.

Exit codes:

| Code | Meaning |
|---|---|
| `0` | Clean stop. |
| `1` | The data directory is locked by another live process, or its snapshot is corrupt. |
| `2` | Usage or configuration error, such as a bad flag, or a non-loopback bind without a password. |

| Option | Default | Description |
|---|---|---|
| `--host ADDR` | `127.0.0.1` | Address to bind. |
| `--port N` | `6380` | TCP port. `0` picks a free port. |
| `--max-memory SIZE` | `0` | Memory limit, in bytes or with a suffix such as `64mb` or `2gb`. `0` is unlimited. |
| `--policy NAME` | `noeviction` | `noeviction`, `allkeys-lru` or `volatile-lru`. |
| `--shards N` | `64` | Number of shards, a power of two. |
| `--dir PATH` | off | Persistence directory. Enables snapshots and the WAL. |
| `--fsync POLICY` | `everysec` | `always`, `everysec` or `never`. Needs `--dir`. |
| `--snapshot-interval SECONDS` | `300` | Seconds between background snapshots, only when there are unsaved writes. `0` disables the timer. Needs `--dir`. |
| `--requirepass PASS` | none | Password clients must send with `AUTH`. Also read from `$PYDIRS_PASSWORD`. A flag takes precedence over the variable. An empty password is refused. |
| `--allow-insecure` | off | Permit a non-loopback bind without a password. |
| `--version` | | Print the version and exit. |

The startup banner ends with the state of the options, for example
`persist=/var/lib/pydirs auth=on` or `persist=off auth=off`.

Binding a non-loopback address (`0.0.0.0`, a LAN address) without a password is
refused with exit code `2`, unless `--allow-insecure` is given. Set
`--requirepass` or `PYDIRS_PASSWORD` for a public bind.

For running it as a service, see [docs/persistence.md](persistence.md#running-as-a-service-untested-examples).
