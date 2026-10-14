# Python API reference

```python
from tinycache import Cache, Server
```

Unless noted otherwise, methods take keys as `str` or `bytes`. Values are
accepted as `bytes`, `str` (encoded as UTF-8), `int` or `float`. Reads return
`bytes`, or `str` when the cache was created with `decode=True`.

## Errors

| Condition | Python exception |
|---|---|
| Operation on a key holding a different type (`WRONGTYPE`) | `TypeError` |
| Write rejected under `policy="noeviction"` when `max_memory` is full | `MemoryError` |
| `incr` / `decr` / `incrby` on a value that is not an integer | `ValueError` |
| Integer overflow in `incr` / `decr` | `ValueError` |
| Invalid constructor argument (for example an unknown `policy` or `serializer`) | `ValueError` |
| Non-JSON-serialisable object passed to a `*_json` method, or to `set`/`mset`/`memoize` with `serializer="json"` | `TypeError` |
| `NaN` or `Infinity` passed as a JSON value | `ValueError` |
| A pydantic method or pydantic-typed `memoize` used without pydantic installed | `ImportError` with the install hint `pip install "tinycache-rs[pydantic]"` |
| Stored data that does not match the requested pydantic model | `pydantic.ValidationError` (subclass of `ValueError`) |
| Stored bytes that are not valid JSON, read with `get_json`, `mget_json`, `hget_json` or `lrange_json`, or with `get` / `c[k]` / `mget` under `serializer="json"` | `ValueError` |

## `Cache`

### `Cache(max_memory=0, policy="noeviction", shards=64, decode=False, serializer="raw")`

Creates an in-process cache.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `max_memory` | `int` | `0` | Approximate memory limit in bytes. `0` means unlimited. |
| `policy` | `str` | `"noeviction"` | One of `"noeviction"`, `"allkeys-lru"`, `"volatile-lru"`. |
| `shards` | `int` | `64` | Number of shards. Must be a power of two. |
| `decode` | `bool` | `False` | When `True`, read methods return `str` instead of `bytes`. |
| `serializer` | `str` | `"raw"` | `"raw"` stores `bytes`/`str`/`int`/`float` as-is. `"json"` routes `set`, `get`, `mget`, `mset`, dict-style access and `memoize` through JSON, so any JSON-compatible value round-trips. Any object with `dumps(obj) -> bytes` and `loads(data) -> obj` methods is also accepted as a custom serializer. |

`Cache` is a context manager. Leaving the `with` block calls `close()`.

### `close() -> None`

Stops the background expiry sweeper.

### Strings

#### `set(key, value, ex=None, px=None, nx=False, xx=False) -> bool`

Stores `value` under `key`.

- `ex`: expiry in seconds.
- `px`: expiry in milliseconds. Pass at most one of `ex` and `px`.
- `nx`: only set if the key does not exist.
- `xx`: only set if the key already exists.

Returns `True` when the value was stored. Returns `False` when an `nx` or `xx`
condition was not met.

#### `get(key) -> bytes | str | None`

Returns the value, or `None` if the key is missing or expired.

#### `mget(keys) -> list`

`keys` is a list of keys. Returns one entry per key, in order. Missing keys, and
keys holding a non-string type, give `None`.

#### `mset(mapping) -> None`

`mapping` is a `dict` of key to value. All entries are written.

#### `incr(key, amount=1) -> int`

Adds `amount` to the integer stored at `key`. A missing key is treated as `0`.
Returns the new value. Raises `ValueError` if the stored value is not an
integer.

#### `decr(key, amount=1) -> int`

Subtracts `amount`. Same behaviour as `incr`.

#### `append(key, value) -> int`

Appends `value` to the string at `key`, creating it if missing. Returns the
length of the string after the append.

### Keys

#### `delete(*keys) -> int`

Deletes the given keys. Returns the number that existed and were removed.

#### `exists(key) -> bool`

Returns `True` if the key exists and has not expired.

#### `expire(key, seconds) -> bool`

Sets a time-to-live in seconds. Returns `True` if the key exists.

#### `pexpire(key, milliseconds) -> bool`

Same as `expire`, in milliseconds.

#### `persist(key) -> bool`

Removes any expiry from the key. Returns `True` if an expiry was removed.

#### `ttl(key) -> int`

Remaining time to live in seconds. Returns `-2` if the key does not exist and
`-1` if it has no expiry.

#### `pttl(key) -> int`

Remaining time to live in milliseconds. Same sentinel values as `ttl`.

#### `keys(pattern="*") -> list`

Returns the keys matching a glob pattern. Supported syntax: `*`, `?`, `[abc]`
and backslash escapes. Expired keys are not returned.

Note: this scans the whole keyspace. Avoid it on large caches in hot paths.

#### `len(c) -> int`

Number of live keys. Expired keys are not counted.

#### `flush() -> None`

Removes all keys.

#### `used_memory -> int` (property)

Approximate bytes in use, as tracked for `max_memory`.

### Hashes

#### `hset(key, field, value) -> bool`

Sets a field in the hash at `key`. Returns `True` if the field is new.

#### `hget(key, field) -> bytes | str | None`

Returns the field value, or `None`.

#### `hdel(key, *fields) -> int`

Deletes fields. Returns the number removed. The key is removed when its last
field is deleted.

#### `hgetall(key) -> dict`

Returns all fields and values. Returns an empty dict for a missing key.

#### `hlen(key) -> int`

Number of fields in the hash.

### Lists

#### `lpush(key, *values) -> int`

Prepends values, in the Redis order (the last argument ends up first). Returns
the list length after the push.

#### `rpush(key, *values) -> int`

Appends values to the tail. Returns the list length after the push.

#### `lpop(key) -> bytes | str | None`

Removes and returns the head. Returns `None` for a missing or empty list.

#### `rpop(key) -> bytes | str | None`

Removes and returns the tail. Same empty behaviour as `lpop`.

#### `lrange(key, start, stop) -> list`

Returns the elements between `start` and `stop`, inclusive. Negative indices
count from the end, as in Redis (`-1` is the last element).

#### `llen(key) -> int`

Number of elements in the list.

### Sets

#### `sadd(key, *members) -> int`

Adds members. Returns the number of members that were new.

#### `srem(key, *members) -> int`

Removes members. Returns the number removed.

#### `smembers(key) -> set`

Returns all members.

#### `sismember(key, member) -> bool`

Returns `True` if `member` is in the set.

#### `scard(key) -> int`

Number of members.

### Dict-style access

```python
c["k"] = "v"     # same as c.set("k", "v")
c["k"]           # same as c.get("k")
del c["k"]       # same as c.delete("k")
"k" in c         # same as c.exists("k")
```

### JSON methods

JSON support uses the standard library `json` module only. It never uses
pickle. Values are serialised with compact separators and `ensure_ascii=False`,
and stored as UTF-8 bytes. `NaN` and `Infinity` are rejected
(`allow_nan=False`), so the stored output is strictly valid JSON.

JSON-compatible values are `dict`, `list`, `str`, `int` (including big
integers), `float`, `bool` and `None`.

#### `set_json(key, obj, ex=None, px=None, nx=False, xx=False) -> bool`

Serialises `obj` and stores it. Expiry and `nx`/`xx` behave as in `set`.
Returns `False` when an `nx` or `xx` condition was not met.

#### `get_json(key, default=None) -> Any`

Returns the decoded object. Returns `default` if the key is missing or expired.
Raises `ValueError` if the stored bytes are not valid JSON.

#### `mget_json(keys) -> list`

Like `mget`, but decodes each value. Missing keys give `None`. Raises
`ValueError` if any stored value is corrupt.

#### `mset_json(mapping) -> None`

Like `mset`, but serialises each value. Raises `TypeError` if any value is not
serialisable, and `ValueError` for `NaN` or `Infinity`.

#### `hset_json(key, field, obj) -> bool`

Stores a JSON-serialised hash field. Returns `True` if the field is new.

#### `hget_json(key, field, default=None) -> Any`

Returns the decoded hash field, or `default` if the field is missing.

#### `lpush_json(key, *objs) -> int`

Prepends JSON-serialised list elements, in the same order as `lpush`. Returns
the list length after the push.

#### `rpush_json(key, *objs) -> int`

Appends JSON-serialised list elements. Returns the list length after the push.

#### `lrange_json(key, start, stop) -> list`

Like `lrange`, but decodes each element. Raises `ValueError` if an element is
corrupt.

#### Example

```python
c = Cache(serializer="json")
c.set("user:1", {"name": "Ada", "tags": ["math", "engines"], "score": 9.5})
c.get("user:1")              # {'name': 'Ada', 'tags': [...], 'score': 9.5}

c2 = Cache()                 # default serializer="raw"
c2.set_json("user:1", {"name": "Ada"})
c2.get_json("user:1")        # {'name': 'Ada'}
c2.get_json("missing", {})   # {}
```

`set_json` always stores JSON text, whatever the cache's serializer. A plain
`set("abc")` stores the bytes `abc`, while `set_json("abc")` stores `"abc"`
(with quotes). So `get_json` reads a value written with `set` only when that
value is itself valid JSON. Under `serializer="json"`, `get` on bytes that are
not valid JSON raises `ValueError`.

### Pydantic methods

Pydantic support is optional: `pip install "tinycache-rs[pydantic]"` (which
installs `pydantic>=2.5`). The package imports and works without pydantic.
Pydantic is imported only when one of these methods is called. Without it,
they raise `ImportError` with the message
`pydantic is required for this feature; install it with: pip install "tinycache-rs[pydantic]"`.

Models are stored as JSON produced by `pydantic.TypeAdapter(type).dump_json`
and read back with `TypeAdapter(model).validate_json`. Supported types are
anything pydantic can serialise: `BaseModel` subclasses, dataclasses,
`TypedDict`, `list[Model]`, `dict[str, Model]`, `Optional[Model]`, and so on.
Pydantic v2 only. Adapters are cached per type.

#### `set_model(key, obj, ex=None, px=None, nx=False, xx=False) -> bool`

Serialises `obj` with `TypeAdapter(type(obj)).dump_json`. Expiry and `nx`/`xx`
behave as in `set`. Returns `False` when an `nx` or `xx` condition was not met.
Raises `TypeError` if pydantic cannot serialise the object.

#### `get_model(key, model, default=None) -> Any`

Validates the stored JSON with `TypeAdapter(model).validate_json` and returns
the instance. `model` is any type annotation (`User`, `list[User]`). Returns
`default` if the key is missing or expired.

Raises `pydantic.ValidationError` if the stored data does not match `model`.
`ValidationError` subclasses `ValueError`, so `except ValueError` also catches
it. Corrupt bytes that are not JSON at all also raise `ValueError`.

#### `mget_model(keys, model) -> list`

Like `get_model` for several keys. Missing keys give `None`. Raises
`pydantic.ValidationError` if any stored value does not match `model`.

#### `hset_model(key, field, obj) -> bool`

Stores a model in a hash field. Returns `True` if the field is new.

#### `hget_model(key, field, model, default=None) -> Any`

Validates and returns a hash field as `model`. Returns `default` if the field is
missing. Raises `pydantic.ValidationError` on mismatch.

#### Example

```python
from pydantic import BaseModel
from tinycache import Cache

class User(BaseModel):
    id: int
    name: str

c = Cache()
c.set_model("user:1", User(id=1, name="Ada"))
c.get_model("user:1", User)                 # User(id=1, name='Ada')
c.get_model("missing", User, default=None)  # None
```

### `memoize(ttl=None, model=None)`

Decorator that caches the return value of a function in this cache. It works
on sync and async functions.

```python
@cache.memoize(ttl=60)
def compute(x, y=0):
    ...
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `ttl` | `int \| None` | `None` | Seconds to keep each result. `None` means no expiry. |
| `model` | type annotation \| `None` | `None` | Type used to store and rebuild results. Overrides the return annotation. |

#### Cache key

The key is `f"{func.__module__}.{func.__qualname__}"` followed by the arguments.
Each positional argument and each keyword argument (sorted by name) is turned
into text with `repr`. The exception is a pydantic model instance, which is
keyed by `model_dump_json()` instead. Make sure that `repr` or
`model_dump_json()` is stable for the arguments you pass.

#### Result storage and return type

- **Serializer.** The result is stored through the cache's serializer. On a
  `serializer="json"` cache it can be any JSON-compatible value. On a `"raw"`
  cache it must be `bytes`, `str`, `int` or `float`, and other values raise
  `TypeError`.
- **Return annotation.** Without `model=`, the decorator reads the function's
  return annotation with `typing.get_type_hints`. If the annotation is a
  pydantic-handled type that is not plain JSON (a `BaseModel`, dataclass,
  `list[Model]`, `dict[str, Model]`, `Optional[Model]`, and so on), results are
  stored with `TypeAdapter(ret).dump_json`. A hit returns the rebuilt model
  instances, not dicts. If `get_type_hints` fails, or the annotation is absent
  or plain JSON, memoize uses the JSON behaviour above.
- **`model=` override.** `@cache.memoize(ttl=60, model=User)` uses `User`
  regardless of the annotation. The function can return a plain dict and the
  hit still comes back as `User`.
- **Async functions.** If the function is a coroutine function, `memoize`
  returns an async wrapper. The awaited result is stored, and a hit returns
  without awaiting the function.
- **Cached `None`.** A stored `None` is a hit. It is not treated as a miss, so
  the function is not called again until the entry expires.

#### Errors

- `ImportError` with the install hint if the annotation or `model=` needs
  pydantic and pydantic is not installed.
- `pydantic.ValidationError` (a `ValueError`) if a cached value does not match
  the model on a hit.

Without pydantic installed, or with no model annotation, `memoize` falls back to
the JSON behaviour in the JSON section.


## `Server`

### `Server(cache, host="127.0.0.1", port=6380)`

A RESP2 server backed by `cache`. The server shares the cache's data, so a
value written through one is visible through the other.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `cache` | `Cache` | required | The cache to serve. |
| `host` | `str` | `"127.0.0.1"` | Bind address. |
| `port` | `int` | `6380` | Bind port. |

### `start() -> None`

Binds the socket and starts accepting connections on a background thread.

### `stop() -> None`

Stops accepting connections and closes open ones. Safe to call more than once.

### `port -> int` (property)

The configured port.

Supported commands and their semantics are listed in the
[README](../README.md#server-mode-resp2).
