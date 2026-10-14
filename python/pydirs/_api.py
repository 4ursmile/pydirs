"""The public API shared by ``Cache`` (native backend) and ``Client`` (socket backend).

``PydirsAPI`` holds every feature that does not depend on the transport:
serializers, ``decode=True``, JSON, pydantic helpers, dict-style sugar,
``memoize`` and the context manager. A subclass supplies a small bytes-level
backend by implementing the ``_b_*`` methods below. The mixin validates
arguments and encodes values before calling a backend method, so both backends
accept the same inputs and raise the same exceptions.
"""

from __future__ import annotations

import operator
from types import TracebackType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from . import _pydantic
from ._memoize import make_decorator
from ._serializers import Serializer, json_dumps, json_loads, resolve_serializer

__all__ = ["PydirsAPI"]

Key = Union[str, bytes, int, float]
T = TypeVar("T")

_I64_MIN = -(1 << 63)


def _positive_int(value: Any, name: str) -> int:
    """Returns ``value`` if it is a strictly positive int (bool is rejected)."""
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _i64(value: Any) -> int:
    """Returns ``value`` as an int in the signed 64-bit range (bool is rejected)."""
    if isinstance(value, bool):
        raise TypeError("expected an int, not bool")
    number = operator.index(value)
    if not -(1 << 63) <= number < (1 << 63):
        raise ValueError("amount is out of the signed 64-bit range")
    return number


def _check_set_args(ex: Any, px: Any, nx: bool, xx: bool) -> None:
    if nx and xx:
        raise ValueError("nx and xx are mutually exclusive")
    if ex is not None and px is not None:
        raise ValueError("ex and px are mutually exclusive")
    if ex is not None:
        _positive_int(ex, "ex")
    if px is not None:
        _positive_int(px, "px")


class PydirsAPI:
    """Mixin with the shared Cache/Client behaviour.

    Backend contract (bytes level). ``key``, ``field``, ``member`` and ``value``
    arguments are bytes, str, int or float; backends encode them. Methods return
    the same shapes as the native core: bytes or ``None`` for single values, lists
    for ``mget``/``lrange``/``keys``, a dict for ``hgetall``, a set-like iterable
    for ``smembers``, and ``bool`` or ``int`` where the command says so.
    ``ex``/``px``/``nx``/``xx`` arrive already validated.
    """

    # ---- backend interface (implemented by Cache and Client) -------------------

    def _b_set(self, key: Key, value: Any, ex: Optional[int], px: Optional[int], nx: bool, xx: bool) -> bool:
        raise NotImplementedError

    def _b_get(self, key: Key) -> Optional[bytes]:
        raise NotImplementedError

    def _b_mget(self, keys: List[Key]) -> List[Optional[bytes]]:
        raise NotImplementedError

    def _b_mset(self, mapping: Dict[Key, Any]) -> None:
        raise NotImplementedError

    def _b_incr(self, key: Key, delta: int) -> int:
        raise NotImplementedError

    def _b_append(self, key: Key, value: Any) -> int:
        raise NotImplementedError

    def _b_delete(self, keys: Tuple[Key, ...]) -> int:
        raise NotImplementedError

    def _b_exists(self, key: Key) -> bool:
        raise NotImplementedError

    def _b_expire(self, key: Key, seconds: int) -> bool:
        raise NotImplementedError

    def _b_pexpire(self, key: Key, milliseconds: int) -> bool:
        raise NotImplementedError

    def _b_persist(self, key: Key) -> bool:
        raise NotImplementedError

    def _b_pttl(self, key: Key) -> int:
        raise NotImplementedError

    def _b_keys(self, pattern: str) -> List[bytes]:
        raise NotImplementedError

    def _b_flush(self) -> None:
        raise NotImplementedError

    def _b_used_memory(self) -> int:
        raise NotImplementedError

    def _b_len(self) -> int:
        raise NotImplementedError

    def _b_hset(self, key: Key, field: Key, value: Any) -> bool:
        raise NotImplementedError

    def _b_hget(self, key: Key, field: Key) -> Optional[bytes]:
        raise NotImplementedError

    def _b_hdel(self, key: Key, fields: Tuple[Key, ...]) -> int:
        raise NotImplementedError

    def _b_hgetall(self, key: Key) -> Dict[bytes, bytes]:
        raise NotImplementedError

    def _b_hlen(self, key: Key) -> int:
        raise NotImplementedError

    def _b_lpush(self, key: Key, values: Tuple[Any, ...]) -> int:
        raise NotImplementedError

    def _b_rpush(self, key: Key, values: Tuple[Any, ...]) -> int:
        raise NotImplementedError

    def _b_lpop(self, key: Key) -> Optional[bytes]:
        raise NotImplementedError

    def _b_rpop(self, key: Key) -> Optional[bytes]:
        raise NotImplementedError

    def _b_lrange(self, key: Key, start: int, stop: int) -> List[bytes]:
        raise NotImplementedError

    def _b_llen(self, key: Key) -> int:
        raise NotImplementedError

    def _b_sadd(self, key: Key, members: Tuple[Any, ...]) -> int:
        raise NotImplementedError

    def _b_srem(self, key: Key, members: Tuple[Any, ...]) -> int:
        raise NotImplementedError

    def _b_smembers(self, key: Key) -> Iterable[bytes]:
        raise NotImplementedError

    def _b_sismember(self, key: Key, member: Any) -> bool:
        raise NotImplementedError

    def _b_scard(self, key: Key) -> int:
        raise NotImplementedError

    # ---- setup and encoding helpers -----------------------------------------------

    def _init_api(self, decode: bool, serializer: Union[str, Serializer]) -> None:
        """Stores the ``decode`` and ``serializer`` options. Call from ``__init__``."""
        self._decode = bool(decode)
        self._ser: Optional[Serializer] = resolve_serializer(serializer)
        self._serializer_arg = serializer

    @property
    def decode(self) -> bool:
        """True when reads return ``str`` instead of ``bytes``."""
        return self._decode

    @property
    def serializer(self) -> Union[str, Serializer]:
        """The ``serializer`` argument this cache was created with."""
        return self._serializer_arg

    def __enter__(self) -> Any:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()  # type: ignore[attr-defined]

    def _enc(self, value: Any) -> Any:
        """Encodes a value for the backend: through the serializer if one is set."""
        if self._ser is None:
            return value
        return self._ser.dumps(value)

    def _dec(self, raw: Optional[bytes]) -> Any:
        """Decodes a value read from the backend."""
        if raw is None:
            return None
        if self._ser is not None:
            return self._ser.loads(raw)
        return self._text(raw)

    def _text(self, raw: Any) -> Any:
        """Applies ``decode=True`` to a bytes value (other values are returned as-is)."""
        if self._decode and isinstance(raw, bytes):
            return raw.decode("utf-8")
        return raw

    def _set_raw(self, key: Key, data: Any, ex: Any, px: Any, nx: bool, xx: bool) -> bool:
        """Validates the TTL and condition arguments, then stores already-encoded ``data``."""
        _check_set_args(ex, px, nx, xx)
        return self._b_set(key, data, ex, px, bool(nx), bool(xx))

    def _load_raw(self, key: Key) -> Optional[bytes]:
        """Reads the stored bytes for ``key`` without decoding (used by memoize)."""
        return self._b_get(key)

    def _store_raw(self, key: Key, value: Any, ttl: Optional[float]) -> None:
        """Writes already-encoded ``value`` with a TTL in seconds (used by memoize)."""
        px = None if ttl is None else max(1, int(ttl * 1000))
        self._b_set(key, value, None, px, False, False)

    # ---- strings -----------------------------------------------------------------

    def set(
        self,
        key: Key,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Stores ``value``. Returns ``False`` when an ``nx`` or ``xx`` condition fails."""
        return self._set_raw(key, self._enc(value), ex, px, nx, xx)

    def get(self, key: Key) -> Any:
        """Returns the value, or ``None`` if missing or expired."""
        return self._dec(self._b_get(key))

    def mget(self, keys: Iterable[Key]) -> List[Any]:
        """Returns one value per key, in order. Missing keys give ``None``."""
        keys = list(keys)
        if not keys:
            return []
        return [self._dec(v) for v in self._b_mget(keys)]

    def mset(self, mapping: Mapping[Key, Any]) -> None:
        """Stores every entry of ``mapping``. All values are encoded before any write."""
        encoded = {k: self._enc(v) for k, v in mapping.items()}
        if encoded:
            self._b_mset(encoded)

    def incr(self, key: Key, amount: int = 1) -> int:
        """Adds ``amount`` to an integer value (missing keys start at 0) and returns it."""
        return self._b_incr(key, _i64(amount))

    def decr(self, key: Key, amount: int = 1) -> int:
        """Subtracts ``amount`` from an integer value and returns the result."""
        amount = _i64(amount)
        if amount == _I64_MIN:
            raise ValueError("decrement would overflow")
        return self._b_incr(key, -amount)

    def append(self, key: Key, value: Any) -> int:
        """Appends raw bytes (no serializer) and returns the new length."""
        return self._b_append(key, value)

    # ---- dict-style sugar ----------------------------------------------------------

    def __getitem__(self, key: Key) -> Any:
        raw = self._b_get(key)
        if raw is None:
            raise KeyError(key)
        return self._dec(raw)

    def __setitem__(self, key: Key, value: Any) -> None:
        self.set(key, value)

    def __delitem__(self, key: Key) -> None:
        if self._b_delete((key,)) == 0:
            raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, (str, bytes, int, float)):
            return False
        return bool(self.exists(key))

    def __len__(self) -> int:
        return self._b_len()

    # ---- keys ----------------------------------------------------------------------

    def delete(self, *keys: Key) -> int:
        """Deletes the keys and returns how many existed."""
        return self._b_delete(keys)

    def exists(self, key: Key) -> bool:
        """True if the key holds a live value."""
        return self._b_exists(key)

    def expire(self, key: Key, seconds: int) -> bool:
        """Sets a TTL in whole seconds. Returns ``False`` for a missing key."""
        return self._b_expire(key, _positive_int(seconds, "seconds"))

    def pexpire(self, key: Key, milliseconds: int) -> bool:
        """Sets a TTL in milliseconds. Returns ``False`` for a missing key."""
        return self._b_pexpire(key, _positive_int(milliseconds, "milliseconds"))

    def persist(self, key: Key) -> bool:
        """Removes the TTL. Returns ``False`` if the key is missing or has none."""
        return self._b_persist(key)

    def ttl(self, key: Key) -> int:
        """Remaining TTL in whole seconds, rounded up. -2 if missing, -1 if no expiry."""
        p = self._b_pttl(key)
        return p if p < 0 else (p + 999) // 1000

    def pttl(self, key: Key) -> int:
        """Remaining TTL in milliseconds. -2 if missing, -1 if no expiry."""
        return self._b_pttl(key)

    def keys(self, pattern: str = "*") -> List[Any]:  # type: ignore[override]
        """Returns the live keys matching a glob ``pattern``."""
        return [self._text(k) for k in self._b_keys(pattern)]

    def flush(self) -> None:
        """Removes every key."""
        self._b_flush()

    @property
    def used_memory(self) -> int:
        """Approximate bytes used by stored data."""
        return self._b_used_memory()

    # ---- hashes --------------------------------------------------------------------

    def hset(self, key: Key, field: Key, value: Any) -> bool:
        """Sets a hash field. Returns ``True`` if the field is new."""
        return self._b_hset(key, field, value)

    def hget(self, key: Key, field: Key) -> Any:
        """Returns a hash field, or ``None``."""
        return self._text(self._b_hget(key, field))

    def hdel(self, key: Key, *fields: Key) -> int:
        """Deletes hash fields and returns how many were removed."""
        return self._b_hdel(key, fields)

    def hgetall(self, key: Key) -> Dict[Any, Any]:
        """Returns all fields of a hash as a ``dict`` (empty for a missing key)."""
        return {self._text(f): self._text(v) for f, v in self._b_hgetall(key).items()}

    def hlen(self, key: Key) -> int:
        """Number of fields in a hash."""
        return self._b_hlen(key)

    # ---- lists ---------------------------------------------------------------------

    def lpush(self, key: Key, *values: Any) -> int:
        """Prepends values (each one in turn, like Redis) and returns the new length."""
        return self._b_lpush(key, values)

    def rpush(self, key: Key, *values: Any) -> int:
        """Appends values and returns the new length."""
        return self._b_rpush(key, values)

    def lpop(self, key: Key) -> Any:
        """Removes and returns the head of a list, or ``None``."""
        return self._text(self._b_lpop(key))

    def rpop(self, key: Key) -> Any:
        """Removes and returns the tail of a list, or ``None``."""
        return self._text(self._b_rpop(key))

    def lrange(self, key: Key, start: int, stop: int) -> List[Any]:
        """Returns list elements from ``start`` to ``stop`` inclusive."""
        return [self._text(v) for v in self._b_lrange(key, start, stop)]

    def llen(self, key: Key) -> int:
        """Length of a list (0 if missing)."""
        return self._b_llen(key)

    # ---- sets ----------------------------------------------------------------------

    def sadd(self, key: Key, *members: Any) -> int:
        """Adds members and returns how many were new."""
        return self._b_sadd(key, members)

    def srem(self, key: Key, *members: Any) -> int:
        """Removes members and returns how many were present."""
        return self._b_srem(key, members)

    def smembers(self, key: Key) -> Set[Any]:
        """Returns all set members as a ``set``."""
        return {self._text(m) for m in self._b_smembers(key)}

    def sismember(self, key: Key, member: Any) -> bool:
        """True if ``member`` is in the set."""
        return self._b_sismember(key, member)

    def scard(self, key: Key) -> int:
        """Number of members in a set."""
        return self._b_scard(key)

    # ---- JSON (stdlib json; never pickle) ------------------------------------------

    def set_json(
        self,
        key: Key,
        obj: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Stores ``obj`` as JSON, whatever the cache's serializer is."""
        return self._set_raw(key, json_dumps(obj), ex, px, nx, xx)

    def get_json(self, key: Key, default: Any = None) -> Any:
        """Returns the decoded JSON value, or ``default`` if the key is missing.

        Raises ``ValueError`` if the stored bytes are not valid JSON.
        """
        raw = self._b_get(key)
        return default if raw is None else json_loads(raw)

    def mget_json(self, keys: Iterable[Key]) -> List[Any]:
        """Like ``mget``, but decodes JSON. Missing keys give ``None``."""
        keys = list(keys)
        if not keys:
            return []
        return [None if raw is None else json_loads(raw) for raw in self._b_mget(keys)]

    def mset_json(self, mapping: Mapping[Key, Any]) -> None:
        """Like ``mset``, but serialises each value as JSON before writing any."""
        encoded = {k: json_dumps(v) for k, v in mapping.items()}
        if encoded:
            self._b_mset(encoded)

    def hset_json(self, key: Key, field: Key, obj: Any) -> bool:
        """Stores a JSON hash field. Returns ``True`` if the field is new."""
        return self._b_hset(key, field, json_dumps(obj))

    def hget_json(self, key: Key, field: Key, default: Any = None) -> Any:
        """Returns a decoded JSON hash field, or ``default`` if it is missing."""
        raw = self._b_hget(key, field)
        return default if raw is None else json_loads(raw)

    def lpush_json(self, key: Key, *objs: Any) -> int:
        """Prepends JSON-encoded elements, in the same order as ``lpush``."""
        return self._b_lpush(key, tuple(json_dumps(o) for o in objs))

    def rpush_json(self, key: Key, *objs: Any) -> int:
        """Appends JSON-encoded elements."""
        return self._b_rpush(key, tuple(json_dumps(o) for o in objs))

    def lrange_json(self, key: Key, start: int, stop: int) -> List[Any]:
        """Like ``lrange``, but decodes each element as JSON."""
        return [json_loads(raw) for raw in self._b_lrange(key, start, stop)]

    # ---- pydantic (optional; pip install 'pydirs[pydantic]') -------------------

    def set_model(
        self,
        key: Key,
        obj: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        """Stores any pydantic-serialisable object as JSON via ``TypeAdapter``."""
        data = bytes(_pydantic.adapter(type(obj)).dump_json(obj))
        return self._set_raw(key, data, ex, px, nx, xx)

    def get_model(self, key: Key, model: Any, default: Any = None) -> Any:
        """Returns the value validated as ``model``, or ``default`` if missing.

        Raises ``pydantic.ValidationError`` (a ``ValueError``) on mismatched data.
        """
        ad = _pydantic.adapter(model)
        raw = self._b_get(key)
        if raw is None:
            return default
        return ad.validate_json(raw)

    def mget_model(self, keys: Iterable[Key], model: Any) -> List[Any]:
        """Like ``mget_model`` for many keys. Missing keys give ``None``."""
        ad = _pydantic.adapter(model)
        keys = list(keys)
        if not keys:
            return []
        return [None if raw is None else ad.validate_json(raw) for raw in self._b_mget(keys)]

    def hset_model(self, key: Key, field: Key, obj: Any) -> bool:
        """Stores a pydantic-serialisable object as a hash field."""
        data = bytes(_pydantic.adapter(type(obj)).dump_json(obj))
        return self._b_hset(key, field, data)

    def hget_model(self, key: Key, field: Key, model: Any, default: Any = None) -> Any:
        """Returns a hash field validated as ``model``, or ``default`` if missing."""
        ad = _pydantic.adapter(model)
        raw = self._b_hget(key, field)
        if raw is None:
            return default
        return ad.validate_json(raw)

    # ---- memoize -------------------------------------------------------------------

    def memoize(self, ttl: Optional[float] = None, model: Any = None) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator that caches a function's results in this cache.

        ``ttl`` is in seconds (``None`` means no expiry). ``model`` overrides the
        return annotation when rehydrating pydantic models. Sync and ``async``
        functions are both supported, and a cached ``None`` is distinct from a miss.
        """
        return make_decorator(self, ttl, model)
