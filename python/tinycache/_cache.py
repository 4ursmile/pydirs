"""The public ``Cache`` class: the native cache plus decoding, serializers, JSON,
pydantic helpers, dict-style access and ``memoize``."""

from __future__ import annotations

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
    Type,
    TypeVar,
    Union,
    cast,
)

from . import _pydantic
from ._memoize import make_decorator
from ._serializers import Serializer, json_dumps, json_loads, resolve_serializer
from ._tinycache import Cache as _NativeCache

__all__ = ["Cache"]

Key = Union[str, bytes, int, float]
T = TypeVar("T")
M = TypeVar("M")


class Cache(_NativeCache):
    """In-process, Redis-like cache.

    Reads return ``bytes``, or ``str`` when ``decode=True``. With
    ``serializer="json"``, values go through JSON on ``set``, ``get``, ``mget``,
    ``mset``, dict-style access and ``memoize``.

    ``Cache`` is a context manager; leaving the ``with`` block calls ``close()``.
    """

    def __new__(
        cls,
        max_memory: int = 0,
        policy: str = "noeviction",
        shards: int = 64,
        decode: bool = False,
        serializer: Union[str, Serializer] = "raw",
    ) -> "Cache":
        return cast("Cache", super().__new__(cls, max_memory, policy, shards))

    def __init__(
        self,
        max_memory: int = 0,
        policy: str = "noeviction",
        shards: int = 64,
        decode: bool = False,
        serializer: Union[str, Serializer] = "raw",
    ) -> None:
        self._decode = bool(decode)
        self._ser: Optional[Serializer] = resolve_serializer(serializer)
        self._serializer_arg = serializer

    # ---- properties and lifecycle -----------------------------------------

    @property
    def decode(self) -> bool:
        """True when reads return ``str`` instead of ``bytes``."""
        return self._decode

    @property
    def serializer(self) -> Union[str, Serializer]:
        """The ``serializer`` argument this cache was created with."""
        return self._serializer_arg

    def __enter__(self) -> "Cache":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.close()

    # ---- encoding helpers --------------------------------------------------

    def _enc(self, value: Any) -> Any:
        """Encodes a value for the core: through the serializer if one is set."""
        if self._ser is None:
            return value
        return self._ser.dumps(value)

    def _dec(self, raw: Optional[bytes]) -> Any:
        """Decodes a value read from the core."""
        if raw is None:
            return None
        if self._ser is not None:
            return self._ser.loads(raw)
        return self._text(raw)

    def _text(self, raw: Any) -> Any:
        """Applies ``decode=True`` to a bytes value (and returns other values as-is)."""
        if self._decode and isinstance(raw, bytes):
            return raw.decode("utf-8")
        return raw

    def _load_raw(self, key: Key) -> Optional[bytes]:
        """Reads the stored bytes for ``key`` without any decoding (used by memoize)."""
        return _NativeCache.get(self, key)

    def _store_raw(self, key: Key, value: Any, ttl: Optional[float]) -> None:
        """Writes already-encoded ``value`` with a TTL in seconds (used by memoize)."""
        px = None if ttl is None else max(1, int(ttl * 1000))
        _NativeCache.set(self, key, value, px=px)

    # ---- strings -----------------------------------------------------------

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
        return _NativeCache.set(self, key, self._enc(value), ex=ex, px=px, nx=nx, xx=xx)

    def get(self, key: Key) -> Any:
        """Returns the value, or ``None`` if missing or expired."""
        return self._dec(_NativeCache.get(self, key))

    def mget(self, keys: Iterable[Key]) -> List[Any]:
        """Returns one value per key, in order. Missing keys give ``None``."""
        return [self._dec(v) for v in _NativeCache.mget(self, list(keys))]

    def mset(self, mapping: Mapping[Key, Any]) -> None:
        """Stores every entry of ``mapping``. All values are encoded before any write."""
        encoded = {k: self._enc(v) for k, v in mapping.items()}
        _NativeCache.mset(self, encoded)

    # ---- dict-style sugar ----------------------------------------------------

    def __getitem__(self, key: Key) -> Any:
        raw = _NativeCache.get(self, key)
        if raw is None:
            raise KeyError(key)
        return self._dec(raw)

    def __setitem__(self, key: Key, value: Any) -> None:
        self.set(key, value)

    def __delitem__(self, key: Key) -> None:
        if _NativeCache.delete(self, key) == 0:
            raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, (str, bytes, int, float)):
            return False
        return bool(self.exists(key))

    # ---- keys ----------------------------------------------------------------

    def keys(self, pattern: str = "*") -> List[Any]:  # type: ignore[override]
        """Returns the live keys matching a glob ``pattern``."""
        return [self._text(k) for k in _NativeCache.keys(self, pattern)]

    # ---- hashes --------------------------------------------------------------

    def hget(self, key: Key, field: Key) -> Any:
        """Returns a hash field, or ``None``."""
        return self._text(_NativeCache.hget(self, key, field))

    def hgetall(self, key: Key) -> Dict[Any, Any]:
        """Returns all fields of a hash as a ``dict`` (empty for a missing key)."""
        return {self._text(f): self._text(v) for f, v in _NativeCache.hgetall(self, key).items()}

    # ---- lists ---------------------------------------------------------------

    def lpop(self, key: Key) -> Any:
        """Removes and returns the head of a list, or ``None``."""
        return self._text(_NativeCache.lpop(self, key))

    def rpop(self, key: Key) -> Any:
        """Removes and returns the tail of a list, or ``None``."""
        return self._text(_NativeCache.rpop(self, key))

    def lrange(self, key: Key, start: int, stop: int) -> List[Any]:
        """Returns list elements from ``start`` to ``stop`` inclusive."""
        return [self._text(v) for v in _NativeCache.lrange(self, key, start, stop)]

    # ---- sets ----------------------------------------------------------------

    def smembers(self, key: Key) -> Set[Any]:
        """Returns all set members as a ``set``."""
        return {self._text(m) for m in _NativeCache.smembers(self, key)}

    # ---- JSON (stdlib json; never pickle) -------------------------------------

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
        return _NativeCache.set(self, key, json_dumps(obj), ex=ex, px=px, nx=nx, xx=xx)

    def get_json(self, key: Key, default: Any = None) -> Any:
        """Returns the decoded JSON value, or ``default`` if the key is missing.

        Raises ``ValueError`` if the stored bytes are not valid JSON.
        """
        raw = _NativeCache.get(self, key)
        return default if raw is None else json_loads(raw)

    def mget_json(self, keys: Iterable[Key]) -> List[Any]:
        """Like ``mget``, but decodes JSON. Missing keys give ``None``."""
        return [None if raw is None else json_loads(raw) for raw in _NativeCache.mget(self, list(keys))]

    def mset_json(self, mapping: Mapping[Key, Any]) -> None:
        """Like ``mset``, but serialises each value as JSON before writing any."""
        encoded = {k: json_dumps(v) for k, v in mapping.items()}
        _NativeCache.mset(self, encoded)

    def hset_json(self, key: Key, field: Key, obj: Any) -> bool:
        """Stores a JSON hash field. Returns ``True`` if the field is new."""
        return _NativeCache.hset(self, key, field, json_dumps(obj))

    def hget_json(self, key: Key, field: Key, default: Any = None) -> Any:
        """Returns a decoded JSON hash field, or ``default`` if it is missing."""
        raw = _NativeCache.hget(self, key, field)
        return default if raw is None else json_loads(raw)

    def lpush_json(self, key: Key, *objs: Any) -> int:
        """Prepends JSON-encoded elements, in the same order as ``lpush``."""
        return _NativeCache.lpush(self, key, *[json_dumps(o) for o in objs])

    def rpush_json(self, key: Key, *objs: Any) -> int:
        """Appends JSON-encoded elements."""
        return _NativeCache.rpush(self, key, *[json_dumps(o) for o in objs])

    def lrange_json(self, key: Key, start: int, stop: int) -> List[Any]:
        """Like ``lrange``, but decodes each element as JSON."""
        return [json_loads(raw) for raw in _NativeCache.lrange(self, key, start, stop)]

    # ---- pydantic (optional; pip install 'tinycache-rs[pydantic]') -------------

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
        data = _pydantic.adapter(type(obj)).dump_json(obj)
        return _NativeCache.set(self, key, bytes(data), ex=ex, px=px, nx=nx, xx=xx)

    def get_model(self, key: Key, model: Any, default: Any = None) -> Any:
        """Returns the value validated as ``model``, or ``default`` if missing.

        Raises ``pydantic.ValidationError`` (a ``ValueError``) on mismatched data.
        """
        ad = _pydantic.adapter(model)
        raw = _NativeCache.get(self, key)
        if raw is None:
            return default
        return ad.validate_json(raw)

    def mget_model(self, keys: Iterable[Key], model: Any) -> List[Any]:
        """Like ``mget_model`` for many keys. Missing keys give ``None``."""
        ad = _pydantic.adapter(model)
        return [None if raw is None else ad.validate_json(raw) for raw in _NativeCache.mget(self, list(keys))]

    def hset_model(self, key: Key, field: Key, obj: Any) -> bool:
        """Stores a pydantic-serialisable object as a hash field."""
        data = _pydantic.adapter(type(obj)).dump_json(obj)
        return _NativeCache.hset(self, key, field, bytes(data))

    def hget_model(self, key: Key, field: Key, model: Any, default: Any = None) -> Any:
        """Returns a hash field validated as ``model``, or ``default`` if missing."""
        ad = _pydantic.adapter(model)
        raw = _NativeCache.hget(self, key, field)
        if raw is None:
            return default
        return ad.validate_json(raw)

    # ---- memoize -----------------------------------------------------------------

    def memoize(self, ttl: Optional[float] = None, model: Any = None) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator that caches a function's results in this cache.

        ``ttl`` is in seconds (``None`` means no expiry). ``model`` overrides the
        return annotation when rehydrating pydantic models. Sync and ``async``
        functions are both supported, and a cached ``None`` is distinct from a miss.
        """
        return make_decorator(self, ttl, model)
