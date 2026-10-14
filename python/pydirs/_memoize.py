"""``Cache.memoize``: caches function results, optionally rehydrating pydantic models.

Result encoding, chosen once when the decorator is applied:

* pydantic ``TypeAdapter`` when ``model=`` is given, or when the return annotation
  is not trivially JSON-native and pydantic is installed;
* the cache's serializer (``"json"`` or a custom one) otherwise;
* raw bytes/str/int/float for a ``serializer="raw"`` cache.
"""

from __future__ import annotations

import functools
import inspect
import types
import typing
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, TypeVar

from . import _pydantic

if TYPE_CHECKING:  # pragma: no cover
    from ._cache import Cache

__all__ = ["make_decorator", "make_key"]

F = TypeVar("F", bound=Callable[..., Any])

_TRIVIAL = frozenset(
    {int, float, str, bool, bytes, type(None), dict, list, tuple, object, typing.Any}
)
_CONTAINERS = frozenset({list, dict, tuple, set, frozenset})
_UNIONS: frozenset = frozenset(
    {typing.Union, getattr(types, "UnionType", typing.Union)}
)


def _is_trivial(tp: Any) -> bool:
    """True for annotations that plain JSON already represents (no pydantic needed)."""
    try:
        if tp in _TRIVIAL:
            return True
    except TypeError:
        return False
    origin = typing.get_origin(tp)
    if origin is None:
        return False
    if origin in _CONTAINERS or origin in _UNIONS:
        return all(arg is Ellipsis or _is_trivial(arg) for arg in typing.get_args(tp))
    return False


def _return_annotation(func: Callable[..., Any]) -> Any:
    try:
        return typing.get_type_hints(func).get("return")
    except Exception:
        return None


def _check_ttl(ttl: Any) -> None:
    if ttl is None:
        return
    if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or ttl <= 0:
        raise ValueError("ttl must be a positive number of seconds, or None")


def _key_part(obj: Any) -> Any:
    text = _pydantic.model_json(obj)
    if text is None:
        return obj
    return ("pydantic", type(obj).__module__, type(obj).__qualname__, text)


def make_key(func: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> str:
    """Builds ``module.qualname`` plus the repr of args and sorted kwargs.

    Pydantic model arguments are represented by ``model_dump_json()`` instead of repr.
    """
    pos = tuple(_key_part(a) for a in args)
    kw = tuple((name, _key_part(kwargs[name])) for name in sorted(kwargs))
    return f"{func.__module__}.{func.__qualname__}{pos!r}{kw!r}"


def _raw_coerce(raw: bytes, ann: Any, decode: bool) -> Any:
    """Turns stored bytes back into the type the annotation promises."""
    if ann is int:
        return int(raw.decode("utf-8"))
    if ann is float:
        return float(raw.decode("utf-8"))
    if ann is str or decode:
        return raw.decode("utf-8")
    return raw


class _Codec:
    """Encodes results for storage and decodes stored bytes on a hit."""

    __slots__ = ("_cache", "_adapter", "_ann")

    def __init__(self, cache: "Cache", adapter: Any, ann: Any) -> None:
        self._cache = cache
        self._adapter = adapter
        self._ann = ann

    def dump(self, value: Any) -> Any:
        if self._adapter is not None:
            return self._adapter.dump_json(value)
        ser = self._cache._ser
        if ser is not None:
            return ser.dumps(value)
        return value  # raw: the core rejects unsupported types with TypeError

    def load(self, raw: bytes) -> Any:
        if self._adapter is not None:
            return self._adapter.validate_json(raw)
        ser = self._cache._ser
        if ser is not None:
            return ser.loads(raw)
        return _raw_coerce(raw, self._ann, self._cache._decode)


def make_decorator(cache: "Cache", ttl: Optional[float], model: Any) -> Callable[[F], F]:
    """Returns the decorator behind ``Cache.memoize(ttl=..., model=...)``."""
    _check_ttl(ttl)

    def decorator(func: F) -> F:
        ann = _return_annotation(func) if model is None else model
        adapter: Any = None
        if model is not None:
            adapter = _pydantic.adapter(model)  # ImportError with hint if missing
        elif ann is not None and not _is_trivial(ann) and _pydantic.available():
            try:
                adapter = _pydantic.adapter(ann)
            except Exception:
                adapter = None  # not a type pydantic can handle: use the serializer
        codec = _Codec(cache, adapter, ann)

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def awrapper(*args: Any, **kwargs: Any) -> Any:
                key = make_key(func, args, kwargs)
                raw = cache._load_raw(key)
                if raw is not None:
                    return codec.load(raw)
                result = await func(*args, **kwargs)
                cache._store_raw(key, codec.dump(result), ttl)
                return result

            return awrapper  # type: ignore[return-value]

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = make_key(func, args, kwargs)
            raw = cache._load_raw(key)
            if raw is not None:
                return codec.load(raw)
            result = func(*args, **kwargs)
            cache._store_raw(key, codec.dump(result), ttl)
            return result

        return wrapper  # type: ignore[return-value]

    return decorator
