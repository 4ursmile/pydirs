"""The public ``Cache`` class: the native in-process backend plus the shared API.

All user-facing behaviour (decoding, serializers, JSON, pydantic, dict sugar,
memoize) lives in ``TinyCacheAPI`` (``_api.py``). This module only maps each
bytes-level backend method onto the native Rust class.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from ._api import TinyCacheAPI
from ._serializers import Serializer
from ._tinycache import Cache as _NativeCache

__all__ = ["Cache"]

Key = Union[str, bytes, int, float]


class Cache(TinyCacheAPI, _NativeCache):
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
        return super().__new__(cls, max_memory, policy, shards)  # type: ignore[no-any-return]

    def __init__(
        self,
        max_memory: int = 0,
        policy: str = "noeviction",
        shards: int = 64,
        decode: bool = False,
        serializer: Union[str, Serializer] = "raw",
    ) -> None:
        self._init_api(decode, serializer)

    # ---- bytes-level backend (native calls) ------------------------------------

    def _b_set(self, key: Key, value: Any, ex: Optional[int], px: Optional[int], nx: bool, xx: bool) -> bool:
        return _NativeCache.set(self, key, value, ex=ex, px=px, nx=nx, xx=xx)

    def _b_get(self, key: Key) -> Optional[bytes]:
        return _NativeCache.get(self, key)

    def _b_mget(self, keys: List[Key]) -> List[Optional[bytes]]:
        return _NativeCache.mget(self, keys)

    def _b_mset(self, mapping: Dict[Key, Any]) -> None:
        _NativeCache.mset(self, mapping)

    def _b_incr(self, key: Key, delta: int) -> int:
        return _NativeCache.incr(self, key, delta)

    def _b_append(self, key: Key, value: Any) -> int:
        return _NativeCache.append(self, key, value)

    def _b_delete(self, keys: Tuple[Key, ...]) -> int:
        return _NativeCache.delete(self, *keys)

    def _b_exists(self, key: Key) -> bool:
        return _NativeCache.exists(self, key)

    def _b_expire(self, key: Key, seconds: int) -> bool:
        return _NativeCache.expire(self, key, seconds)

    def _b_pexpire(self, key: Key, milliseconds: int) -> bool:
        return _NativeCache.pexpire(self, key, milliseconds)

    def _b_persist(self, key: Key) -> bool:
        return _NativeCache.persist(self, key)

    def _b_pttl(self, key: Key) -> int:
        return _NativeCache.pttl(self, key)

    def _b_keys(self, pattern: str) -> List[bytes]:
        return _NativeCache.keys(self, pattern)

    def _b_flush(self) -> None:
        _NativeCache.flush(self)

    def _b_used_memory(self) -> int:
        return _NativeCache.used_memory.__get__(self, type(self))  # type: ignore[no-any-return]

    def _b_len(self) -> int:
        return _NativeCache.__len__(self)

    def _b_hset(self, key: Key, field: Key, value: Any) -> bool:
        return _NativeCache.hset(self, key, field, value)

    def _b_hget(self, key: Key, field: Key) -> Optional[bytes]:
        return _NativeCache.hget(self, key, field)

    def _b_hdel(self, key: Key, fields: Tuple[Key, ...]) -> int:
        return _NativeCache.hdel(self, key, *fields)

    def _b_hgetall(self, key: Key) -> Dict[bytes, bytes]:
        return _NativeCache.hgetall(self, key)

    def _b_hlen(self, key: Key) -> int:
        return _NativeCache.hlen(self, key)

    def _b_lpush(self, key: Key, values: Tuple[Any, ...]) -> int:
        return _NativeCache.lpush(self, key, *values)

    def _b_rpush(self, key: Key, values: Tuple[Any, ...]) -> int:
        return _NativeCache.rpush(self, key, *values)

    def _b_lpop(self, key: Key) -> Optional[bytes]:
        return _NativeCache.lpop(self, key)

    def _b_rpop(self, key: Key) -> Optional[bytes]:
        return _NativeCache.rpop(self, key)

    def _b_lrange(self, key: Key, start: int, stop: int) -> List[bytes]:
        return _NativeCache.lrange(self, key, start, stop)

    def _b_llen(self, key: Key) -> int:
        return _NativeCache.llen(self, key)

    def _b_sadd(self, key: Key, members: Tuple[Any, ...]) -> int:
        return _NativeCache.sadd(self, key, *members)

    def _b_srem(self, key: Key, members: Tuple[Any, ...]) -> int:
        return _NativeCache.srem(self, key, *members)

    def _b_smembers(self, key: Key) -> Iterable[bytes]:
        return _NativeCache.smembers(self, key)

    def _b_sismember(self, key: Key, member: Any) -> bool:
        return _NativeCache.sismember(self, key, member)

    def _b_scard(self, key: Key) -> int:
        return _NativeCache.scard(self, key)
