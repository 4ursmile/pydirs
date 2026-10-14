"""tinycache: a lightweight, Redis-like in-memory cache with a Rust core.

The pure-Python layer adds decoding, JSON and optional pydantic support on top
of the native ``_tinycache`` extension. pydantic is never required.

``Cache`` is the in-process cache. ``Client`` talks to a running server
(``python -m tinycache``) over RESP2 and offers the same API.
"""

from __future__ import annotations

from importlib import metadata as _metadata

from ._cache import Cache
from ._client import Client, ResponseError
from ._serializers import JsonSerializer, PydanticSerializer, Serializer
from ._server import Server

try:
    __version__: str = _metadata.version("tinycache-rs")
except _metadata.PackageNotFoundError:  # pragma: no cover - source checkout without install
    __version__ = "0.0.0+unknown"

__all__ = [
    "Cache",
    "Client",
    "JsonSerializer",
    "PydanticSerializer",
    "ResponseError",
    "Serializer",
    "Server",
    "__version__",
]
