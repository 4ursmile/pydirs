"""Shared fixtures for the tinycache Python test suite.

Tests use short TTLs (``px=50``) and 0.15 s sleeps so the whole suite stays fast.

``make_api`` is parametrised over the two backends: ``"cache"`` (in-process
``Cache``) and ``"client"`` (``Client`` talking RESP2 to an in-process ``Server``
on a free port). Tests that use it run once per backend.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, List

import pytest

from tinycache import Cache, Client, Server

_CACHE_KEYS = ("max_memory", "policy", "shards")


@pytest.fixture
def cache() -> Iterator[Cache]:
    c = Cache()
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def jcache() -> Iterator[Cache]:
    c = Cache(serializer="json")
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
def dcache() -> Iterator[Cache]:
    c = Cache(decode=True)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture(params=["cache", "client"])
def make_api(request: pytest.FixtureRequest) -> Iterator[Callable[..., Any]]:
    """Factory returning a fresh ``Cache`` or ``Client``, depending on the test parameter.

    Keyword arguments ``max_memory``, ``policy`` and ``shards`` configure the
    underlying cache (the in-process one, or the one behind the server). The
    rest, such as ``decode`` and ``serializer``, go to the object itself.
    """
    cleanups: List[Callable[[], None]] = []

    def factory(**kwargs: Any) -> Any:
        cache_kwargs = {k: kwargs.pop(k) for k in list(kwargs) if k in _CACHE_KEYS}
        if request.param == "cache":
            obj = Cache(**cache_kwargs, **kwargs)
            cleanups.append(obj.close)
            return obj
        backing = Cache(**cache_kwargs)
        srv = Server(backing, host="127.0.0.1", port=0)
        srv.start()
        client = Client(host="127.0.0.1", port=srv.port, **kwargs)
        cleanups.extend([client.close, srv.stop, backing.close])
        return client

    factory.backend = request.param  # type: ignore[attr-defined]
    try:
        yield factory
    finally:
        for cleanup in reversed(cleanups):
            cleanup()
