"""Shared fixtures for the tinycache Python test suite.

Tests use short TTLs (``px=50``) and 0.15 s sleeps so the whole suite stays fast.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from tinycache import Cache


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
