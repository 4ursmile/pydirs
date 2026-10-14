"""Dict-style access and the context manager."""

from __future__ import annotations

import pytest

from pydirs import Cache


def test_setitem_and_getitem(cache: Cache) -> None:
    cache["k"] = "v"
    assert cache["k"] == b"v"


def test_getitem_missing_raises_keyerror(cache: Cache) -> None:
    with pytest.raises(KeyError):
        _ = cache["missing"]


def test_delitem_removes_key(cache: Cache) -> None:
    cache["k"] = "v"
    del cache["k"]
    assert "k" not in cache


def test_delitem_missing_raises_keyerror(cache: Cache) -> None:
    with pytest.raises(KeyError):
        del cache["missing"]


def test_contains(cache: Cache) -> None:
    assert "k" not in cache
    cache["k"] = 1
    assert "k" in cache


def test_contains_rejects_unhashable_type(cache: Cache) -> None:
    assert [] not in cache  # type: ignore[comparison-overlap]


def test_getitem_decode(dcache: Cache) -> None:
    dcache["k"] = "v"
    assert dcache["k"] == "v"


def test_getitem_json_null_is_not_missing() -> None:
    with Cache(serializer="json") as c:
        c["k"] = None
        assert c["k"] is None


def test_context_manager_returns_cache_and_closes() -> None:
    with Cache() as c:
        assert isinstance(c, Cache)
        c.set("k", "v")
    # close() has run; the object stays usable for reads of already-closed state
    # without raising, and a second close is harmless.
    c.close()


def test_context_manager_closes_on_exception() -> None:
    with pytest.raises(RuntimeError):
        with Cache() as c:
            raise RuntimeError("boom")
    c.close()


def test_decode_property_and_serializer_property() -> None:
    with Cache(decode=True, serializer="json") as c:
        assert c.decode is True
        assert c.serializer == "json"
