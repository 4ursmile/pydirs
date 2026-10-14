"""JSON support (PLAN section 4a): stdlib json only, never pickle."""

from __future__ import annotations

import math
import time

import pytest

from pydirs import Cache
from pydirs._pydirs import Cache as _NativeCache

from helpers import SHORT_PX, wait_for_expiry


def test_set_json_get_json_round_trip_nested(cache: Cache) -> None:
    obj = {"name": "Ada", "tags": ["math", "engines"], "meta": {"score": 9.5, "ok": True, "n": None}}
    assert cache.set_json("u", obj) is True
    assert cache.get_json("u") == obj


@pytest.mark.parametrize("value", [1, -3, 2.5, "s", True, None, [], {}, [1, [2, [3]]]])
def test_json_scalars_and_containers_round_trip(cache: Cache, value: object) -> None:
    cache.set_json("v", value)
    assert cache.get_json("v") == value


def test_unicode_round_trip_is_utf8(cache: Cache) -> None:
    text = "héllo 世界 🚀"
    cache.set_json("u", {"t": text})
    raw = cache.get("u")
    assert isinstance(raw, bytes)
    assert "世界".encode("utf-8") in raw  # ensure_ascii=False
    assert cache.get_json("u") == {"t": text}


def test_compact_separators(cache: Cache) -> None:
    cache.set_json("c", {"a": [1, 2]})
    assert cache.get("c") == b'{"a":[1,2]}'


def test_big_integer_round_trip(cache: Cache) -> None:
    big = 2**200 + 12345
    cache.set_json("big", big)
    assert cache.get_json("big") == big


def test_float_round_trip(cache: Cache) -> None:
    cache.set_json("f", 0.1)
    assert cache.get_json("f") == 0.1


def test_get_json_missing_returns_default(cache: Cache) -> None:
    assert cache.get_json("missing") is None
    assert cache.get_json("missing", {}) == {}


def test_corrupt_bytes_raise_value_error(cache: Cache) -> None:
    cache.set("bad", b"{not json")
    with pytest.raises(ValueError):
        cache.get_json("bad")


def test_nan_rejected_on_write(cache: Cache) -> None:
    with pytest.raises(ValueError):
        cache.set_json("n", math.nan)


def test_infinity_rejected_on_write(cache: Cache) -> None:
    with pytest.raises(ValueError):
        cache.set_json("i", {"x": math.inf})


def test_non_serialisable_object_is_type_error(cache: Cache) -> None:
    with pytest.raises(TypeError):
        cache.set_json("o", object())


def test_set_json_ttl(cache: Cache) -> None:
    cache.set_json("t", {"a": 1}, px=SHORT_PX)
    assert cache.get_json("t") == {"a": 1}
    wait_for_expiry()
    assert cache.get_json("t", "gone") == "gone"


def test_set_json_nx_xx(cache: Cache) -> None:
    assert cache.set_json("k", 1, nx=True) is True
    assert cache.set_json("k", 2, nx=True) is False
    assert cache.set_json("missing", 2, xx=True) is False
    assert cache.get_json("k") == 1


def test_set_json_string_stores_quoted_text(cache: Cache) -> None:
    cache.set_json("abc", "abc")
    assert cache.get("abc") == b'"abc"'


def test_mset_json_and_mget_json(cache: Cache) -> None:
    cache.mset_json({"a": [1, 2], "b": None})
    assert cache.mget_json(["a", "b", "nope"]) == [[1, 2], None, None]


def test_mget_json_corrupt_value_raises(cache: Cache) -> None:
    cache.set("bad", b"<<")
    with pytest.raises(ValueError):
        cache.mget_json(["bad"])


def test_mset_json_rejects_nan_without_partial_write(cache: Cache) -> None:
    with pytest.raises(ValueError):
        cache.mset_json({"ok": 1, "bad": math.nan})
    assert cache.get("ok") is None


def test_hset_json_hget_json(cache: Cache) -> None:
    assert cache.hset_json("profile", "prefs", {"theme": "dark"}) is True
    assert cache.hset_json("profile", "prefs", {"theme": "light"}) is False
    assert cache.hget_json("profile", "prefs") == {"theme": "light"}
    assert cache.hget_json("profile", "missing", "dflt") == "dflt"


def test_lpush_json_lrange_json_order(cache: Cache) -> None:
    assert cache.lpush_json("l", {"n": 1}, {"n": 2}) == 2
    assert cache.lrange_json("l", 0, -1) == [{"n": 2}, {"n": 1}]


def test_rpush_json_lrange_json(cache: Cache) -> None:
    assert cache.rpush_json("events", {"n": 1}, {"n": 2}) == 2
    assert cache.lrange_json("events", 0, -1) == [{"n": 1}, {"n": 2}]


def test_lrange_json_corrupt_element_raises(cache: Cache) -> None:
    cache.rpush("l", "{ok")
    with pytest.raises(ValueError):
        cache.lrange_json("l", 0, -1)


def test_serializer_json_routes_set_get(jcache: Cache) -> None:
    jcache.set("k", {"x": [1, None]})
    assert jcache.get("k") == {"x": [1, None]}


def test_serializer_json_mget_mset(jcache: Cache) -> None:
    jcache.mset({"a": {"v": 1}, "b": [2]})
    assert jcache.mget(["a", "b", "z"]) == [{"v": 1}, [2], None]


def test_serializer_json_dict_sugar(jcache: Cache) -> None:
    jcache["config"] = {"retries": 3, "hosts": ["a", "b"]}
    assert jcache["config"] == {"retries": 3, "hosts": ["a", "b"]}


def test_serializer_json_get_corrupt_is_value_error() -> None:
    with Cache(serializer="json") as c:
        # Bypass the serializer and write non-JSON bytes straight into the core.
        _NativeCache.set(c, "raw", b"not-json")
        with pytest.raises(ValueError):
            c.get("raw")
        with pytest.raises(ValueError):
            c["raw"]


def test_serializer_json_set_rejects_nan(jcache: Cache) -> None:
    with pytest.raises(ValueError):
        jcache.set("n", math.nan)


def test_serializer_json_rejects_non_serialisable(jcache: Cache) -> None:
    with pytest.raises(TypeError):
        jcache.set("o", object())


def test_serializer_json_memoize_returns_dict(jcache: Cache) -> None:
    calls = []

    @jcache.memoize(ttl=60)
    def build(n: int) -> dict:
        calls.append(n)
        return {"n": n, "sq": n * n}

    assert build(3) == {"n": 3, "sq": 9}
    assert build(3) == {"n": 3, "sq": 9}
    assert calls == [3]


def test_json_ttl_with_real_sleep(cache: Cache) -> None:
    cache.set_json("short", [1], px=SHORT_PX)
    time.sleep(0.15)
    assert cache.get_json("short") is None
