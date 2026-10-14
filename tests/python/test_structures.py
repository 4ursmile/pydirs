"""Hashes, lists and sets, plus their decode behaviour."""

from __future__ import annotations

import pytest

from tinycache import Cache


def test_hset_reports_new_field(cache: Cache) -> None:
    assert cache.hset("h", "f", "1") is True
    assert cache.hset("h", "f", "2") is False
    assert cache.hget("h", "f") == b"2"


def test_hget_missing_field_and_key(cache: Cache) -> None:
    assert cache.hget("nokey", "f") is None
    cache.hset("h", "f", "v")
    assert cache.hget("h", "other") is None


def test_hdel_counts_and_removes_empty_hash(cache: Cache) -> None:
    cache.hset("h", "a", "1")
    cache.hset("h", "b", "2")
    assert cache.hdel("h", "a", "zzz") == 1
    assert cache.hdel("h", "b") == 1
    assert cache.exists("h") is False


def test_hgetall_returns_dict(cache: Cache) -> None:
    cache.hset("h", "name", "Ada")
    cache.hset("h", "lang", "en")
    assert cache.hgetall("h") == {b"name": b"Ada", b"lang": b"en"}


def test_hgetall_missing_key_is_empty_dict(cache: Cache) -> None:
    assert cache.hgetall("missing") == {}


def test_hgetall_decode(dcache: Cache) -> None:
    dcache.hset("h", "name", "Ada")
    assert dcache.hgetall("h") == {"name": "Ada"}


def test_hlen(cache: Cache) -> None:
    assert cache.hlen("h") == 0
    cache.hset("h", "a", 1)
    cache.hset("h", "b", 2)
    assert cache.hlen("h") == 2


def test_hash_field_int_coercion(cache: Cache) -> None:
    cache.hset("h", 7, 9)
    assert cache.hget("h", "7") == b"9"


def test_lpush_order_matches_redis(cache: Cache) -> None:
    assert cache.lpush("l", "a", "b", "c") == 3
    assert cache.lrange("l", 0, -1) == [b"c", b"b", b"a"]


def test_rpush_appends(cache: Cache) -> None:
    assert cache.rpush("l", "a") == 1
    assert cache.rpush("l", "b", "c") == 3
    assert cache.lrange("l", 0, -1) == [b"a", b"b", b"c"]


def test_lpop_and_rpop(cache: Cache) -> None:
    cache.rpush("l", "a", "b", "c")
    assert cache.lpop("l") == b"a"
    assert cache.rpop("l") == b"c"
    assert cache.llen("l") == 1


def test_pop_missing_list_is_none(cache: Cache) -> None:
    assert cache.lpop("nope") is None
    assert cache.rpop("nope") is None


def test_empty_list_is_removed(cache: Cache) -> None:
    cache.rpush("l", "only")
    cache.lpop("l")
    assert cache.exists("l") is False
    assert cache.llen("l") == 0


def test_lrange_negative_indices(cache: Cache) -> None:
    cache.rpush("l", "a", "b", "c", "d")
    assert cache.lrange("l", -2, -1) == [b"c", b"d"]
    assert cache.lrange("l", 1, 2) == [b"b", b"c"]
    assert cache.lrange("l", 10, 20) == []


def test_lrange_decode(dcache: Cache) -> None:
    dcache.rpush("l", "x", "y")
    assert dcache.lrange("l", 0, -1) == ["x", "y"]


def test_list_on_string_key_is_type_error(cache: Cache) -> None:
    cache.set("s", "v")
    with pytest.raises(TypeError):
        cache.rpush("s", "x")


def test_sadd_counts_new_members(cache: Cache) -> None:
    assert cache.sadd("s", "a", "b") == 2
    assert cache.sadd("s", "b", "c") == 1


def test_srem_counts_removed(cache: Cache) -> None:
    cache.sadd("s", "a", "b")
    assert cache.srem("s", "a", "zzz") == 1
    assert cache.scard("s") == 1


def test_smembers_returns_set(cache: Cache) -> None:
    cache.sadd("s", "a", "b")
    assert cache.smembers("s") == {b"a", b"b"}


def test_smembers_missing_is_empty_set(cache: Cache) -> None:
    assert cache.smembers("missing") == set()


def test_smembers_decode(dcache: Cache) -> None:
    dcache.sadd("s", "a")
    assert dcache.smembers("s") == {"a"}


def test_sismember(cache: Cache) -> None:
    cache.sadd("s", "a")
    assert cache.sismember("s", "a") is True
    assert cache.sismember("s", "b") is False
    assert cache.sismember("missing", "a") is False


def test_scard(cache: Cache) -> None:
    assert cache.scard("s") == 0
    cache.sadd("s", 1, 2, 3)
    assert cache.scard("s") == 3


def test_empty_set_is_removed(cache: Cache) -> None:
    cache.sadd("s", "only")
    cache.srem("s", "only")
    assert cache.exists("s") is False


def test_set_on_hash_key_is_type_error(cache: Cache) -> None:
    cache.hset("h", "f", "v")
    with pytest.raises(TypeError):
        cache.sadd("h", "m")
