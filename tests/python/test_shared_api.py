"""One behaviour suite run against both ``Cache`` and ``Client`` (PLAN section 4e).

Every test takes ``make_api``, which yields a fresh object per backend. The
assertions are the same for both, so a difference between the two is a bug.
"""

from __future__ import annotations

import pytest

from helpers import SHORT_PX, wait_for_expiry


def test_set_get_round_trip(make_api) -> None:
    api = make_api()
    assert api.set("k", "v") is True
    assert api.get("k") == b"v"


def test_get_missing_is_none(make_api) -> None:
    assert make_api().get("nope") is None


def test_set_nx_and_xx(make_api) -> None:
    api = make_api()
    assert api.set("k", "1", nx=True) is True
    assert api.set("k", "2", nx=True) is False
    assert api.set("k", "3", xx=True) is True
    assert api.set("missing", "x", xx=True) is False
    assert api.get("k") == b"3"


def test_set_px_expires(make_api) -> None:
    api = make_api()
    api.set("k", "v", px=SHORT_PX)
    wait_for_expiry()
    assert api.get("k") is None


def test_set_ex_sets_ttl(make_api) -> None:
    api = make_api()
    api.set("k", "v", ex=100)
    assert 99 <= api.ttl("k") <= 100


def test_mget_and_mset(make_api) -> None:
    api = make_api()
    api.mset({"a": "1", "b": 2})
    assert api.mget(["a", "missing", "b"]) == [b"1", None, b"2"]
    assert api.mget([]) == []


def test_incr_decr(make_api) -> None:
    api = make_api()
    assert api.incr("n") == 1
    assert api.incr("n", 9) == 10
    assert api.decr("n", 4) == 6
    assert api.decr("n") == 5


def test_append(make_api) -> None:
    api = make_api()
    assert api.append("s", "ab") == 2
    assert api.append("s", "cd") == 4
    assert api.get("s") == b"abcd"


def test_incr_on_text_is_value_error(make_api) -> None:
    api = make_api()
    api.set("s", "hello")
    with pytest.raises(ValueError):
        api.incr("s")


def test_wrong_type_is_type_error(make_api) -> None:
    api = make_api()
    api.rpush("l", "a")
    with pytest.raises(TypeError):
        api.get("l")


def test_delete_exists_len_flush(make_api) -> None:
    api = make_api()
    api.set("a", 1)
    api.set("b", 2)
    assert api.exists("a") is True
    assert api.delete("a", "b", "missing") == 2
    assert api.exists("a") is False
    api.set("c", 3)
    assert len(api) == 1
    api.flush()
    assert len(api) == 0


def test_expire_persist_ttl_pttl(make_api) -> None:
    api = make_api()
    api.set("k", "v")
    assert api.ttl("k") == -1
    assert api.expire("k", 100) is True
    assert 99 <= api.ttl("k") <= 100
    assert api.persist("k") is True
    assert api.ttl("k") == -1
    assert api.ttl("missing") == -2
    assert api.pexpire("k", 5000) is True
    assert 4000 < api.pttl("k") <= 5000


def test_keys_glob(make_api) -> None:
    api = make_api()
    for k in ["user:1", "user:22", "item:1"]:
        api.set(k, 1)
    assert sorted(api.keys("user:*")) == [b"user:1", b"user:22"]


def test_hash_ops(make_api) -> None:
    api = make_api()
    assert api.hset("h", "a", "1") is True
    assert api.hset("h", "a", "2") is False
    api.hset("h", "b", "3")
    assert api.hget("h", "a") == b"2"
    assert api.hgetall("h") == {b"a": b"2", b"b": b"3"}
    assert api.hlen("h") == 2
    assert api.hdel("h", "a", "zzz") == 1


def test_list_ops(make_api) -> None:
    api = make_api()
    assert api.lpush("l", "a", "b") == 2
    assert api.rpush("l", "c") == 3
    assert api.lrange("l", 0, -1) == [b"b", b"a", b"c"]
    assert api.lpop("l") == b"b"
    assert api.rpop("l") == b"c"
    assert api.llen("l") == 1


def test_set_ops(make_api) -> None:
    api = make_api()
    assert api.sadd("s", "a", "b") == 2
    assert api.sadd("s", "b", "c") == 1
    assert api.srem("s", "a", "zzz") == 1
    assert api.smembers("s") == {b"b", b"c"}
    assert api.sismember("s", "b") is True
    assert api.scard("s") == 2


def test_dict_sugar(make_api) -> None:
    api = make_api()
    api["k"] = "v"
    assert api["k"] == b"v"
    assert "k" in api
    del api["k"]
    assert "k" not in api
    with pytest.raises(KeyError):
        _ = api["k"]
    with pytest.raises(KeyError):
        del api["k"]


def test_decode_true_returns_str(make_api) -> None:
    api = make_api(decode=True)
    api.set("k", "héllo")
    assert api.get("k") == "héllo"
    assert api.hgetall("h") == {}
    api.hset("h", "f", "v")
    assert api.hgetall("h") == {"f": "v"}


def test_json_round_trip(make_api) -> None:
    api = make_api()
    api.set_json("k", {"a": [1, None, "é"]})
    assert api.get_json("k") == {"a": [1, None, "é"]}
    assert api.get_json("missing", default="d") == "d"


def test_json_serializer_routes_set_get(make_api) -> None:
    api = make_api(serializer="json")
    api.set("k", {"x": 1})
    assert api.get("k") == {"x": 1}
    api.mset({"a": [1], "b": None})
    assert api.mget(["a", "b"]) == [[1], None]


def test_memoize_sync(make_api) -> None:
    api = make_api(serializer="json")
    calls: list[int] = []

    @api.memoize(ttl=60)
    def double(n: int) -> int:
        calls.append(n)
        return n * 2

    assert double(4) == 8
    assert double(4) == 8
    assert calls == [4]


def test_context_manager_closes(make_api) -> None:
    api = make_api()
    with api as entered:
        assert entered is api
        api.set("k", "v")
    api.close()


def test_bytes_and_str_keys_equivalent(make_api) -> None:
    api = make_api()
    api.set("same", 1)
    assert api.get(b"same") == b"1"
