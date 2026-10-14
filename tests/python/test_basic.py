"""Coercion, return types, NX/XX conditions, strings and error mapping."""

from __future__ import annotations

import pytest

from tinycache import Cache


def test_version_is_a_string() -> None:
    import tinycache

    assert isinstance(tinycache.__version__, str)
    assert tinycache.__version__


def test_all_exports_resolve() -> None:
    import tinycache

    for name in tinycache.__all__:
        assert hasattr(tinycache, name), name


def test_default_get_returns_bytes(cache: Cache) -> None:
    cache.set("k", "v")
    assert cache.get("k") == b"v"
    assert isinstance(cache.get("k"), bytes)


def test_decode_true_returns_str(dcache: Cache) -> None:
    dcache.set("k", "héllo")
    assert dcache.get("k") == "héllo"
    assert isinstance(dcache.get("k"), str)


def test_str_value_is_utf8_encoded(cache: Cache) -> None:
    cache.set("k", "héllo")
    assert cache.get("k") == "héllo".encode("utf-8")


def test_bytes_value_round_trips(cache: Cache) -> None:
    payload = bytes(range(256))
    cache.set(b"bin", payload)
    assert cache.get(b"bin") == payload


def test_str_and_bytes_keys_are_equivalent(cache: Cache) -> None:
    cache.set("same", 1)
    assert cache.get(b"same") == b"1"


def test_int_value_stored_as_decimal_text(cache: Cache) -> None:
    cache.set("n", 42)
    assert cache.get("n") == b"42"


def test_negative_int_value(cache: Cache) -> None:
    cache.set("n", -7)
    assert cache.get("n") == b"-7"


def test_float_value_stored_as_text(cache: Cache) -> None:
    cache.set("f", 1.5)
    assert cache.get("f") == b"1.5"


def test_bool_value_is_rejected(cache: Cache) -> None:
    with pytest.raises(TypeError):
        cache.set("b", True)


def test_none_value_is_rejected(cache: Cache) -> None:
    with pytest.raises(TypeError):
        cache.set("n", None)


def test_list_value_is_rejected(cache: Cache) -> None:
    with pytest.raises(TypeError):
        cache.set("l", [1, 2])


def test_missing_key_returns_none(cache: Cache) -> None:
    assert cache.get("nope") is None


def test_set_returns_true(cache: Cache) -> None:
    assert cache.set("k", "v") is True


def test_overwrite_replaces_value(cache: Cache) -> None:
    cache.set("k", "one")
    cache.set("k", "two")
    assert cache.get("k") == b"two"


def test_nx_sets_only_when_missing(cache: Cache) -> None:
    assert cache.set("k", "first", nx=True) is True
    assert cache.set("k", "second", nx=True) is False
    assert cache.get("k") == b"first"


def test_xx_sets_only_when_present(cache: Cache) -> None:
    assert cache.set("k", "v", xx=True) is False
    assert cache.get("k") is None
    cache.set("k", "v")
    assert cache.set("k", "w", xx=True) is True
    assert cache.get("k") == b"w"


def test_nx_and_xx_together_is_an_error(cache: Cache) -> None:
    with pytest.raises(ValueError):
        cache.set("k", "v", nx=True, xx=True)


def test_ex_and_px_together_is_an_error(cache: Cache) -> None:
    with pytest.raises(ValueError):
        cache.set("k", "v", ex=1, px=1000)


@pytest.mark.parametrize("bad", [0, -1])
def test_non_positive_ex_is_an_error(cache: Cache, bad: int) -> None:
    with pytest.raises(ValueError):
        cache.set("k", "v", ex=bad)


def test_ex_must_be_int_not_bool(cache: Cache) -> None:
    with pytest.raises(ValueError):
        cache.set("k", "v", ex=True)


def test_mget_returns_list_in_order(cache: Cache) -> None:
    cache.set("a", "1")
    cache.set("c", "3")
    assert cache.mget(["a", "b", "c"]) == [b"1", None, b"3"]


def test_mget_with_decode(dcache: Cache) -> None:
    dcache.set("a", "x")
    assert dcache.mget(["a", "z"]) == ["x", None]


def test_mget_empty_list(cache: Cache) -> None:
    assert cache.mget([]) == []


def test_mset_writes_all_entries(cache: Cache) -> None:
    cache.mset({"a": "1", "b": 2, "c": b"3"})
    assert cache.get("a") == b"1"
    assert cache.get("b") == b"2"
    assert cache.get("c") == b"3"


def test_mset_rejects_bad_value_without_partial_write_for_json(jcache: Cache) -> None:
    with pytest.raises(TypeError):
        jcache.mset({"good": 1, "bad": object()})
    assert jcache.get("good") is None


def test_append_creates_and_extends(cache: Cache) -> None:
    assert cache.append("s", "ab") == 2
    assert cache.append("s", "cd") == 4
    assert cache.get("s") == b"abcd"


def test_incr_missing_key_starts_at_zero(cache: Cache) -> None:
    assert cache.incr("n") == 1
    assert cache.incr("n") == 2


def test_incr_with_amount(cache: Cache) -> None:
    assert cache.incr("n", 10) == 10
    assert cache.incr("n", 5) == 15


def test_decr_default_and_amount(cache: Cache) -> None:
    assert cache.decr("n") == -1
    assert cache.decr("n", 4) == -5


def test_incr_on_non_integer_is_value_error(cache: Cache) -> None:
    cache.set("s", "hello")
    with pytest.raises(ValueError):
        cache.incr("s")


def test_incr_on_float_text_is_value_error(cache: Cache) -> None:
    cache.set("f", 1.5)
    with pytest.raises(ValueError):
        cache.incr("f")


def test_incr_overflow_is_value_error(cache: Cache) -> None:
    cache.set("big", str(2**63 - 1))
    with pytest.raises(ValueError):
        cache.incr("big")


def test_wrong_type_on_string_op_is_type_error(cache: Cache) -> None:
    cache.rpush("lst", "a")
    with pytest.raises(TypeError):
        cache.get("lst")


def test_wrong_type_on_hash_op_is_type_error(cache: Cache) -> None:
    cache.set("s", "v")
    with pytest.raises(TypeError):
        cache.hset("s", "f", "v")


def test_mget_on_wrong_type_gives_none(cache: Cache) -> None:
    cache.sadd("set", "m")
    cache.set("str", "v")
    assert cache.mget(["set", "str"]) == [None, b"v"]


def test_constructor_rejects_unknown_policy() -> None:
    with pytest.raises(ValueError):
        Cache(policy="bogus")


@pytest.mark.parametrize("shards", [0, 3, 6, 100_000])
def test_constructor_rejects_bad_shard_count(shards: int) -> None:
    with pytest.raises(ValueError):
        Cache(shards=shards)


@pytest.mark.parametrize("shards", [1, 2, 64, 1024])
def test_constructor_accepts_power_of_two_shards(shards: int) -> None:
    with Cache(shards=shards) as c:
        c.set("k", "v")
        assert c.get("k") == b"v"


def test_constructor_rejects_unknown_serializer() -> None:
    with pytest.raises(ValueError):
        Cache(serializer="pickle")


def test_constructor_rejects_non_serializer_object() -> None:
    with pytest.raises(ValueError):
        Cache(serializer=object())


def test_close_is_idempotent() -> None:
    c = Cache()
    c.close()
    c.close()
