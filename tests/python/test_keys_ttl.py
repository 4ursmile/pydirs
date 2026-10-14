"""Key management, TTL semantics (px=50, 0.15 s sleeps) and glob patterns."""

from __future__ import annotations

import pytest

from tinycache import Cache

from helpers import SHORT_PX, wait_for_expiry


def test_exists_true_and_false(cache: Cache) -> None:
    assert cache.exists("k") is False
    cache.set("k", "v")
    assert cache.exists("k") is True


def test_delete_returns_number_removed(cache: Cache) -> None:
    cache.set("a", 1)
    cache.set("b", 2)
    assert cache.delete("a", "b", "missing") == 2
    assert cache.exists("a") is False


def test_delete_with_no_keys(cache: Cache) -> None:
    assert cache.delete() == 0


def test_len_counts_live_keys(cache: Cache) -> None:
    assert len(cache) == 0
    cache.set("a", 1)
    cache.set("b", 2)
    assert len(cache) == 2


def test_flush_removes_everything(cache: Cache) -> None:
    cache.set("a", 1)
    cache.rpush("l", "x")
    cache.flush()
    assert len(cache) == 0
    assert cache.get("a") is None


def test_keys_default_pattern_returns_all(cache: Cache) -> None:
    cache.set("alpha", 1)
    cache.set("beta", 2)
    assert sorted(cache.keys()) == [b"alpha", b"beta"]


def test_keys_glob_star_and_question(cache: Cache) -> None:
    for k in ["user:1", "user:22", "item:1"]:
        cache.set(k, 1)
    assert sorted(cache.keys("user:*")) == [b"user:1", b"user:22"]
    assert sorted(cache.keys("user:?")) == [b"user:1"]


def test_keys_glob_character_class(cache: Cache) -> None:
    for k in ["a1", "a2", "a3", "b1"]:
        cache.set(k, 1)
    assert sorted(cache.keys("a[12]")) == [b"a1", b"a2"]


def test_expire_sets_ttl_and_ttl_reports_it(cache: Cache) -> None:
    cache.set("k", "v")
    assert cache.expire("k", 100) is True
    assert 99 <= cache.ttl("k") <= 100


def test_expire_on_missing_key_is_false(cache: Cache) -> None:
    assert cache.expire("missing", 10) is False


def test_expire_rejects_non_positive(cache: Cache) -> None:
    cache.set("k", "v")
    with pytest.raises(ValueError):
        cache.expire("k", 0)


def test_pexpire_and_pttl(cache: Cache) -> None:
    cache.set("k", "v")
    assert cache.pexpire("k", 5000) is True
    assert 4000 < cache.pttl("k") <= 5000


def test_ttl_sentinels(cache: Cache) -> None:
    assert cache.ttl("missing") == -2
    assert cache.pttl("missing") == -2
    cache.set("k", "v")
    assert cache.ttl("k") == -1
    assert cache.pttl("k") == -1


def test_ttl_rounds_up_to_whole_seconds(cache: Cache) -> None:
    cache.set("k", "v")
    cache.pexpire("k", 1500)
    assert cache.ttl("k") == 2


def test_set_with_px_expires(cache: Cache) -> None:
    cache.set("k", "v", px=SHORT_PX)
    assert cache.get("k") == b"v"
    wait_for_expiry()
    assert cache.get("k") is None
    assert cache.exists("k") is False


def test_set_with_ex_sets_ttl(cache: Cache) -> None:
    cache.set("k", "v", ex=100)
    assert 99 <= cache.ttl("k") <= 100


def test_expired_key_is_not_counted_or_listed(cache: Cache) -> None:
    cache.set("gone", "v", px=SHORT_PX)
    cache.set("kept", "v")
    wait_for_expiry()
    assert len(cache) == 1
    assert cache.keys() == [b"kept"]


def test_expired_key_is_absent_for_nx(cache: Cache) -> None:
    cache.set("k", "old", px=SHORT_PX)
    wait_for_expiry()
    assert cache.set("k", "new", nx=True) is True
    assert cache.get("k") == b"new"


def test_expired_key_is_absent_for_xx(cache: Cache) -> None:
    cache.set("k", "old", px=SHORT_PX)
    wait_for_expiry()
    assert cache.set("k", "new", xx=True) is False


def test_expire_on_expired_key_is_false(cache: Cache) -> None:
    cache.set("k", "v", px=SHORT_PX)
    wait_for_expiry()
    assert cache.expire("k", 10) is False


def test_persist_removes_expiry(cache: Cache) -> None:
    cache.set("k", "v", ex=100)
    assert cache.persist("k") is True
    assert cache.ttl("k") == -1
    assert cache.persist("k") is False


def test_persist_on_missing_key_is_false(cache: Cache) -> None:
    assert cache.persist("missing") is False


def test_plain_set_clears_previous_ttl(cache: Cache) -> None:
    cache.set("k", "v", px=SHORT_PX)
    cache.set("k", "w")
    wait_for_expiry()
    assert cache.get("k") == b"w"
    assert cache.ttl("k") == -1


def test_used_memory_tracks_writes_and_deletes(cache: Cache) -> None:
    before = cache.used_memory
    cache.set("big", "x" * 1000)
    grown = cache.used_memory
    assert grown > before
    cache.delete("big")
    assert cache.used_memory < grown


def test_used_memory_is_zero_after_flush(cache: Cache) -> None:
    cache.set("k", "x" * 100)
    cache.flush()
    assert cache.used_memory == 0
