"""Memory limits and the three eviction policies."""

from __future__ import annotations

import pytest

from tinycache import Cache

PAYLOAD = "x" * 400


def test_noeviction_raises_memory_error_when_full() -> None:
    with Cache(max_memory=2000, policy="noeviction") as c:
        with pytest.raises(MemoryError):
            for i in range(100):
                c.set(f"k{i}", PAYLOAD)


def test_noeviction_keeps_existing_keys() -> None:
    with Cache(max_memory=2000, policy="noeviction") as c:
        c.set("first", PAYLOAD)
        with pytest.raises(MemoryError):
            for i in range(100):
                c.set(f"k{i}", PAYLOAD)
        assert c.get("first") == PAYLOAD.encode()


def test_allkeys_lru_evicts_instead_of_failing() -> None:
    with Cache(max_memory=4000, policy="allkeys-lru") as c:
        for i in range(50):
            c.set(f"k{i}", PAYLOAD)
        assert len(c) < 50
        assert c.used_memory <= 4000


def test_allkeys_lru_keeps_recently_used_key() -> None:
    with Cache(max_memory=4000, policy="allkeys-lru", shards=1) as c:
        c.set("hot", PAYLOAD)
        for i in range(30):
            c.get("hot")
            c.set(f"cold{i}", PAYLOAD)
        assert c.get("hot") == PAYLOAD.encode()


def test_volatile_lru_only_evicts_keys_with_ttl() -> None:
    with Cache(max_memory=4000, policy="volatile-lru", shards=1) as c:
        c.set("forever", PAYLOAD)
        for i in range(100):
            c.set(f"ttl{i}", PAYLOAD, ex=3600)
        assert c.get("forever") == PAYLOAD.encode()
        assert c.used_memory <= 4000


def test_volatile_lru_without_ttl_keys_raises() -> None:
    # Each 400-byte entry accounts for about 465 bytes; three exceed a 1000-byte budget.
    with Cache(max_memory=1000, policy="volatile-lru", shards=1) as c:
        c.set("a", PAYLOAD)
        c.set("b", PAYLOAD)
        with pytest.raises(MemoryError):
            c.set("c", PAYLOAD)


def test_volatile_lru_evicts_expiring_keys() -> None:
    with Cache(max_memory=4000, policy="volatile-lru") as c:
        for i in range(50):
            c.set(f"ttl{i}", PAYLOAD, ex=3600)
        assert len(c) < 50


def test_used_memory_stays_under_limit_with_lru() -> None:
    with Cache(max_memory=8000, policy="allkeys-lru") as c:
        for i in range(200):
            c.set(f"k{i}", "y" * (i % 97 + 1))
        assert c.used_memory <= 8000


def test_unlimited_by_default() -> None:
    with Cache() as c:
        for i in range(200):
            c.set(f"k{i}", PAYLOAD)
        assert len(c) == 200


def test_unknown_policy_rejected() -> None:
    with pytest.raises(ValueError):
        Cache(max_memory=100, policy="random")
