"""``Cache(persist_dir=...)``: round trips, crash safety, locking, corruption (PLAN 4d/4e).

Every test works in its own ``tmp_path``. Each opened cache is closed in a ``finally``
block, because an open cache keeps its directory locked for the whole process.
"""

from __future__ import annotations

import os
import pathlib
import signal
import subprocess
import sys
import textwrap
import time
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

import pytest

from tinycache import Cache, CacheLockedError, CorruptDataError

SNAPSHOT = "snapshot.tcs"


@pytest.fixture
def opener(tmp_path: pathlib.Path) -> Iterator[Callable[..., Cache]]:
    """Opens persistent caches on ``tmp_path / "data"`` and closes them all at teardown."""
    opened: List[Cache] = []

    def open_cache(**kwargs: Any) -> Cache:
        kwargs.setdefault("persist_dir", str(tmp_path / "data"))
        c = Cache(**kwargs)
        opened.append(c)
        return c

    try:
        yield open_cache
    finally:
        for c in reversed(opened):
            c.close()


@dataclass
class _Point:
    x: int
    y: int


# ---- round trips across close and reopen ------------------------------------


def test_bytes_and_str_strings_survive_reopen(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("s", "hello")
    c.set("b", b"\x00\xff")
    c.set("n", 42)
    c.set("f", 1.5)
    c.close()
    c = opener()
    assert c.get("s") == b"hello"
    assert c.get("b") == b"\x00\xff"
    assert c.get("n") == b"42"
    assert c.get("f") == b"1.5"


def test_strings_with_ttl_keep_their_expiry(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("ttl", "v", ex=3600)
    c.set("pttl", "w", px=3_600_000)
    c.close()
    c = opener()
    assert c.get("ttl") == b"v"
    assert 3500 <= c.ttl("ttl") <= 3600
    assert 3500 <= c.ttl("pttl") <= 3600


def test_hash_round_trip(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.hset("h", "a", "1")
    c.hset("h", "b", "2")
    c.close()
    c = opener()
    assert c.hgetall("h") == {b"a": b"1", b"b": b"2"}
    assert c.hlen("h") == 2


def test_list_round_trip_keeps_order(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.rpush("l", "b", "c")
    c.lpush("l", "a")
    c.close()
    c = opener()
    assert c.lrange("l", 0, -1) == [b"a", b"b", b"c"]


def test_set_round_trip(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.sadd("s", "x", "y", "z")
    c.srem("s", "y")
    c.close()
    c = opener()
    assert c.smembers("s") == {b"x", b"z"}
    assert c.sismember("s", "x") is True


def test_counter_survives_reopen(opener: Callable[..., Cache]) -> None:
    c = opener()
    assert c.incr("n", 5) == 5
    assert c.incr("n", 2) == 7
    c.close()
    c = opener()
    assert c.get("n") == b"7"
    assert c.incr("n") == 8


def test_append_survives_reopen(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("a", "ab")
    c.append("a", "cd")
    c.close()
    c = opener()
    assert c.get("a") == b"abcd"


def test_delete_and_expire_and_persist_are_replayed(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("gone", "1")
    c.set("kept", "2")
    c.delete("gone")
    c.set("exp", "3")
    c.expire("exp", 3600)
    c.set("nopersist", "4", ex=3600)
    c.persist("nopersist")
    c.close()
    c = opener()
    assert c.exists("gone") is False
    assert c.get("kept") == b"2"
    assert 3500 <= c.ttl("exp") <= 3600
    assert c.ttl("nopersist") == -1


def test_flush_is_replayed(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("a", "1")
    c.flush()
    c.set("b", "2")
    c.close()
    c = opener()
    assert c.keys() == [b"b"]


def test_json_values_round_trip(opener: Callable[..., Cache]) -> None:
    value = {"name": "tiny", "nums": [1, 2.5, None], "nested": {"ok": True}}
    c = opener(serializer="json")
    c.set("j", value)
    c.hset_json("hj", "f", [1, "two"])
    c.close()
    c = opener(serializer="json")
    assert c.get("j") == value
    assert c.hget_json("hj", "f") == [1, "two"]


def test_pydantic_models_round_trip(opener: Callable[..., Cache]) -> None:
    pydantic = pytest.importorskip("pydantic")

    class Address(pydantic.BaseModel):
        city: str

    class User(pydantic.BaseModel):
        id: int
        name: str
        address: Optional[Address] = None

    c = opener()
    c.set_model("u", User(id=1, name="ann", address=Address(city="Oslo")))
    c.set_model("list", [User(id=2, name="bo")])
    c.close()
    c = opener()
    assert c.get_model("u", User) == User(id=1, name="ann", address=Address(city="Oslo"))
    assert c.get_model("list", list[User]) == [User(id=2, name="bo")]


def test_dataclass_json_round_trip_via_dict(opener: Callable[..., Cache]) -> None:
    c = opener(serializer="json")
    c.set("p", {"x": 1, "y": 2})
    c.close()
    c = opener(serializer="json")
    assert _Point(**c.get("p")) == _Point(1, 2)


def test_unicode_keys_and_values_persist(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("ключ", "значение ✓")
    c.close()
    c = opener()
    assert c.get("ключ") == "значение ✓".encode()


def test_many_keys_round_trip(opener: Callable[..., Cache]) -> None:
    c = opener(shards=16)
    for i in range(2000):
        c.set(f"k{i}", f"v{i}")
    c.close()
    c = opener(shards=16)
    assert len(c) == 2000
    assert c.get("k1999") == b"v1999"


def test_decode_and_serializer_are_not_persisted_flags(opener: Callable[..., Cache]) -> None:
    c = opener(decode=True)
    c.set("s", "x")
    c.close()
    c = opener()
    assert c.get("s") == b"x"


# ---- expiry ----------------------------------------------------------------


def test_expired_keys_are_not_restored(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("short", "v", px=100)
    c.set("long", "v", ex=3600)
    c.close()
    time.sleep(0.25)
    c = opener()
    assert c.exists("short") is False
    assert c.exists("long") is True
    assert len(c) == 1


def test_expiry_counts_downtime(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("k", "v", px=300)
    c.close()
    time.sleep(0.4)
    c = opener()
    assert c.get("k") is None


# ---- save, persist_stats and the persistence flags -----------------------------


def test_save_then_stats_report_a_snapshot(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("a", "1")
    c.save()
    stats = c.persist_stats
    assert stats is not None
    assert stats["snapshot_bytes"] > 0
    assert stats["last_save_ms"] > 0
    assert stats["last_error"] is None


def test_persist_stats_has_the_documented_shape(opener: Callable[..., Cache]) -> None:
    c = opener()
    stats = c.persist_stats
    assert stats is not None
    assert set(stats) == {"last_save_ms", "snapshot_bytes", "wal_bytes", "loaded_keys", "last_error"}
    assert isinstance(stats["last_save_ms"], int)
    assert isinstance(stats["snapshot_bytes"], int)
    assert isinstance(stats["wal_bytes"], int)
    assert isinstance(stats["loaded_keys"], int)
    assert stats["last_error"] is None or isinstance(stats["last_error"], str)


def test_loaded_keys_counts_recovered_keys(opener: Callable[..., Cache]) -> None:
    c = opener()
    c.set("a", "1")
    c.set("b", "2")
    c.close()
    c = opener()
    assert c.persist_stats["loaded_keys"] == 2


def test_wal_bytes_grow_with_writes_and_reset_after_save(opener: Callable[..., Cache]) -> None:
    c = opener()
    base = c.persist_stats["wal_bytes"]
    c.set("k", "x" * 500)
    grown = c.persist_stats["wal_bytes"]
    assert grown > base
    c.save()
    assert c.persist_stats["wal_bytes"] < grown


def test_persistent_property(opener: Callable[..., Cache]) -> None:
    assert opener().persistent is True
    assert Cache().persistent is False


def test_non_persistent_cache_has_no_stats() -> None:
    c = Cache()
    try:
        assert c.persist_stats is None
    finally:
        c.close()


def test_save_without_persistence_raises_value_error() -> None:
    c = Cache()
    try:
        with pytest.raises(ValueError, match="persistence is not enabled"):
            c.save()
    finally:
        c.close()


def test_persist_dir_accepts_pathlike(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=tmp_path / "pl")
    try:
        c.set("x", "1")
        c.save()
    finally:
        c.close()
    assert (tmp_path / "pl" / SNAPSHOT).exists()


def test_snapshot_file_is_created_by_save(opener: Callable[..., Cache], tmp_path: pathlib.Path) -> None:
    c = opener()
    c.set("a", "1")
    c.save()
    assert (tmp_path / "data" / SNAPSHOT).exists()


@pytest.mark.parametrize("policy", ["always", "everysec", "never"])
def test_every_fsync_policy_round_trips(tmp_path: pathlib.Path, policy: str) -> None:
    c = Cache(persist_dir=str(tmp_path), fsync=policy)
    try:
        c.set("k", policy)
    finally:
        c.close()
    c = Cache(persist_dir=str(tmp_path), fsync=policy)
    try:
        assert c.get("k") == policy.encode()
    finally:
        c.close()


@pytest.mark.parametrize("bad", ["sometimes", "", "ALWAYS", "no"])
def test_invalid_fsync_is_rejected(tmp_path: pathlib.Path, bad: str) -> None:
    with pytest.raises(ValueError, match="fsync"):
        Cache(persist_dir=str(tmp_path), fsync=bad)


def test_negative_snapshot_interval_is_rejected(tmp_path: pathlib.Path) -> None:
    with pytest.raises(ValueError, match="snapshot_interval"):
        Cache(persist_dir=str(tmp_path), snapshot_interval=-1)


def test_zero_snapshot_interval_is_accepted(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=str(tmp_path), snapshot_interval=0)
    c.close()


def test_background_snapshotter_writes_without_explicit_save(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=str(tmp_path), snapshot_interval=1)
    try:
        c.set("k", "v")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and c.persist_stats["last_save_ms"] == 0:
            time.sleep(0.05)
        assert c.persist_stats["last_save_ms"] > 0
    finally:
        c.close()


# ---- close and context manager --------------------------------------------------


def test_context_manager_writes_the_final_snapshot(tmp_path: pathlib.Path) -> None:
    with Cache(persist_dir=str(tmp_path)) as c:
        c.set("ctx", "1")
    assert (tmp_path / SNAPSHOT).exists()
    with Cache(persist_dir=str(tmp_path)) as c:
        assert c.get("ctx") == b"1"


def test_context_manager_closes_on_exception(tmp_path: pathlib.Path) -> None:
    with pytest.raises(RuntimeError):
        with Cache(persist_dir=str(tmp_path)) as c:
            c.set("boom", "1")
            raise RuntimeError("fail inside block")
    with Cache(persist_dir=str(tmp_path)) as c:
        assert c.get("boom") == b"1"


def test_close_is_idempotent(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=str(tmp_path))
    c.close()
    c.close()


def test_close_releases_the_lock_for_a_new_cache(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=str(tmp_path))
    c.close()
    second = Cache(persist_dir=str(tmp_path))
    second.close()


# ---- crash safety --------------------------------------------------------------


CRASH_WRITER = textwrap.dedent(
    """
    import sys, time
    from tinycache import Cache
    c = Cache(persist_dir=sys.argv[1], fsync="always")
    for i in range(50):
        c.set(f"before{i}", f"v{i}")
    c.save()
    for i in range(20):
        c.set(f"after{i}", f"w{i}")
    print("ready", flush=True)
    time.sleep(60)
    """
)


def test_keys_written_before_sigkill_are_recovered(tmp_path: pathlib.Path) -> None:
    data = tmp_path / "crash"
    proc = subprocess.Popen(
        [sys.executable, "-c", CRASH_WRITER, str(data)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "ready", proc.stderr.read() if proc.stderr else ""
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
    c = Cache(persist_dir=str(data))
    try:
        assert c.get("before0") == b"v0"
        assert c.get("before49") == b"v49"
        assert c.get("after0") == b"w0"
        assert c.get("after19") == b"w19"
        assert len(c) == 70
    finally:
        c.close()


def test_lock_held_by_killed_process_is_taken_over(tmp_path: pathlib.Path) -> None:
    data = tmp_path / "stale"
    proc = subprocess.Popen(
        [sys.executable, "-c", CRASH_WRITER, str(data)],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    proc.stdout.readline()
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=10)
    c = Cache(persist_dir=str(data))
    c.close()


def test_save_then_kill_keeps_the_saved_snapshot(tmp_path: pathlib.Path) -> None:
    data = tmp_path / "saved"
    script = textwrap.dedent(
        """
        import sys, time
        from tinycache import Cache
        c = Cache(persist_dir=sys.argv[1], fsync="always")
        c.set("durable", "yes")
        c.save()
        print("saved", flush=True)
        time.sleep(60)
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", script, str(data)], stdout=subprocess.PIPE, text=True)
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == "saved"
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=10)
    c = Cache(persist_dir=str(data))
    try:
        assert c.get("durable") == b"yes"
    finally:
        c.close()


# ---- lock contention ---------------------------------------------------------


def test_second_cache_on_same_dir_in_same_process_is_locked(tmp_path: pathlib.Path) -> None:
    first = Cache(persist_dir=str(tmp_path))
    try:
        with pytest.raises(CacheLockedError):
            Cache(persist_dir=str(tmp_path))
    finally:
        first.close()


def test_locked_error_is_an_os_error(tmp_path: pathlib.Path) -> None:
    first = Cache(persist_dir=str(tmp_path))
    try:
        with pytest.raises(OSError):
            Cache(persist_dir=str(tmp_path))
    finally:
        first.close()


def test_lock_held_by_live_subprocess_is_refused(tmp_path: pathlib.Path) -> None:
    script = textwrap.dedent(
        """
        import sys, time
        from tinycache import Cache
        c = Cache(persist_dir=sys.argv[1])
        print("locked", flush=True)
        time.sleep(60)
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", script, str(tmp_path)], stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "locked"
        with pytest.raises(CacheLockedError):
            Cache(persist_dir=str(tmp_path))
    finally:
        proc.kill()
        proc.wait(timeout=10)


def test_cache_opens_again_after_the_first_is_closed(tmp_path: pathlib.Path) -> None:
    first = Cache(persist_dir=str(tmp_path))
    first.set("k", "v")
    first.close()
    second = Cache(persist_dir=str(tmp_path))
    try:
        assert second.get("k") == b"v"
    finally:
        second.close()


# ---- corruption ------------------------------------------------------------------


def _flip_byte(path: pathlib.Path, offset: int) -> None:
    data = bytearray(path.read_bytes())
    data[offset] ^= 0xFF
    path.write_bytes(bytes(data))


def test_corrupt_snapshot_raises_corrupt_data_error(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=str(tmp_path))
    c.set("a", "1" * 64)
    c.close()
    snap = tmp_path / SNAPSHOT
    _flip_byte(snap, snap.stat().st_size // 2)
    with pytest.raises(CorruptDataError):
        Cache(persist_dir=str(tmp_path))


def test_corrupt_data_error_is_a_value_error(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=str(tmp_path))
    c.set("a", "1")
    c.close()
    snap = tmp_path / SNAPSHOT
    _flip_byte(snap, snap.stat().st_size - 1)
    with pytest.raises(ValueError):
        Cache(persist_dir=str(tmp_path))


def test_corrupt_snapshot_does_not_leave_the_lock_held(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=str(tmp_path))
    c.set("a", "1")
    c.close()
    snap = tmp_path / SNAPSHOT
    _flip_byte(snap, snap.stat().st_size // 2)
    with pytest.raises(CorruptDataError):
        Cache(persist_dir=str(tmp_path))
    # The failed open must not keep the directory locked.
    with pytest.raises(CorruptDataError):
        Cache(persist_dir=str(tmp_path))


def test_bad_magic_is_corrupt(tmp_path: pathlib.Path) -> None:
    c = Cache(persist_dir=str(tmp_path))
    c.set("a", "1")
    c.close()
    (tmp_path / SNAPSHOT).write_bytes(b"NOTASNAPSHOT" + b"\x00" * 32)
    with pytest.raises(CorruptDataError):
        Cache(persist_dir=str(tmp_path))


def test_exported_exceptions_are_the_native_classes() -> None:
    import tinycache

    assert issubclass(tinycache.CacheLockedError, OSError)
    assert issubclass(tinycache.CorruptDataError, ValueError)
    assert "CacheLockedError" in tinycache.__all__
    assert "CorruptDataError" in tinycache.__all__

