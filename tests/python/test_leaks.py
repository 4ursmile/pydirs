"""Memory and file-descriptor leak checks (PLAN section 4f). Marker: ``leak``.

Each test runs a workload many times and asserts that memory and fd counts are
flat after warm-up. Growth proportional to iterations fails. RSS is read from
``/proc/self/statm`` on Linux and from ``ps`` elsewhere. Run only these with
``python -m pytest -m leak``.
"""

from __future__ import annotations

import asyncio
import gc
import os
import subprocess
import threading
from collections.abc import Callable, Iterator
from typing import Optional

import pytest

from tinycache import Cache, Client, Server

pytestmark = pytest.mark.leak

MB = 1024 * 1024
RSS_BUDGET = 4 * MB  # allowed growth over the measured half of a run


def _rss_bytes() -> Optional[int]:
    """Resident set size of this process in bytes, or ``None`` if it cannot be read."""
    try:
        with open("/proc/self/statm") as fh:
            pages = int(fh.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except OSError:
        pass
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout.strip()
        return int(out) * 1024 if out else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def _fd_count() -> int:
    try:
        return len(os.listdir("/dev/fd"))
    except OSError:  # pragma: no cover - no /dev/fd on this platform
        return -1


def _measure_rss_growth(step: Callable[[int], None], iterations: int) -> int:
    """Runs ``step`` for ``iterations``, returns RSS growth across the second half.

    A warm-up pass runs first so allocator and import caches settle. ``gc`` runs
    before each sample so freed cycles do not count as growth.
    """
    for i in range(iterations // 4):
        step(i)
    gc.collect()
    before = _rss_bytes()
    for i in range(iterations // 4, iterations // 4 + iterations // 2):
        step(i)
    gc.collect()
    after = _rss_bytes()
    if before is None or after is None:
        pytest.skip("RSS not readable on this platform")
    return after - before


def _assert_flat(growth: int) -> None:
    assert growth <= RSS_BUDGET, f"RSS grew by {growth / MB:.2f} MiB over the measured half"


@pytest.fixture
def server() -> Iterator[Server]:
    cache = Cache()
    srv = Server(cache, host="127.0.0.1", port=0)
    srv.start()
    try:
        yield srv
    finally:
        srv.stop()
        cache.close()


# ---- Cache lifecycle and workloads ---------------------------------------------


def test_cache_create_close_del_is_flat() -> None:
    def step(_i: int) -> None:
        c = Cache(shards=16)
        c.set("k", "v" * 64)
        c.close()
        del c

    _assert_flat(_measure_rss_growth(step, 500))


def test_set_get_delete_loop_is_flat() -> None:
    cache = Cache()

    def step(i: int) -> None:
        key = f"k{i % 1000}"
        cache.set(key, "x" * 64)
        cache.get(key)
        cache.delete(key)

    try:
        _assert_flat(_measure_rss_growth(step, 20_000))
    finally:
        cache.close()


def test_large_results_discarded_are_flat() -> None:
    cache = Cache()
    for i in range(200):
        cache.rpush("l", f"item{i}")
        cache.hset("h", f"f{i}", "v" * 32)
        cache.sadd("s", f"m{i}")

    def step(_i: int) -> None:
        cache.mget([f"k{j}" for j in range(50)])
        cache.hgetall("h")
        cache.smembers("s")
        cache.lrange("l", 0, -1)

    try:
        _assert_flat(_measure_rss_growth(step, 1000))
    finally:
        cache.close()


def test_exceptions_in_a_loop_do_not_leak() -> None:
    cache = Cache()
    cache.rpush("l", "x")
    cache.set("s", "text")

    def step(_i: int) -> None:
        for call in (
            lambda: cache.get("l"),
            lambda: cache.incr("s"),
            lambda: cache.hset("s", "f", "v"),
            lambda: cache.set("k", object()),
        ):
            try:
                call()
            except (TypeError, ValueError):
                pass

    try:
        _assert_flat(_measure_rss_growth(step, 4000))
    finally:
        cache.close()


def test_json_round_trip_loop_is_flat() -> None:
    cache = Cache(serializer="json")
    payload = {"a": list(range(20)), "b": {"c": "héllo", "d": None}}

    def step(i: int) -> None:
        cache.set_json(f"j{i % 500}", payload)
        cache.get_json(f"j{i % 500}")

    try:
        _assert_flat(_measure_rss_growth(step, 6000))
    finally:
        cache.close()


def test_pydantic_round_trip_loop_is_flat() -> None:
    pydantic = pytest.importorskip("pydantic")

    class Item(pydantic.BaseModel):
        id: int
        name: str

    cache = Cache()

    def step(i: int) -> None:
        cache.set_model(f"m{i % 500}", Item(id=i, name="n"))
        cache.get_model(f"m{i % 500}", Item)

    try:
        _assert_flat(_measure_rss_growth(step, 4000))
    finally:
        cache.close()


def test_memoize_unique_args_is_bounded_by_ttl() -> None:
    cache = Cache(serializer="json")

    @cache.memoize(ttl=0.02)
    def compute(n: int) -> dict:
        return {"n": n, "pad": "x" * 64}

    def step(i: int) -> None:
        compute(i)  # every call is a new argument, so every entry is new

    try:
        _assert_flat(_measure_rss_growth(step, 4000))
    finally:
        cache.close()


def test_memoize_unique_args_with_flush_is_flat() -> None:
    cache = Cache(serializer="json")

    @cache.memoize(ttl=60)
    def compute(n: int) -> dict:
        return {"n": n}

    def step(i: int) -> None:
        compute(i)
        if i % 200 == 199:
            cache.flush()

    try:
        _assert_flat(_measure_rss_growth(step, 4000))
    finally:
        cache.close()


# ---- Server and Client connections ------------------------------------------------


def test_server_start_stop_cycles_are_flat() -> None:
    cache = Cache()

    def step(_i: int) -> None:
        srv = Server(cache, host="127.0.0.1", port=0)
        srv.start()
        srv.stop()

    try:
        _assert_flat(_measure_rss_growth(step, 100))
    finally:
        cache.close()


def test_client_connect_close_200_returns_fds_to_baseline(server: Server) -> None:
    gc.collect()
    baseline = _fd_count()
    for _ in range(200):
        c = Client(host="127.0.0.1", port=server.port, timeout=5)
        assert c.ping() is True
        c.close()
    gc.collect()
    assert _fd_count() <= baseline + 1, f"fd count {baseline} -> {_fd_count()}"


def test_client_connect_close_rss_is_flat(server: Server) -> None:
    def step(_i: int) -> None:
        with Client(host="127.0.0.1", port=server.port, timeout=5) as c:
            c.set("k", "v")
            c.get("k")

    _assert_flat(_measure_rss_growth(step, 400))


def test_client_pool_exhaustion_timeouts_leak_no_sockets() -> None:
    # A listener that never answers, so every command times out holding a slot.
    import socket

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(64)
    port = listener.getsockname()[1]
    try:
        gc.collect()
        baseline = _fd_count()
        c = Client(host="127.0.0.1", port=port, timeout=0.05, pool_size=2)
        for _ in range(60):
            with pytest.raises(ConnectionError):
                c.get("k")
        c.close()
        gc.collect()
        assert _fd_count() <= baseline + 1, f"fd count {baseline} -> {_fd_count()}"
    finally:
        listener.close()


def test_client_pool_returns_slots_after_failures(server: Server) -> None:
    c = Client(host="127.0.0.1", port=server.port, timeout=5, pool_size=2)
    try:
        for i in range(300):
            c.set(f"k{i % 10}", "v")
        assert c._slots.acquire(timeout=1)  # both slots are free
        c._slots.release()
    finally:
        c.close()


def test_client_threads_fd_flat(server: Server) -> None:
    c = Client(host="127.0.0.1", port=server.port, timeout=5, pool_size=8)
    try:
        def hammer() -> None:
            for _ in range(200):
                c.incr("n")

        for _ in range(3):  # warm the pool
            threads = [threading.Thread(target=hammer) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        gc.collect()
        baseline = _fd_count()
        for _ in range(5):
            threads = [threading.Thread(target=hammer) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        gc.collect()
        assert _fd_count() <= baseline + 1
        assert len(c._idle) <= 8
    finally:
        c.close()


def test_threads_release_gil_without_leak() -> None:
    cache = Cache()

    def worker() -> None:
        for i in range(200):
            cache.set(f"t{i % 50}", "x" * 32)
            cache.get(f"t{i % 50}")

    def step(_i: int) -> None:
        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    try:
        _assert_flat(_measure_rss_growth(step, 40))
    finally:
        cache.close()


def test_async_memoize_loop_is_flat() -> None:
    cache = Cache(serializer="json")

    @cache.memoize(ttl=0.02)
    async def compute(n: int) -> dict:
        await asyncio.sleep(0)
        return {"n": n, "pad": "x" * 64}

    def step(i: int) -> None:
        asyncio.run(compute(i))

    try:
        _assert_flat(_measure_rss_growth(step, 1000))
    finally:
        cache.close()
