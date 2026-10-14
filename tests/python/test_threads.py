"""Concurrency: GIL release and atomic increments across threads."""

from __future__ import annotations

import threading

from tinycache import Cache


def test_eight_threads_1000_incr_each_is_exact() -> None:
    with Cache(shards=8) as c:
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                for _ in range(1000):
                    c.incr("counter")
            except BaseException as exc:  # pragma: no cover - reported below
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert c.get("counter") == b"8000"


def test_concurrent_writers_on_distinct_keys() -> None:
    with Cache() as c:

        def worker(n: int) -> None:
            for i in range(200):
                c.set(f"t{n}:{i}", str(i))

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(c) == 1600
