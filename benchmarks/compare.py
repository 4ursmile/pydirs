"""Benchmark tinycache against Redis.

Three comparisons, because "embedded vs network" is not apples to apples:

1. Python embedded: tinycache.Cache (in-process) vs redis-py -> redis-server (TCP, localhost).
2. Python dict baseline for reference.
3. Wire protocol: redis-benchmark against tinycache.Server vs redis-server (same RESP clients).

Usage: python benchmarks/compare.py [--n 200000] [--redis-port 6390] [--tc-port 6391]
"""

from __future__ import annotations

import argparse
import os
import resource
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path

import tinycache


def timeit(fn, n: int) -> float:
    t = time.perf_counter()
    fn(n)
    dt = time.perf_counter() - t
    return n / dt


def run_ops(set_fn, get_fn):
    def sets(n):
        for i in range(n):
            set_fn(f"key:{i % 10000}", "x" * 64)

    def gets(n):
        for i in range(n):
            get_fn(f"key:{i % 10000}")

    return sets, gets


def threaded(fn, n: int, threads: int) -> float:
    per = n // threads
    ts = [threading.Thread(target=fn, args=(per,)) for _ in range(threads)]
    t = time.perf_counter()
    for th in ts:
        th.start()
    for th in ts:
        th.join()
    return (per * threads) / (time.perf_counter() - t)


def start_redis(port: int) -> subprocess.Popen:
    p = subprocess.Popen(
        ["redis-server", "--port", str(port), "--save", "", "--appendonly", "no", "--daemonize", "no"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    time.sleep(0.5)
    return p


def rss_kb(pid: int) -> int:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True)
    return int(out.stdout.strip() or 0)


def redis_benchmark(port: int, n: int, pipeline: int = 1) -> dict[str, float]:
    out = subprocess.run(
        ["redis-benchmark", "-p", str(port), "-n", str(n), "-c", "50", "-P", str(pipeline),
         "-d", "64", "-t", "set,get,incr", "-q", "--csv"],
        capture_output=True, text=True,
    ).stdout
    res = {}
    for line in out.splitlines():
        parts = [p.strip('"') for p in line.split(",")]
        if len(parts) >= 2:
            try:
                res[parts[0]] = float(parts[1])
            except ValueError:
                pass
    return res


def fmt(x: float) -> str:
    return f"{x:>12,.0f}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200_000)
    ap.add_argument("--redis-port", type=int, default=6390)
    ap.add_argument("--tc-port", type=int, default=6391)
    args = ap.parse_args()
    n = args.n

    rows: list[tuple[str, float, float, float]] = []

    # ---- 1. Python embedded ------------------------------------------------
    c = tinycache.Cache()
    d: dict = {}
    tc_set, tc_get = run_ops(c.set, c.get)
    d_set, d_get = run_ops(d.__setitem__, d.get)
    r_set = r_get = None
    redis_proc = None
    try:
        import redis  # type: ignore

        redis_proc = start_redis(args.redis_port)
        rc = redis.Redis(port=args.redis_port)
        rc.ping()
        r_set, r_get = run_ops(rc.set, rc.get)
    except Exception as e:  # noqa: BLE001
        print(f"redis-py unavailable ({e}); skipping redis-py rows", file=sys.stderr)

    print(f"\n== Python client, {n:,} ops, 64-byte values, 10k keys (ops/sec) ==")
    print(f"{'':<34}{'SET':>12}{'GET':>12}")
    for name, s, g in (
        ("python dict", d_set, d_get),
        ("tinycache.Cache (in-process)", tc_set, tc_get),
        ("redis-py -> redis-server (TCP)", r_set, r_get),
    ):
        if s is None:
            continue
        nn = n if "redis-py" not in name else min(n, 50_000)
        print(f"{name:<34}{fmt(timeit(s, nn))}{fmt(timeit(g, nn))}")

    print("\n== 8 Python threads, GET (ops/sec; GIL released in tinycache) ==")
    print(f"{'tinycache.Cache':<34}{fmt(threaded(tc_get, n, 8))}")

    # ---- 2. Wire protocol (redis-benchmark) ---------------------------------
    if shutil.which("redis-benchmark") and redis_proc is not None:
        srv = tinycache.Server(c, port=args.tc_port)
        srv.start()
        try:
            for pipe in (1, 16):
                tcb = redis_benchmark(args.tc_port, n, pipe)
                rdb = redis_benchmark(args.redis_port, n, pipe)
                print(f"\n== redis-benchmark, 50 clients, pipeline={pipe} (req/sec) ==")
                print(f"{'':<14}{'tinycache':>14}{'redis':>14}")
                for k in ("SET", "GET", "INCR"):
                    print(f"{k:<14}{fmt(tcb.get(k, 0)):>14}{fmt(rdb.get(k, 0)):>14}")
        finally:
            srv.stop()

        # ---- 3. Memory: 1M keys x 64B ----------------------------------------
        print("\n== Memory, 1,000,000 keys x 64-byte values ==")
        rc.flushall()
        base = rss_kb(redis_proc.pid)
        pipe = rc.pipeline(transaction=False)
        for i in range(1_000_000):
            pipe.set(f"key:{i}", "x" * 64)
            if i % 10_000 == 0:
                pipe.execute()
        pipe.execute()
        redis_mb = (rss_kb(redis_proc.pid) - base) / 1024
        c2 = tinycache.Cache()
        base_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        before = rss_kb(os.getpid())
        for i in range(1_000_000):
            c2.set(f"key:{i}", "x" * 64)
        tc_mb = (rss_kb(os.getpid()) - before) / 1024
        print(f"{'redis-server RSS growth':<34}{redis_mb:>10.1f} MB")
        print(f"{'tinycache RSS growth':<34}{tc_mb:>10.1f} MB (core used_memory estimate: {c2.used_memory/1048576:.1f} MB)")
        _ = base_rss

    if redis_proc is not None:
        redis_proc.terminate()

    # ---- 4. Artifact size ----------------------------------------------------
    print("\n== Size ==")
    so = list(Path(tinycache.__file__).parent.glob("_tinycache*"))
    for f in so:
        print(f"{f.name:<34}{f.stat().st_size / 1024:>10.0f} KiB (extension module)")
    redis_bin = shutil.which("redis-server")
    if redis_bin:
        print(f"{'redis-server binary':<34}{Path(redis_bin).stat().st_size / 1024:>10.0f} KiB")


if __name__ == "__main__":
    main()
