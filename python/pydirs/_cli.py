"""Command-line server: ``pydirs-server`` or ``python -m pydirs``.

Starts one shared cache behind the RESP2 server so that many processes can use
``pydirs.Client``. Prints a one-line banner with the bound port (use
``--port 0`` for a free port), runs until SIGINT or SIGTERM, then stops the server
and closes the cache, which writes a final snapshot when ``--dir`` is set.

Exit codes: 0 for a clean stop; 1 when the data directory is locked by another
live process or its snapshot is corrupt; 2 for usage or configuration errors.
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import sys
import threading
from typing import Any, Callable, List, Optional, Sequence, TextIO

from . import __version__
from ._pydirs import CacheLockedError, CorruptDataError

__all__ = ["main"]

PASSWORD_ENV = "PYDIRS_PASSWORD"
EXIT_RUNTIME = 1
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_SIZE_RE = re.compile(r"^\s*(\d+)\s*([kmgt]?)b?\s*$", re.IGNORECASE)
_SIZE_FACTOR = {"": 1, "k": 1 << 10, "m": 1 << 20, "g": 1 << 30, "t": 1 << 40}


class UsageError(Exception):
    """A configuration problem that makes the server refuse to start (exit code 2)."""


class DataError(Exception):
    """The data directory cannot be used: locked by another process or corrupt (exit code 1)."""


def parse_size(text: str) -> int:
    """Parses a byte count such as ``1048576``, ``64mb``, ``2gb`` or ``512k``."""
    match = _SIZE_RE.match(text)
    if not match:
        raise argparse.ArgumentTypeError(
            f"invalid size {text!r}; use bytes or a suffix such as 64mb or 2gb"
        )
    return int(match.group(1)) * _SIZE_FACTOR[match.group(2).lower()]


def _positive_port(text: str) -> int:
    try:
        value = int(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid port {text!r}") from exc
    if not 0 <= value <= 65535:
        raise argparse.ArgumentTypeError("port must be between 0 and 65535")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pydirs-server",
        description="Run a shared pydirs server (RESP2, Redis-compatible commands).",
    )
    parser.add_argument("--version", action="version", version=f"pydirs {__version__}")
    parser.add_argument("--host", default="127.0.0.1", help="address to bind (default: %(default)s)")
    parser.add_argument("--port", type=_positive_port, default=6380,
                        help="TCP port; 0 picks a free port (default: %(default)s)")
    parser.add_argument("--max-memory", type=parse_size, default=0, metavar="SIZE",
                        help="memory limit, bytes or a suffix like 64mb or 2gb; 0 means unlimited")
    parser.add_argument("--policy", default="noeviction",
                        choices=["noeviction", "allkeys-lru", "volatile-lru"],
                        help="eviction policy when the limit is reached (default: %(default)s)")
    parser.add_argument("--shards", type=int, default=64,
                        help="number of shards, a power of two (default: %(default)s)")
    parser.add_argument("--requirepass", metavar="PASS", default=None,
                        help="password clients must send with AUTH (or set $%s)" % PASSWORD_ENV)
    parser.add_argument("--allow-insecure", action="store_true",
                        help="allow binding a non-loopback host without a password")
    parser.add_argument("--dir", metavar="PATH", default=None,
                        help="directory for snapshots and the append-only log; omit to run in memory only")
    parser.add_argument("--fsync", choices=["everysec", "always", "never"], default="everysec",
                        help="when to fsync the log, with --dir (default: %(default)s)")
    parser.add_argument("--snapshot-interval", type=_non_negative_int, default=300, metavar="SECONDS",
                        help="seconds between background snapshots, 0 disables them (default: %(default)s)")
    return parser


def _non_negative_int(text: str) -> int:
    try:
        value = int(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer {text!r}") from exc
    if value < 0:
        raise argparse.ArgumentTypeError("must be 0 or more")
    return value


def check_bind_safety(host: str, password: Optional[str], allow_insecure: bool) -> None:
    """Refuses a non-loopback bind without a password unless explicitly allowed."""
    if host in LOOPBACK_HOSTS or host.startswith("127."):
        return
    if password is None and not allow_insecure:
        raise UsageError(
            f"refusing to bind {host!r} without a password: set --requirepass "
            "(or PYDIRS_PASSWORD), or pass --allow-insecure to accept the risk"
        )


# ---- running ----------------------------------------------------------------


def _install_signal_handlers(stop: threading.Event) -> Callable[[], None]:
    """Routes SIGINT and SIGTERM to ``stop``. Returns a function that restores the handlers."""
    previous: List[Any] = []

    def handler(_signum: int, _frame: Any) -> None:
        stop.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        previous.append((signum, signal.signal(signum, handler)))

    def restore() -> None:
        for signum, old in previous:
            signal.signal(signum, old)

    return restore


def run(args: argparse.Namespace, out: TextIO = sys.stdout, stop: Optional[threading.Event] = None) -> int:
    """Starts the server with validated ``args``, waits for a stop signal, shuts down cleanly.

    Raises ``UsageError`` for configuration problems and ``DataError`` for a locked or
    corrupt data directory. Other ``OSError``s (such as a port in use) propagate.
    """
    password = args.requirepass if args.requirepass is not None else os.environ.get(PASSWORD_ENV)
    if password == "":
        raise UsageError("the password must not be empty")
    check_bind_safety(args.host, password, args.allow_insecure)

    from ._cache import Cache
    from ._server import Server

    persist_dir = args.dir
    try:
        cache = Cache(
            max_memory=args.max_memory,
            policy=args.policy,
            shards=args.shards,
            persist_dir=persist_dir,
            fsync=args.fsync,
            snapshot_interval=args.snapshot_interval,
        )
    except CacheLockedError as exc:
        raise DataError(f"{exc}; stop the other server or choose another --dir") from exc
    except CorruptDataError as exc:
        raise DataError(f"{exc}; move the damaged files aside to start empty") from exc

    stop = stop or threading.Event()
    restore = _install_signal_handlers(stop) if threading.current_thread() is threading.main_thread() else None
    server = Server(cache, host=args.host, port=args.port, password=password)
    try:
        server.start()
        persistence = f"persist={persist_dir}" if persist_dir is not None else "persist=off"
        print(
            f"pydirs {__version__} listening on {args.host}:{server.port} "
            f"(max_memory={args.max_memory or 'unlimited'}, policy={args.policy}, "
            f"shards={args.shards}, {persistence}, auth={'on' if password else 'off'})",
            file=out,
            flush=True,
        )
        while not stop.wait(0.2):
            pass
    finally:
        server.stop()
        cache.close()
        if restore is not None:
            restore()
    print("pydirs stopped", file=out, flush=True)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point for ``pydirs-server`` and ``python -m pydirs``. Returns the exit code."""
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        return run(args)
    except UsageError as exc:
        print(f"pydirs-server: error: {exc}", file=sys.stderr, flush=True)
        return 2
    except DataError as exc:
        print(f"pydirs-server: error: {exc}", file=sys.stderr, flush=True)
        return EXIT_RUNTIME
    except OSError as exc:
        print(f"pydirs-server: error: cannot listen on {args.host}:{args.port}: {exc}",
              file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
