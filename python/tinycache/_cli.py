"""Command-line server: ``tinycache-server`` or ``python -m tinycache``.

Starts one shared cache behind the RESP2 server so that many processes can use
``tinycache.Client``. Prints a one-line banner with the bound port (use
``--port 0`` for a free port), runs until SIGINT or SIGTERM, then stops the server
and closes the cache. Exit codes: 0 for a clean stop, 2 for usage or
configuration errors.

Persistence (``--dir``, ``--fsync``, ``--snapshot-interval``) and password
enforcement (``--requirepass``) are parsed now but only work once the native module
supports them. Until then, passing them exits with a clear message.
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
from ._tinycache import Cache as _NativeCache
from ._tinycache import Server as _NativeServer

__all__ = ["main"]

PASSWORD_ENV = "TINYCACHE_PASSWORD"
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_SIZE_RE = re.compile(r"^\s*(\d+)\s*([kmgt]?)b?\s*$", re.IGNORECASE)
_SIZE_FACTOR = {"": 1, "k": 1 << 10, "m": 1 << 20, "g": 1 << 30, "t": 1 << 40}


class UsageError(Exception):
    """A configuration problem that makes the server refuse to start (exit code 2)."""


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
        prog="tinycache-server",
        description="Run a shared tinycache server (RESP2, Redis-compatible commands).",
    )
    parser.add_argument("--version", action="version", version=f"tinycache {__version__}")
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
                        help="directory for snapshots and the append-only log (needs a newer build)")
    parser.add_argument("--fsync", choices=["everysec", "always", "never"], default=None,
                        help="when to fsync the log (needs a newer build)")
    parser.add_argument("--snapshot-interval", type=int, default=None, metavar="SECONDS",
                        help="seconds between background snapshots (needs a newer build)")
    return parser


# ---- feature detection -------------------------------------------------------


def supports_persistence() -> bool:
    """True if the native Cache accepts the ``persist_dir`` keyword (a probe call)."""
    try:
        probe = _NativeCache(persist_dir=None)  # type: ignore[call-arg]
    except TypeError:
        return False
    probe.close()
    return True


def supports_password() -> bool:
    """True if the native Server accepts the ``password`` keyword (a probe call)."""
    probe_cache = _NativeCache()
    try:
        _NativeServer(probe_cache, password=None)  # type: ignore[call-arg]
    except TypeError:
        return False
    finally:
        probe_cache.close()
    return True


def _check_feature_flags(args: argparse.Namespace, password: Optional[str]) -> None:
    """Rejects persistence or password options the native module cannot honour."""
    persist_flags = [
        flag
        for flag, value in (
            ("--dir", args.dir),
            ("--fsync", args.fsync),
            ("--snapshot-interval", args.snapshot_interval),
        )
        if value is not None
    ]
    if persist_flags and not supports_persistence():
        raise UsageError(
            f"{', '.join(persist_flags)}: not supported by this build yet "
            "(persistence arrives in a later release)"
        )
    if password is not None and not supports_password():
        raise UsageError(
            "--requirepass / TINYCACHE_PASSWORD: not supported by this build yet "
            "(password enforcement arrives in a later release)"
        )


def check_bind_safety(host: str, password: Optional[str], allow_insecure: bool) -> None:
    """Refuses a non-loopback bind without a password unless explicitly allowed.

    Until the server enforces passwords, a password given here cannot be checked,
    so ``--allow-insecure`` is the only way past this check on a public address.
    """
    if host in LOOPBACK_HOSTS or host.startswith("127."):
        return
    if password is None and not allow_insecure:
        raise UsageError(
            f"refusing to bind {host!r} without a password: set --requirepass "
            "(or TINYCACHE_PASSWORD), or pass --allow-insecure to accept the risk"
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
    """Starts the server with validated ``args``, waits for a stop signal, shuts down cleanly."""
    password = args.requirepass if args.requirepass is not None else os.environ.get(PASSWORD_ENV)
    _check_feature_flags(args, password)
    check_bind_safety(args.host, password, args.allow_insecure)

    from ._cache import Cache
    from ._server import Server

    stop = stop or threading.Event()
    restore = _install_signal_handlers(stop) if threading.current_thread() is threading.main_thread() else None
    cache = Cache(max_memory=args.max_memory, policy=args.policy, shards=args.shards)
    server = Server(cache, host=args.host, port=args.port)
    try:
        server.start()
        print(
            f"tinycache {__version__} listening on {args.host}:{server.port} "
            f"(max_memory={args.max_memory or 'unlimited'}, policy={args.policy}, shards={args.shards})",
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
    print("tinycache stopped", file=out, flush=True)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Entry point for ``tinycache-server`` and ``python -m tinycache``. Returns the exit code."""
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        return run(args)
    except UsageError as exc:
        print(f"tinycache-server: error: {exc}", file=sys.stderr, flush=True)
        return 2
    except OSError as exc:
        print(f"tinycache-server: error: cannot listen on {args.host}:{args.port}: {exc}",
              file=sys.stderr, flush=True)
        return 2


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
