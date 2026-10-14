"""The ``tinycache-server`` / ``python -m tinycache`` command, run as a subprocess."""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from typing import IO, Optional

import pytest

from tinycache import Client
from tinycache._cli import build_parser, parse_size

BANNER = re.compile(r"listening on (\S+):(\d+)")


def _spawn(*args: str, env: Optional[dict] = None) -> subprocess.Popen:
    full_env = dict(os.environ)
    full_env.pop("TINYCACHE_PASSWORD", None)
    if env:
        full_env.update(env)
    return subprocess.Popen(
        [sys.executable, "-m", "tinycache", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=full_env,
    )


def _read_banner(proc: subprocess.Popen, timeout: float = 10.0) -> tuple[str, int]:
    """Reads the startup line from stdout and returns (host, bound_port)."""
    deadline = time.monotonic() + timeout
    stdout: IO[str] = proc.stdout  # type: ignore[assignment]
    while time.monotonic() < deadline:
        line = stdout.readline()
        if line:
            match = BANNER.search(line)
            if match:
                return match.group(1), int(match.group(2))
        elif proc.poll() is not None:
            break
    proc.kill()
    raise AssertionError(f"no banner; stderr={proc.stderr.read() if proc.stderr else ''!r}")


@pytest.fixture
def running() -> Iterator[subprocess.Popen]:
    proc = _spawn("--port", "0")
    try:
        yield proc
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


def test_banner_reports_the_bound_port(running: subprocess.Popen) -> None:
    host, port = _read_banner(running)
    assert host == "127.0.0.1"
    assert port > 0


def test_client_round_trip_against_cli_server(running: subprocess.Popen) -> None:
    _, port = _read_banner(running)
    with Client(host="127.0.0.1", port=port, timeout=5) as c:
        assert c.ping() is True
        c.set("cli", "works")
        assert c.get("cli") == b"works"
        assert c.incr("n", 3) == 3


def test_sigterm_exits_zero_and_stops_serving(running: subprocess.Popen) -> None:
    _, port = _read_banner(running)
    running.send_signal(signal.SIGTERM)
    assert running.wait(timeout=10) == 0
    with pytest.raises(ConnectionError):
        Client(host="127.0.0.1", port=port, timeout=1).get("k")


def test_sigint_exits_zero(running: subprocess.Popen) -> None:
    _read_banner(running)
    running.send_signal(signal.SIGINT)
    assert running.wait(timeout=10) == 0


def test_insecure_bind_refused_without_password() -> None:
    proc = _spawn("--host", "0.0.0.0", "--port", "0")
    try:
        code = proc.wait(timeout=10)
        err = proc.stderr.read() if proc.stderr else ""
    finally:
        if proc.poll() is None:
            proc.kill()
    assert code == 2
    assert "refusing to bind" in err
    assert "--allow-insecure" in err


def test_allow_insecure_permits_public_bind_for_the_process() -> None:
    proc = _spawn("--host", "0.0.0.0", "--port", "0", "--allow-insecure")
    try:
        host, port = _read_banner(proc)
        assert host == "0.0.0.0"
        assert port > 0
    finally:
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=10) == 0


def test_requirepass_flag_is_not_supported_yet() -> None:
    proc = _spawn("--port", "0", "--requirepass", "pw")
    code = proc.wait(timeout=10)
    err = proc.stderr.read() if proc.stderr else ""
    assert code == 2
    assert "not supported by this build yet" in err


def test_password_env_is_not_supported_yet() -> None:
    proc = _spawn("--port", "0", env={"TINYCACHE_PASSWORD": "pw"})
    code = proc.wait(timeout=10)
    err = proc.stderr.read() if proc.stderr else ""
    assert code == 2
    assert "not supported by this build yet" in err


@pytest.mark.parametrize("flag", [["--dir", "/tmp/x"], ["--fsync", "always"], ["--snapshot-interval", "60"]])
def test_persistence_flags_are_not_supported_yet(flag: list[str]) -> None:
    proc = _spawn("--port", "0", *flag)
    code = proc.wait(timeout=10)
    err = proc.stderr.read() if proc.stderr else ""
    assert code == 2
    assert "not supported by this build yet" in err


def test_bad_port_is_a_usage_error() -> None:
    proc = _spawn("--port", "70000")
    assert proc.wait(timeout=10) == 2


def test_version_flag() -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "tinycache", "--version"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode == 0
    assert "tinycache" in proc.stdout


@pytest.mark.parametrize(
    ("text", "expected"),
    [("1048576", 1048576), ("64mb", 64 << 20), ("2gb", 2 << 30), ("512k", 512 << 10), ("0", 0), ("1G", 1 << 30)],
)
def test_parse_size_accepts_bytes_and_suffixes(text: str, expected: int) -> None:
    assert parse_size(text) == expected


@pytest.mark.parametrize("text", ["abc", "1.5gb", "-1", ""])
def test_parse_size_rejects_bad_values(text: str) -> None:
    import argparse

    with pytest.raises(argparse.ArgumentTypeError):
        parse_size(text)


def test_parser_defaults_match_the_plan() -> None:
    args = build_parser().parse_args([])
    assert args.host == "127.0.0.1"
    assert args.port == 6380
    assert args.max_memory == 0
    assert args.policy == "noeviction"
    assert args.shards == 64
    assert args.requirepass is None
    assert args.allow_insecure is False


def test_max_memory_flag_is_applied_through_parser() -> None:
    args = build_parser().parse_args(["--max-memory", "64mb"])
    assert args.max_memory == 64 << 20
