"""``python -m pydirs`` with ``--dir``, ``--requirepass`` and the exit codes (PLAN 4e).

Each test runs the real CLI as a subprocess, so signals, exit codes and the banner are
the ones an operator sees.
"""

from __future__ import annotations

import os
import pathlib
import re
import signal
import subprocess
import sys
import time
from collections.abc import Iterator
from typing import List, Optional, Tuple

import pytest

from pydirs import AuthenticationError, Cache, Client

BANNER = re.compile(r"listening on (\S+):(\d+) \((.*)\)")


def _env(extra: Optional[dict] = None) -> dict:
    env = dict(os.environ)
    env.pop("PYDIRS_PASSWORD", None)
    if extra:
        env.update(extra)
    return env


def _spawn(*args: str, env: Optional[dict] = None) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "pydirs", *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=_env(env),
    )


def _banner(proc: subprocess.Popen, timeout: float = 15.0) -> Tuple[str, int, str]:
    """Reads the startup banner and returns (host, port, the parenthesised details)."""
    assert proc.stdout is not None
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline()
        if line:
            match = BANNER.search(line)
            if match:
                return match.group(1), int(match.group(2)), match.group(3)
        elif proc.poll() is not None:
            break
    err = proc.stderr.read() if proc.stderr else ""
    proc.kill()
    raise AssertionError(f"no banner; exit={proc.poll()} stderr={err!r}")


def _stop(proc: subprocess.Popen, sig: int = signal.SIGTERM) -> int:
    proc.send_signal(sig)
    return proc.wait(timeout=15)


@pytest.fixture
def procs() -> Iterator[List[subprocess.Popen]]:
    started: List[subprocess.Popen] = []
    try:
        yield started
    finally:
        for p in started:
            if p.poll() is None:
                p.kill()
            p.wait(timeout=15)


# ---- persistence through the CLI --------------------------------------------------


def test_sigterm_snapshots_and_restart_restores_keys(tmp_path: pathlib.Path, procs: List) -> None:
    data = str(tmp_path / "cli")
    first = _spawn("--port", "0", "--dir", data)
    procs.append(first)
    _, port, _ = _banner(first)
    with Client(host="127.0.0.1", port=port, timeout=5) as c:
        c.set("cli-key", "survives")
        c.hset("cli-hash", "f", "1")
    assert _stop(first) == 0

    second = _spawn("--port", "0", "--dir", data)
    procs.append(second)
    _, port, _ = _banner(second)
    with Client(host="127.0.0.1", port=port, timeout=5) as c:
        assert c.get("cli-key") == b"survives"
        assert c.hget("cli-hash", "f") == b"1"
    assert _stop(second) == 0


def test_sigint_also_snapshots(tmp_path: pathlib.Path, procs: List) -> None:
    data = str(tmp_path / "sigint")
    first = _spawn("--port", "0", "--dir", data)
    procs.append(first)
    _, port, _ = _banner(first)
    with Client(host="127.0.0.1", port=port, timeout=5) as c:
        c.set("k", "int")
    assert _stop(first, signal.SIGINT) == 0
    assert (tmp_path / "sigint" / "snapshot.tcs").exists()


def test_banner_shows_persistence_directory(tmp_path: pathlib.Path, procs: List) -> None:
    data = str(tmp_path / "banner")
    proc = _spawn("--port", "0", "--dir", data)
    procs.append(proc)
    _, port, details = _banner(proc)
    assert f"persist={data}" in details
    assert port > 0


def test_banner_shows_persistence_off_by_default(procs: List) -> None:
    proc = _spawn("--port", "0")
    procs.append(proc)
    _, _, details = _banner(proc)
    assert "persist=off" in details
    assert _stop(proc) == 0


def test_banner_shows_auth_state(procs: List) -> None:
    proc = _spawn("--port", "0", "--requirepass", "pw")
    procs.append(proc)
    _, _, details = _banner(proc)
    assert "auth=on" in details
    assert "pw" not in details
    assert _stop(proc) == 0


def test_locked_dir_exits_one_with_a_clear_message(tmp_path: pathlib.Path, procs: List) -> None:
    data = str(tmp_path / "locked")
    holder = _spawn("--port", "0", "--dir", data)
    procs.append(holder)
    _banner(holder)
    other = _spawn("--port", "0", "--dir", data)
    code = other.wait(timeout=15)
    err = other.stderr.read() if other.stderr else ""
    assert code == 1
    assert "in use by another live process" in err
    assert _stop(holder) == 0


def test_corrupt_snapshot_exits_one_with_a_clear_message(tmp_path: pathlib.Path) -> None:
    data = tmp_path / "corrupt"
    c = Cache(persist_dir=str(data))
    c.set("a", "1" * 64)
    c.close()
    snap = data / "snapshot.tcs"
    raw = bytearray(snap.read_bytes())
    raw[len(raw) // 2] ^= 0xFF
    snap.write_bytes(bytes(raw))
    proc = _spawn("--port", "0", "--dir", str(data))
    code = proc.wait(timeout=15)
    err = proc.stderr.read() if proc.stderr else ""
    assert code == 1
    assert "cannot load data" in err
    assert "move the damaged files aside" in err


def test_fsync_and_snapshot_interval_flags_are_accepted(tmp_path: pathlib.Path, procs: List) -> None:
    proc = _spawn("--port", "0", "--dir", str(tmp_path / "f"), "--fsync", "always", "--snapshot-interval", "0")
    procs.append(proc)
    _banner(proc)
    assert _stop(proc) == 0


def test_bad_fsync_flag_is_a_usage_error() -> None:
    proc = _spawn("--port", "0", "--fsync", "sometimes")
    assert proc.wait(timeout=15) == 2


def test_negative_snapshot_interval_is_a_usage_error() -> None:
    proc = _spawn("--port", "0", "--snapshot-interval", "-5")
    assert proc.wait(timeout=15) == 2


# ---- passwords through the CLI -----------------------------------------------------


def test_requirepass_flag_is_enforced(procs: List) -> None:
    proc = _spawn("--port", "0", "--requirepass", "flagpw")
    procs.append(proc)
    _, port, _ = _banner(proc)
    with Client(host="127.0.0.1", port=port, timeout=5) as anon:
        with pytest.raises(AuthenticationError):
            anon.get("k")
    with Client(host="127.0.0.1", port=port, password="flagpw", timeout=5) as ok:
        ok.set("k", "v")
        assert ok.get("k") == b"v"
    assert _stop(proc) == 0


def test_password_from_env_is_enforced(procs: List) -> None:
    proc = _spawn("--port", "0", env={"PYDIRS_PASSWORD": "envpw"})
    procs.append(proc)
    _, port, _ = _banner(proc)
    with Client(host="127.0.0.1", port=port, timeout=5) as anon:
        with pytest.raises(AuthenticationError):
            anon.ping()
    with Client(host="127.0.0.1", port=port, password="envpw", timeout=5) as ok:
        assert ok.ping() is True
    assert _stop(proc) == 0


def test_flag_password_wins_over_env(procs: List) -> None:
    proc = _spawn("--port", "0", "--requirepass", "flagwins", env={"PYDIRS_PASSWORD": "envloses"})
    procs.append(proc)
    _, port, _ = _banner(proc)
    with Client(host="127.0.0.1", port=port, password="envloses", timeout=5) as c:
        with pytest.raises(AuthenticationError):
            c.ping()
    with Client(host="127.0.0.1", port=port, password="flagwins", timeout=5) as c:
        assert c.ping() is True
    assert _stop(proc) == 0


def test_empty_password_env_is_refused() -> None:
    proc = _spawn("--port", "0", env={"PYDIRS_PASSWORD": ""})
    assert proc.wait(timeout=15) == 2


def test_non_loopback_without_password_is_still_refused() -> None:
    proc = _spawn("--host", "0.0.0.0", "--port", "0")
    assert proc.wait(timeout=15) == 2
    err = proc.stderr.read() if proc.stderr else ""
    assert "refusing to bind" in err


def test_non_loopback_with_password_is_allowed(procs: List) -> None:
    proc = _spawn("--host", "0.0.0.0", "--port", "0", "--requirepass", "pw")
    procs.append(proc)
    host, port, _ = _banner(proc)
    assert host == "0.0.0.0"
    assert _stop(proc) == 0


def test_allow_insecure_still_permits_public_bind(procs: List) -> None:
    proc = _spawn("--host", "0.0.0.0", "--port", "0", "--allow-insecure")
    procs.append(proc)
    _banner(proc)
    assert _stop(proc) == 0


def test_info_and_save_work_against_cli_server(tmp_path: pathlib.Path, procs: List) -> None:
    proc = _spawn("--port", "0", "--dir", str(tmp_path / "admin"), "--requirepass", "adm")
    procs.append(proc)
    _, port, _ = _banner(proc)
    with Client(host="127.0.0.1", port=port, password="adm", timeout=5) as c:
        c.set("k", "v")
        c.save()
        assert c.lastsave() > 0
        assert c.info()["persistence_enabled"] == 1
    assert _stop(proc) == 0
