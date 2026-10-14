"""Server passwords (AUTH), the Client admin commands (SAVE, BGSAVE, LASTSAVE, INFO) and
the ``Server(password=)`` validation (PLAN 4e)."""

from __future__ import annotations

import pathlib
import socket
import time
from collections.abc import Iterator

import pytest

from tinycache import AuthenticationError, Cache, Client, ResponseError, Server

PASSWORD = "s3cret-pass"


def _encode(*args: str) -> bytes:
    parts = [f"*{len(args)}\r\n".encode()]
    for a in args:
        raw = a.encode()
        parts.append(f"${len(raw)}\r\n".encode() + raw + b"\r\n")
    return b"".join(parts)


def _read_line(sock: socket.socket) -> bytes:
    buf = b""
    while not buf.endswith(b"\r\n"):
        chunk = sock.recv(1)
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
    return buf[:-2]


def _raw_reply(sock: socket.socket) -> bytes:
    return _read_line(sock)


@pytest.fixture
def secured() -> Iterator[Server]:
    cache = Cache()
    srv = Server(cache, host="127.0.0.1", port=0, password=PASSWORD)
    srv.start()
    try:
        yield srv
    finally:
        srv.stop()
        cache.close()


@pytest.fixture
def persistent_cache(tmp_path: pathlib.Path) -> Iterator[Cache]:
    c = Cache(persist_dir=str(tmp_path / "data"))
    try:
        yield c
    finally:
        c.close()


# ---- Server password validation ------------------------------------------------


@pytest.mark.parametrize("bad", ["", b"x", 123])
def test_server_password_must_be_a_non_empty_str(bad: object) -> None:
    cache = Cache()
    try:
        with pytest.raises((TypeError, ValueError)):
            Server(cache, port=0, password=bad)  # type: ignore[arg-type]
    finally:
        cache.close()


def test_server_without_password_keeps_working() -> None:
    cache = Cache()
    srv = Server(cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            assert c.ping() is True
    finally:
        srv.stop()
        cache.close()


# ---- AUTH behaviour -------------------------------------------------------------


def test_raw_command_without_auth_gets_noauth(secured: Server) -> None:
    with socket.create_connection(("127.0.0.1", secured.port), timeout=5) as s:
        s.sendall(_encode("GET", "k"))
        assert _raw_reply(s).startswith(b"-NOAUTH")


def test_raw_ping_without_auth_is_refused(secured: Server) -> None:
    with socket.create_connection(("127.0.0.1", secured.port), timeout=5) as s:
        s.sendall(_encode("PING"))
        assert _raw_reply(s).startswith(b"-NOAUTH")


def test_raw_auth_with_correct_password_unlocks_commands(secured: Server) -> None:
    with socket.create_connection(("127.0.0.1", secured.port), timeout=5) as s:
        s.sendall(_encode("AUTH", PASSWORD))
        assert _raw_reply(s) == b"+OK"
        s.sendall(_encode("SET", "k", "v"))
        assert _raw_reply(s) == b"+OK"


def test_raw_auth_with_wrong_password_is_wrongpass(secured: Server) -> None:
    with socket.create_connection(("127.0.0.1", secured.port), timeout=5) as s:
        s.sendall(_encode("AUTH", "nope"))
        assert _raw_reply(s).startswith(b"-WRONGPASS")
        s.sendall(_encode("GET", "k"))
        assert _raw_reply(s).startswith(b"-NOAUTH")


def test_raw_quit_is_allowed_without_auth(secured: Server) -> None:
    with socket.create_connection(("127.0.0.1", secured.port), timeout=5) as s:
        s.sendall(_encode("QUIT"))
        assert _raw_reply(s) == b"+OK"


def test_auth_is_per_connection(secured: Server) -> None:
    with socket.create_connection(("127.0.0.1", secured.port), timeout=5) as authed:
        authed.sendall(_encode("AUTH", PASSWORD))
        assert _raw_reply(authed) == b"+OK"
    with socket.create_connection(("127.0.0.1", secured.port), timeout=5) as fresh:
        fresh.sendall(_encode("GET", "k"))
        assert _raw_reply(fresh).startswith(b"-NOAUTH")


def test_client_without_password_is_refused(secured: Server) -> None:
    with Client(host="127.0.0.1", port=secured.port, timeout=5) as c:
        with pytest.raises(AuthenticationError):
            c.get("k")


def test_client_with_wrong_password_is_refused(secured: Server) -> None:
    c = Client(host="127.0.0.1", port=secured.port, password="wrong", timeout=5)
    try:
        with pytest.raises(AuthenticationError, match="WRONGPASS"):
            c.set("k", "v")
    finally:
        c.close()


def test_client_with_correct_password_works(secured: Server) -> None:
    with Client(host="127.0.0.1", port=secured.port, password=PASSWORD, timeout=5) as c:
        c.set("k", "v")
        assert c.get("k") == b"v"
        assert c.incr("n", 2) == 2


def test_client_password_as_bytes_works(secured: Server) -> None:
    with Client(host="127.0.0.1", port=secured.port, password=PASSWORD.encode(), timeout=5) as c:
        assert c.ping() is True


def test_authentication_error_is_a_connection_error() -> None:
    assert issubclass(AuthenticationError, ConnectionError)


def test_noauth_and_wrongpass_map_to_authentication_error() -> None:
    from tinycache._client import _map_error

    assert isinstance(_map_error("NOAUTH Authentication required."), AuthenticationError)
    assert isinstance(_map_error("WRONGPASS invalid password"), AuthenticationError)
    assert isinstance(_map_error("ERR invalid password"), AuthenticationError)


def test_unknown_errors_are_still_response_errors() -> None:
    from tinycache._client import _map_error

    assert isinstance(_map_error("ERR something odd"), ResponseError)


def test_auth_command_without_configured_password_is_an_error() -> None:
    cache = Cache()
    srv = Server(cache, port=0)
    srv.start()
    try:
        with socket.create_connection(("127.0.0.1", srv.port), timeout=5) as s:
            s.sendall(_encode("AUTH", "x"))
            assert _raw_reply(s).startswith(b"-ERR")
    finally:
        srv.stop()
        cache.close()


def test_password_is_shared_by_every_client_in_a_pool(secured: Server) -> None:
    with Client(host="127.0.0.1", port=secured.port, password=PASSWORD, timeout=5, pool_size=4) as c:
        for i in range(8):
            c.set(f"k{i}", str(i))
        assert c.keys("k*")


# ---- Client admin commands -------------------------------------------------------


def test_info_returns_a_dict_with_numeric_values() -> None:
    cache = Cache()
    srv = Server(cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            info = c.info()
            assert isinstance(info, dict)
            assert isinstance(info["uptime_in_seconds"], int)
            assert isinstance(info["used_memory"], int)
            assert isinstance(info["keys"], int)
            assert info["persistence_enabled"] == 0
    finally:
        srv.stop()
        cache.close()


def test_info_drops_section_headers() -> None:
    cache = Cache()
    srv = Server(cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            info = c.info()
            assert all(not k.startswith("#") for k in info)
            assert "uptime_in_seconds" in info
    finally:
        srv.stop()
        cache.close()


def test_info_reflects_keys_and_memory() -> None:
    cache = Cache()
    srv = Server(cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            before = c.info()["used_memory"]
            c.set("a", "x" * 1000)
            info = c.info()
            assert info["keys"] == 1
            assert info["used_memory"] > before
    finally:
        srv.stop()
        cache.close()


def test_info_keeps_text_values_as_strings() -> None:
    from tinycache._client import _parse_info

    parsed = _parse_info("# Persistence\r\npersistence_enabled:1\r\nlast_error:disk full: no space\r\n")
    assert parsed == {"persistence_enabled": 1, "last_error": "disk full: no space"}


def test_info_converts_floats() -> None:
    from tinycache._client import _parse_info

    assert _parse_info("ratio:0.5\r\nname:x")["ratio"] == 0.5


def test_used_memory_via_client_equals_info(secured: Server) -> None:
    with Client(host="127.0.0.1", port=secured.port, password=PASSWORD) as c:
        assert c.used_memory == c.info()["used_memory"]


def test_save_without_persistence_is_a_value_error() -> None:
    cache = Cache()
    srv = Server(cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            with pytest.raises(ValueError, match="persistence is not enabled"):
                c.save()
    finally:
        srv.stop()
        cache.close()


def test_save_bgsave_lastsave_through_client(persistent_cache: Cache) -> None:
    srv = Server(persistent_cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            assert c.lastsave() == 0
            c.set("k", "v")
            c.save()
            assert c.lastsave() > 0
            info = c.info()
            assert info["persistence_enabled"] == 1
            assert info["snapshot_bytes"] > 0
            assert info["last_error"] == ""
    finally:
        srv.stop()


def test_bgsave_through_client_writes_a_snapshot(persistent_cache: Cache) -> None:
    srv = Server(persistent_cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            c.set("k", "v")
            c.bgsave()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline and c.lastsave() == 0:
                time.sleep(0.05)
            assert c.lastsave() > 0
    finally:
        srv.stop()


def test_bgsave_without_persistence_is_refused() -> None:
    cache = Cache()
    srv = Server(cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            with pytest.raises(ValueError, match="persistence is not enabled"):
                c.bgsave()
    finally:
        srv.stop()
        cache.close()


def test_lastsave_without_persistence_is_zero() -> None:
    cache = Cache()
    srv = Server(cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            assert c.lastsave() == 0
    finally:
        srv.stop()
        cache.close()


def test_server_restart_on_same_dir_keeps_saved_data(tmp_path: pathlib.Path) -> None:
    data = str(tmp_path / "srv")
    cache = Cache(persist_dir=data)
    srv = Server(cache, port=0)
    srv.start()
    with Client(host="127.0.0.1", port=srv.port) as c:
        c.set("persisted", "yes")
        c.save()
    srv.stop()
    cache.close()
    cache = Cache(persist_dir=data)
    srv = Server(cache, port=0)
    srv.start()
    try:
        with Client(host="127.0.0.1", port=srv.port) as c:
            assert c.get("persisted") == b"yes"
    finally:
        srv.stop()
        cache.close()
