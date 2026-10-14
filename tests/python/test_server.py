"""RESP2 server exercised over a raw socket."""

from __future__ import annotations

import contextlib
import socket
from collections.abc import Iterator

import pytest

from pydirs import Cache, Server


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


def _read_reply(sock: socket.socket) -> object:
    line = _read_line(sock)
    kind, body = line[:1], line[1:]
    if kind == b"+":
        return body.decode()
    if kind == b"-":
        return ("error", body.decode())
    if kind == b":":
        return int(body)
    if kind == b"$":
        n = int(body)
        if n < 0:
            return None
        data = b""
        while len(data) < n + 2:
            data += sock.recv(n + 2 - len(data))
        return data[:-2]
    raise AssertionError(f"unexpected reply {line!r}")


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


@contextlib.contextmanager
def _client(srv: Server) -> Iterator[socket.socket]:
    sock = socket.create_connection(("127.0.0.1", srv.port), timeout=5)
    try:
        yield sock
    finally:
        sock.close()


def test_port_zero_gets_an_os_port(server: Server) -> None:
    assert server.port > 0
    assert server.running is True


def test_ping(server: Server) -> None:
    with _client(server) as s:
        s.sendall(_encode("PING"))
        assert _read_reply(s) == "PONG"


def test_set_then_get(server: Server) -> None:
    with _client(server) as s:
        s.sendall(_encode("SET", "k", "hello"))
        assert _read_reply(s) == "OK"
        s.sendall(_encode("GET", "k"))
        assert _read_reply(s) == b"hello"


def test_get_missing_is_null(server: Server) -> None:
    with _client(server) as s:
        s.sendall(_encode("GET", "nope"))
        assert _read_reply(s) is None


def test_server_shares_data_with_cache() -> None:
    with Cache() as cache:
        cache.set("from_python", "1")
        with Server(cache, port=0) as srv:
            with _client(srv) as s:
                s.sendall(_encode("GET", "from_python"))
                assert _read_reply(s) == b"1"
                s.sendall(_encode("SET", "from_server", "2"))
                assert _read_reply(s) == "OK"
            assert cache.get("from_server") == b"2"


def test_server_incr_on_text_is_error(server: Server) -> None:
    with _client(server) as s:
        s.sendall(_encode("SET", "k", "abc"))
        assert _read_reply(s) == "OK"
        s.sendall(_encode("INCR", "k"))
        kind = _read_reply(s)
        assert isinstance(kind, tuple) and kind[0] == "error"


def test_quit_closes_connection(server: Server) -> None:
    with _client(server) as s:
        s.sendall(_encode("QUIT"))
        assert _read_reply(s) == "OK"
        assert s.recv(1) == b""


def test_stop_then_connect_fails(server: Server) -> None:
    port = server.port
    server.stop()
    assert server.running is False
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=1)


def test_stop_is_idempotent(server: Server) -> None:
    server.stop()
    server.stop()
    assert server.running is False


def test_bind_failure_raises_oserror() -> None:
    with socket.socket() as blocker:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        taken = blocker.getsockname()[1]
        with Cache() as c:
            srv = Server(c, host="127.0.0.1", port=taken)
            with pytest.raises(OSError):
                srv.start()
            assert srv.running is False


def test_context_manager_starts_and_stops() -> None:
    with Cache() as c:
        with Server(c, port=0) as srv:
            assert srv.running is True
            with _client(srv) as s:
                s.sendall(_encode("PING"))
                assert _read_reply(s) == "PONG"
        assert srv.running is False


def test_restart_after_stop() -> None:
    with Cache() as c:
        srv = Server(c, port=0)
        srv.start()
        srv.stop()
        srv.start()
        try:
            with _client(srv) as s:
                s.sendall(_encode("PING"))
                assert _read_reply(s) == "PONG"
        finally:
            srv.stop()


def test_invalid_port_rejected() -> None:
    with Cache() as c:
        with pytest.raises(ValueError):
            Server(c, port=70000)
