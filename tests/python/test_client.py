"""Client specifics: RESP2 codec, errors, pooling, pipeline, AUTH and reconnect rules."""

from __future__ import annotations

import socket
import threading
from collections.abc import Iterator

import pytest

import pydirs
from pydirs import Cache, Client, ResponseError, Server
from pydirs._client import _encode, _map_error, _read_reply

from fake_resp import FakeServer, settle


@pytest.fixture
def live() -> Iterator[tuple[Server, Cache]]:
    cache = Cache()
    srv = Server(cache, host="127.0.0.1", port=0)
    srv.start()
    try:
        yield srv, cache
    finally:
        srv.stop()
        cache.close()


def _client(srv: Server, **kwargs: object) -> Client:
    return Client(host="127.0.0.1", port=srv.port, **kwargs)  # type: ignore[arg-type]


# ---- RESP2 codec -------------------------------------------------------------


def test_encode_builds_resp_array_of_bulk_strings() -> None:
    assert _encode(["SET", "k", 5]) == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\n5\r\n"


def test_encode_rejects_bool_and_none() -> None:
    with pytest.raises(TypeError):
        _encode(["SET", True])
    with pytest.raises(TypeError):
        _encode(["SET", None])


def _decode(data: bytes) -> object:
    import io

    return _read_reply(io.BytesIO(data))


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"+OK\r\n", "OK"),
        (b":42\r\n", 42),
        (b":-7\r\n", -7),
        (b"$5\r\nhello\r\n", b"hello"),
        (b"$0\r\n\r\n", b""),
        (b"$-1\r\n", None),
        (b"*2\r\n$1\r\na\r\n:2\r\n", [b"a", 2]),
        (b"*-1\r\n", None),
        (b"*0\r\n", []),
    ],
)
def test_decode_scalar_and_array_replies(raw: bytes, expected: object) -> None:
    assert _decode(raw) == expected


def test_decode_error_reply_is_not_raised_by_reader() -> None:
    reply = _decode(b"-WRONGTYPE nope\r\n")
    assert getattr(reply, "message") == "WRONGTYPE nope"


def test_decode_binary_bulk_with_crlf_inside() -> None:
    assert _decode(b"$4\r\na\r\nb\r\n") == b"a\r\nb"


@pytest.mark.parametrize("raw", [b"$5\r\nhi\r\n", b"?x\r\n", b":abc\r\n", b""])
def test_decode_malformed_reply_is_broken(raw: bytes) -> None:
    from pydirs._client import _Broken

    with pytest.raises(_Broken):
        _decode(raw)


# ---- error mapping ----------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "exc_type"),
    [
        ("WRONGTYPE Operation against a key holding the wrong kind of value", TypeError),
        ("OOM command not allowed when used memory > 'maxmemory'", MemoryError),
        ("ERR value is not an integer or out of range", ValueError),
        ("ERR increment or decrement would overflow", ValueError),
        ("ERR syntax error", ResponseError),
        ("ERR unknown command 'FOO'", ResponseError),
    ],
)
def test_error_message_maps_to_python_exception(message: str, exc_type: type) -> None:
    err = _map_error(message)
    assert type(err) is exc_type
    assert message in str(err)


def test_response_error_is_exported_exception() -> None:
    assert issubclass(ResponseError, Exception)
    assert "ResponseError" in pydirs.__all__
    assert "Client" in pydirs.__all__


def test_server_errors_map_through_real_client(live) -> None:
    srv, _ = live
    with _client(srv) as c:
        c.rpush("l", "a")
        with pytest.raises(TypeError):
            c.get("l")
        c.set("s", "text")
        with pytest.raises(ValueError):
            c.incr("s")
        with pytest.raises(ResponseError):
            c._call("NOSUCHCOMMAND")


def test_oom_maps_to_memory_error_through_client() -> None:
    with Cache(max_memory=2000, policy="noeviction") as backing:
        srv = Server(backing, port=0)
        srv.start()
        try:
            with _client(srv) as c:
                with pytest.raises(MemoryError):
                    for i in range(100):
                        c.set(f"k{i}", "x" * 400)
        finally:
            srv.stop()


# ---- connections and reconnect ----------------------------------------------------


def test_connection_refused_is_connection_error() -> None:
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()  # nothing listens here now
    c = Client(host="127.0.0.1", port=port, timeout=1)
    with pytest.raises(ConnectionError):
        c.get("k")
    c.close()


def test_closed_client_raises_connection_error(live) -> None:
    srv, _ = live
    c = _client(srv)
    c.close()
    c.close()  # idempotent
    with pytest.raises(ConnectionError):
        c.get("k")


def test_ping_round_trip(live) -> None:
    srv, _ = live
    with _client(srv) as c:
        assert c.ping() is True


def test_reconnects_after_server_restart(live) -> None:
    srv, cache = live
    c = _client(srv)
    c.set("before", "1")
    port = srv.port
    srv.stop()
    srv2 = Server(cache, host="127.0.0.1", port=port)
    srv2.start()
    try:
        # The pooled socket is dead. The idempotent GET reconnects and succeeds.
        assert c.get("before") == b"1"
        assert c.set("after", "2") is True
    finally:
        c.close()
        srv2.stop()


def test_idempotent_command_retries_once_after_broken_connection(live) -> None:
    srv, cache = live
    c = _client(srv)
    c.set("k", "v")  # warms one pooled connection
    srv.stop()
    srv2 = Server(cache, host="127.0.0.1", port=srv.port)
    srv2.start()
    try:
        assert c.get("k") == b"v"
    finally:
        c.close()
        srv2.stop()


def test_incr_is_not_retried_after_broken_connection() -> None:
    # The first INCRBY reaches the server and the connection drops before the reply.
    # A retry would count it twice, so the client must raise instead.
    fake = FakeServer(lambda conn, cmd: None if cmd[0] == b"INCRBY" else b"+OK\r\n")
    try:
        c = Client(host="127.0.0.1", port=fake.port, timeout=2)
        with pytest.raises(ConnectionError):
            c.incr("n")
        settle()
        assert fake.names().count("INCRBY") == 1
        c.close()
    finally:
        fake.close()


@pytest.mark.parametrize("command", ["APPEND", "LPUSH", "RPUSH", "LPOP", "RPOP", "HSET", "SADD"])
def test_non_idempotent_writes_are_never_retried(command: str) -> None:
    def handler(conn: int, cmd: list[bytes]) -> bytes | None:
        return None if cmd[0].decode() == command else b"+OK\r\n"

    fake = FakeServer(handler)
    try:
        c = Client(host="127.0.0.1", port=fake.port, timeout=2)
        method = {
            "APPEND": lambda: c.append("k", "x"),
            "LPUSH": lambda: c.lpush("k", "x"),
            "RPUSH": lambda: c.rpush("k", "x"),
            "LPOP": lambda: c.lpop("k"),
            "RPOP": lambda: c.rpop("k"),
            "HSET": lambda: c.hset("k", "f", "x"),
            "SADD": lambda: c.sadd("k", "x"),
        }[command]
        with pytest.raises(ConnectionError):
            method()
        settle()
        assert fake.names().count(command) == 1
        c.close()
    finally:
        fake.close()


def test_idempotent_get_is_retried_once_on_dropped_reply() -> None:
    # The first GET is dropped mid-exchange. The retry on a new connection succeeds.
    fake = FakeServer(lambda conn, cmd: None if conn == 0 else b"$1\r\nv\r\n")
    try:
        c = Client(host="127.0.0.1", port=fake.port, timeout=2)
        assert c.get("k") == b"v"
        settle()
        assert fake.names().count("GET") == 2
        c.close()
    finally:
        fake.close()


def test_unreachable_after_retry_is_connection_error() -> None:
    fake = FakeServer(lambda conn, cmd: None)
    try:
        c = Client(host="127.0.0.1", port=fake.port, timeout=2)
        with pytest.raises(ConnectionError):
            c.get("k")
        c.close()
    finally:
        fake.close()


def test_timeout_is_connection_error() -> None:
    def silent(conn: int, cmd: list[bytes]) -> bytes | None:
        import time

        time.sleep(3)
        return b"+OK\r\n"

    fake = FakeServer(silent)
    try:
        c = Client(host="127.0.0.1", port=fake.port, timeout=0.2)
        with pytest.raises(ConnectionError):
            c.get("k")
        c.close()
    finally:
        fake.close()


# ---- AUTH ------------------------------------------------------------------------


def test_auth_is_sent_on_connect_when_password_set() -> None:
    def handler(conn: int, cmd: list[bytes]) -> bytes:
        return b"+OK\r\n"

    fake = FakeServer(handler)
    try:
        c = Client(host="127.0.0.1", port=fake.port, password="s3cret", timeout=2)
        c.set("k", "v")
        settle()
        assert fake.received[0] == [b"AUTH", b"s3cret"]
        c.close()
    finally:
        fake.close()


def test_auth_rejected_raises_connection_error() -> None:
    fake = FakeServer(lambda conn, cmd: b"-WRONGPASS invalid password\r\n")
    try:
        c = Client(host="127.0.0.1", port=fake.port, password="bad", timeout=2)
        with pytest.raises(ConnectionError, match="AUTH failed"):
            c.get("k")
        c.close()
    finally:
        fake.close()


def test_no_auth_sent_without_password() -> None:
    fake = FakeServer(lambda conn, cmd: b"$-1\r\n")
    try:
        c = Client(host="127.0.0.1", port=fake.port, timeout=2)
        c.get("k")
        settle()
        assert fake.names() == ["GET"]
        c.close()
    finally:
        fake.close()


# ---- constructor validation -------------------------------------------------------


@pytest.mark.parametrize("kwargs", [{"pool_size": 0}, {"pool_size": True}, {"port": 0}, {"timeout": 0}])
def test_constructor_rejects_bad_arguments(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        Client(**kwargs)


def test_repr_mentions_endpoint() -> None:
    assert repr(Client(port=6399)) == "Client(host='127.0.0.1', port=6399)"


# ---- pool concurrency ----------------------------------------------------------------


def test_eight_threads_500_incr_each_is_exact(live) -> None:
    srv, _ = live
    with _client(srv, pool_size=8) as c:
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                for _ in range(500):
                    c.incr("total")
            except BaseException as exc:  # pragma: no cover - reported below
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert c.get("total") == b"4000"


def test_pool_never_exceeds_pool_size(live) -> None:
    srv, _ = live
    with _client(srv, pool_size=2) as c:
        opened = []
        original = c._new_connection

        def counting() -> object:
            conn = original()
            opened.append(conn)
            return conn

        c._new_connection = counting  # type: ignore[method-assign]
        threads = [threading.Thread(target=lambda: [c.get("x") for _ in range(50)]) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # Six threads share two slots, so at most two connections are ever checked out
        # at once. Idle connections beyond pool_size are closed rather than kept.
        assert len(opened) <= 6
        assert len(c._idle) <= 2


def test_pool_size_one_serialises_correctly(live) -> None:
    srv, _ = live
    with _client(srv, pool_size=1) as c:
        results: list[int] = []

        def worker() -> None:
            for _ in range(100):
                results.append(c.incr("n"))

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(results) == list(range(1, 401))


# ---- pipeline ----------------------------------------------------------------------


def test_pipeline_returns_results_in_order(live) -> None:
    srv, _ = live
    with _client(srv) as c:
        with c.pipeline() as p:
            p.set("a", "1").get("a").incr("n", 3).hset("h", "f", "v").hget("h", "f")
            p.rpush("l", "x").lpush("l", "y").delete("a", "missing")
        # rpush "x" to an empty list gives 1, lpush "y" gives 2, delete removes "a" only.
        assert p.results == [True, b"1", 3, True, b"v", 1, 2, 1]


def test_pipeline_is_one_write_for_many_commands(live) -> None:
    srv, _ = live
    with _client(srv) as c:
        c.set("n", 0)
        with c.pipeline() as p:
            for _ in range(20):
                p.incr("n")
        assert p.results == list(range(1, 21))
        assert c.get("n") == b"20"


def test_pipeline_explicit_execute(live) -> None:
    srv, _ = live
    with _client(srv) as c:
        p = c.pipeline()
        p.set("k", "v", px=5000)
        assert len(p) == 1
        assert p.execute() == [True]
        assert p.execute() == []


def test_pipeline_error_reply_raises_after_reading(live) -> None:
    srv, _ = live
    with _client(srv) as c:
        c.rpush("l", "x")
        p = c.pipeline()
        p.set("ok", "1").get("l")
        with pytest.raises(TypeError):
            p.execute()
        # The connection stayed in sync: later commands still work.
        assert c.get("ok") == b"1"


def test_pipeline_exception_inside_block_sends_nothing(live) -> None:
    srv, _ = live
    with _client(srv) as c:
        with pytest.raises(RuntimeError):
            with c.pipeline() as p:
                p.set("never", "1")
                raise RuntimeError("boom")
        assert c.get("never") is None


def test_pipeline_rejects_empty_delete(live) -> None:
    srv, _ = live
    with _client(srv) as c:
        with pytest.raises(ValueError):
            c.pipeline().delete()


def test_pipeline_decode_option(live) -> None:
    srv, _ = live
    with _client(srv, decode=True) as c:
        c.set("k", "text")
        with c.pipeline() as p:
            p.get("k")
        assert p.results == ["text"]


# ---- ping and API parity -------------------------------------------------------------


# Persistence belongs to the process that owns the data directory, so a remote Client
# has no equivalent. Its server-side counterparts are SAVE, BGSAVE, LASTSAVE and INFO.
CACHE_ONLY = {"persistent", "persist_stats"}


def test_client_offers_every_public_cache_method() -> None:
    cache_api = {n for n in dir(Cache) if not n.startswith("_")}
    client_api = {n for n in dir(Client) if not n.startswith("_")}
    assert cache_api - client_api == CACHE_ONLY
