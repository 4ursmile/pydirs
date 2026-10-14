"""``Client``: a thread-safe, pure-Python client for the tinycache RESP2 server.

The wire protocol (RESP2) is encoded and decoded here by hand, so there is no
dependency on redis-py. Every public method comes from ``TinyCacheAPI``; this
module only implements the bytes-level backend on top of a small connection pool.

Retry rule: a command is re-sent once on a fresh connection only when it is
*idempotent*, meaning repeating it cannot change the result (pure reads, plain
SET/MSET, EXPIRE/PEXPIRE, FLUSHALL, PING, INFO). Commands that change state
non-idempotently (INCR, APPEND, LPUSH/RPUSH, LPOP/RPOP, and DEL/HSET/SADD/SREM/
PERSIST, whose replies would change on repeat) are never re-sent after a possible
send. Pooled connections whose socket is already closed by the peer are dropped
before anything is sent, so a server restart is invisible to the caller.
"""

from __future__ import annotations

import select
import socket
import threading
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, Union

from ._api import TinyCacheAPI, _check_set_args
from ._serializers import Serializer

__all__ = ["AuthenticationError", "Client", "Pipeline", "ResponseError"]

Key = Union[str, bytes, int, float]


class ResponseError(Exception):
    """The server replied with an error that has no more specific Python exception."""


class AuthenticationError(ConnectionError):
    """The server requires a password that was not sent, or the one sent is wrong.

    Subclasses ``ConnectionError``, so code that already handles connection failures
    also catches it.
    """


class _ErrorReply:
    """An ``-ERR``-style reply, returned (not raised) while decoding so pipelines can finish reading."""

    __slots__ = ("message",)

    def __init__(self, message: str) -> None:
        self.message = message


class _Broken(Exception):
    """The connection failed mid-exchange, or the reply stream is malformed."""


# ---- RESP2 encoding ----------------------------------------------------------


def _arg(obj: Any) -> bytes:
    """Converts one command argument to bytes, with the same rules as the native cache."""
    if isinstance(obj, bytes):
        return obj
    if isinstance(obj, str):
        return obj.encode("utf-8")
    if isinstance(obj, bool):
        raise TypeError("expected bytes, str, int or float, not bool")
    if isinstance(obj, (int, float)):
        return str(obj).encode("ascii")
    raise TypeError(f"expected bytes, str, int or float, not {type(obj).__name__}")


def _encode(args: Sequence[Any]) -> bytes:
    """Encodes a command as a RESP2 array of bulk strings."""
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        raw = _arg(arg)
        parts.append(b"$%d\r\n" % len(raw))
        parts.append(raw)
        parts.append(b"\r\n")
    return b"".join(parts)


# ---- RESP2 decoding ----------------------------------------------------------


def _read_line(rfile: Any) -> bytes:
    line = rfile.readline()
    if not line:
        raise _Broken("connection closed by server")
    if not line.endswith(b"\r\n"):
        raise _Broken("truncated reply line")
    return line[:-2]


def _int_body(body: bytes) -> int:
    try:
        return int(body)
    except ValueError as exc:
        raise _Broken(f"invalid integer in reply: {body!r}") from exc


def _read_reply(rfile: Any) -> Any:
    """Reads one RESP2 reply.

    Simple strings become ``str``, bulk strings ``bytes``, integers ``int``, nil
    ``None``, arrays ``list``. Error replies come back as ``_ErrorReply``.
    """
    line = _read_line(rfile)
    if not line:
        raise _Broken("empty reply")
    kind, body = line[:1], line[1:]
    if kind == b"+":
        return body.decode("utf-8", "replace")
    if kind == b"-":
        return _ErrorReply(body.decode("utf-8", "replace"))
    if kind == b":":
        return _int_body(body)
    if kind == b"$":
        size = _int_body(body)
        if size < 0:
            return None
        data = rfile.read(size + 2)
        if len(data) != size + 2 or not data.endswith(b"\r\n"):
            raise _Broken("truncated bulk reply")
        return data[:-2]
    if kind == b"*":
        count = _int_body(body)
        if count < 0:
            return None
        return [_read_reply(rfile) for _ in range(count)]
    raise _Broken(f"unknown reply type {kind!r}")


def _map_error(message: str) -> Exception:
    """Maps a server error line onto the Python exception the API documents."""
    head = message.split(" ", 1)[0]
    if head == "WRONGTYPE":
        return TypeError(message)
    if head == "OOM":
        return MemoryError(message)
    if head in ("NOAUTH", "WRONGPASS") or message.startswith("ERR invalid password"):
        return AuthenticationError(message)
    if message.startswith("ERR value is not an integer") or "would overflow" in message:
        return ValueError(message)
    if message.startswith("ERR persistence is not enabled"):
        return ValueError(message)
    return ResponseError(message)


def _info_value(text: str) -> Any:
    """Converts one INFO value: integers and floats become numbers, anything else stays text."""
    try:
        return int(text)
    except ValueError:
        pass
    try:
        return float(text)
    except ValueError:
        return text


def _parse_info(text: str) -> Dict[str, Any]:
    """Parses INFO text (``key:value`` lines, ``# Section`` headers ignored) into a dict."""
    out: Dict[str, Any] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        key, value = line.split(":", 1)
        out[key] = _info_value(value)
    return out


# ---- connections and pool ----------------------------------------------------


class _Connection:
    """One TCP connection. Used by a single thread at a time (checked out of the pool)."""

    def __init__(self, host: str, port: int, timeout: Optional[float]) -> None:
        try:
            self.sock = socket.create_connection((host, port), timeout=timeout)
        except OSError as exc:
            raise ConnectionError(f"cannot connect to {host}:{port}: {exc}") from exc
        try:
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:  # pragma: no cover - platform without TCP_NODELAY
            pass
        self._rfile = self.sock.makefile("rb")

    def usable(self) -> bool:
        """False when an idle socket has become readable: EOF or unexpected bytes."""
        try:
            readable, _, _ = select.select([self.sock], [], [], 0)
        except (OSError, ValueError):
            return False
        return not readable

    def send(self, payload: bytes) -> None:
        self.sock.sendall(payload)

    def read_reply(self) -> Any:
        return _read_reply(self._rfile)

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._rfile.close()
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class Client(TinyCacheAPI):
    """Client for a running ``tinycache`` server (``python -m tinycache`` or ``tinycache.Server``).

    It offers the same methods as ``Cache``. Every call borrows a pooled
    connection, so one ``Client`` can be shared by many threads; at most
    ``pool_size`` commands run at once. Connection problems raise
    ``ConnectionError``. Server errors map to ``TypeError`` (WRONGTYPE),
    ``MemoryError`` (OOM), ``ValueError`` (not an integer) or ``ResponseError``.

    Commands that the server does not implement (``used_memory``) raise
    ``NotImplementedError``.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        password: Optional[Union[str, bytes]] = None,
        timeout: Optional[float] = 5.0,
        pool_size: int = 8,
        decode: bool = False,
        serializer: Union[str, Serializer] = "raw",
    ) -> None:
        if isinstance(pool_size, bool) or not isinstance(pool_size, int) or pool_size < 1:
            raise ValueError("pool_size must be a positive integer")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be a positive number of seconds, or None")
        if isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536:
            raise ValueError("port must be an integer between 1 and 65535")
        self.host = host
        self.port = port
        self._password = password
        self._timeout = timeout
        self._pool_size = pool_size
        self._idle: List[_Connection] = []
        self._lock = threading.Lock()
        self._slots = threading.BoundedSemaphore(pool_size)
        self._closed = False
        self._init_api(decode, serializer)

    def __repr__(self) -> str:
        return f"Client(host={self.host!r}, port={self.port!r})"

    # ---- pool ----------------------------------------------------------------

    def _take(self) -> Optional[_Connection]:
        """Returns an idle connection that still looks alive, or ``None``."""
        with self._lock:
            while self._idle:
                conn = self._idle.pop()
                if conn.usable():
                    return conn
                conn.close()
        return None

    def _give_back(self, conn: _Connection) -> None:
        with self._lock:
            if not self._closed and len(self._idle) < self._pool_size:
                self._idle.append(conn)
                return
        conn.close()

    def _new_connection(self) -> _Connection:
        conn = _Connection(self.host, self.port, self._timeout)
        if self._password is None:
            return conn
        try:
            conn.send(_encode(["AUTH", self._password]))
            reply = conn.read_reply()
        except (_Broken, OSError) as exc:
            conn.close()
            raise ConnectionError(f"AUTH failed: {exc}") from exc
        if isinstance(reply, _ErrorReply):
            conn.close()
            raise AuthenticationError(f"AUTH failed: {reply.message}")
        return conn

    def _exchange(self, commands: List[bytes], retry: bool) -> List[Any]:
        """Sends encoded commands in one write and returns their raw replies.

        ``retry`` allows one reconnect-and-resend after a broken connection. It must
        be False for any command that may have taken effect.
        """
        if self._closed:
            raise ConnectionError("client is closed")
        payload = b"".join(commands)
        if not self._slots.acquire(timeout=self._timeout):
            raise ConnectionError("timed out waiting for a free pooled connection")
        try:
            last: Optional[BaseException] = None
            for _attempt in range(2 if retry else 1):
                # Connecting failures raise ConnectionError here, outside the
                # retry handler below, so an unreachable server is reported at once.
                conn = self._take() or self._new_connection()
                try:
                    conn.send(payload)
                    replies = [conn.read_reply() for _ in commands]
                except (_Broken, OSError) as exc:
                    conn.close()
                    last = exc
                    continue
                self._give_back(conn)
                return replies
            raise ConnectionError(f"connection to {self.host}:{self.port} lost: {last}") from last
        finally:
            self._slots.release()

    def _call(self, *args: Any, retry: bool = True) -> Any:
        """Runs one command and returns its reply, raising mapped server errors."""
        reply = self._exchange([_encode(args)], retry)[0]
        if isinstance(reply, _ErrorReply):
            raise _map_error(reply.message)
        return reply

    def close(self) -> None:
        """Closes every pooled connection. Later calls raise ``ConnectionError``. Idempotent."""
        with self._lock:
            self._closed = True
            idle, self._idle = self._idle, []
        for conn in idle:
            conn.close()

    # ---- health and pipelining ---------------------------------------------------

    def ping(self) -> bool:
        """True if the server answers PING."""
        return self._call("PING") == "PONG"

    def pipeline(self) -> "Pipeline":
        """Returns a ``Pipeline`` that sends queued commands in one write."""
        return Pipeline(self)

    # ---- backend: strings ----------------------------------------------------------

    def _b_set(self, key: Key, value: Any, ex: Optional[int], px: Optional[int], nx: bool, xx: bool) -> bool:
        args: List[Any] = ["SET", key, value]
        if ex is not None:
            args += ["EX", ex]
        if px is not None:
            args += ["PX", px]
        if nx:
            args.append("NX")
        if xx:
            args.append("XX")
        return self._call(*args, retry=not (nx or xx)) == "OK"

    def _b_get(self, key: Key) -> Optional[bytes]:
        return self._call("GET", key)  # type: ignore[no-any-return]

    def _b_mget(self, keys: List[Key]) -> List[Optional[bytes]]:
        return self._call("MGET", *keys)  # type: ignore[no-any-return]

    def _b_mset(self, mapping: Dict[Key, Any]) -> None:
        args: List[Any] = ["MSET"]
        for k, v in mapping.items():
            args += [k, v]
        self._call(*args)

    def _b_incr(self, key: Key, delta: int) -> int:
        return self._call("INCRBY", key, delta, retry=False)  # type: ignore[no-any-return]

    def _b_append(self, key: Key, value: Any) -> int:
        return self._call("APPEND", key, value, retry=False)  # type: ignore[no-any-return]

    # ---- backend: keys -------------------------------------------------------------

    def _b_delete(self, keys: Tuple[Key, ...]) -> int:
        if not keys:
            return 0
        return self._call("DEL", *keys, retry=False)  # type: ignore[no-any-return]

    def _b_exists(self, key: Key) -> bool:
        return self._call("EXISTS", key) > 0

    def _b_expire(self, key: Key, seconds: int) -> bool:
        return self._call("EXPIRE", key, seconds) == 1

    def _b_pexpire(self, key: Key, milliseconds: int) -> bool:
        return self._call("PEXPIRE", key, milliseconds) == 1

    def _b_persist(self, key: Key) -> bool:
        return self._call("PERSIST", key, retry=False) == 1

    def _b_pttl(self, key: Key) -> int:
        return self._call("PTTL", key)  # type: ignore[no-any-return]

    def _b_keys(self, pattern: str) -> List[bytes]:
        return self._call("KEYS", pattern) or []  # type: ignore[no-any-return]

    def _b_flush(self) -> None:
        self._call("FLUSHALL")

    def _b_used_memory(self) -> int:
        value = self.info().get("used_memory")
        if not isinstance(value, int):
            raise NotImplementedError("the server's INFO reply has no used_memory field")
        return value

    # ---- server admin and persistence ---------------------------------------------

    def info(self, section: Optional[str] = None) -> Dict[str, Any]:
        """Server statistics from ``INFO`` as a dict. Numeric values are converted to ``int``
        or ``float``. Section headers are dropped, so keys are unique across sections."""
        args: List[Any] = ["INFO"]
        if section is not None:
            args.append(section)
        text = self._call(*args)
        return _parse_info((text or b"").decode("utf-8", "replace"))

    def save(self) -> None:
        """Writes a snapshot now (``SAVE``). Raises ``ValueError`` if the server has no persistence."""
        self._call("SAVE")

    def bgsave(self) -> None:
        """Starts a background snapshot (``BGSAVE``) and returns at once."""
        self._call("BGSAVE")

    def lastsave(self) -> int:
        """Unix time, in whole seconds, of the last successful snapshot (0 if none)."""
        return self._call("LASTSAVE")  # type: ignore[no-any-return]

    def _b_len(self) -> int:
        return self._call("DBSIZE")  # type: ignore[no-any-return]

    # ---- backend: hashes -------------------------------------------------------------

    def _b_hset(self, key: Key, field: Key, value: Any) -> bool:
        return self._call("HSET", key, field, value, retry=False) > 0

    def _b_hget(self, key: Key, field: Key) -> Optional[bytes]:
        return self._call("HGET", key, field)  # type: ignore[no-any-return]

    def _b_hdel(self, key: Key, fields: Tuple[Key, ...]) -> int:
        if not fields:
            return 0
        return self._call("HDEL", key, *fields, retry=False)  # type: ignore[no-any-return]

    def _b_hgetall(self, key: Key) -> Dict[bytes, bytes]:
        flat = self._call("HGETALL", key) or []
        return dict(zip(flat[::2], flat[1::2]))

    def _b_hlen(self, key: Key) -> int:
        return self._call("HLEN", key)  # type: ignore[no-any-return]

    # ---- backend: lists --------------------------------------------------------------

    def _b_lpush(self, key: Key, values: Tuple[Any, ...]) -> int:
        return self._call("LPUSH", key, *values, retry=False)  # type: ignore[no-any-return]

    def _b_rpush(self, key: Key, values: Tuple[Any, ...]) -> int:
        return self._call("RPUSH", key, *values, retry=False)  # type: ignore[no-any-return]

    def _b_lpop(self, key: Key) -> Optional[bytes]:
        return self._call("LPOP", key, retry=False)  # type: ignore[no-any-return]

    def _b_rpop(self, key: Key) -> Optional[bytes]:
        return self._call("RPOP", key, retry=False)  # type: ignore[no-any-return]

    def _b_lrange(self, key: Key, start: int, stop: int) -> List[bytes]:
        return self._call("LRANGE", key, start, stop) or []  # type: ignore[no-any-return]

    def _b_llen(self, key: Key) -> int:
        return self._call("LLEN", key)  # type: ignore[no-any-return]

    # ---- backend: sets --------------------------------------------------------------

    def _b_sadd(self, key: Key, members: Tuple[Any, ...]) -> int:
        if not members:
            return 0
        return self._call("SADD", key, *members, retry=False)  # type: ignore[no-any-return]

    def _b_srem(self, key: Key, members: Tuple[Any, ...]) -> int:
        if not members:
            return 0
        return self._call("SREM", key, *members, retry=False)  # type: ignore[no-any-return]

    def _b_smembers(self, key: Key) -> List[bytes]:
        return self._call("SMEMBERS", key) or []  # type: ignore[no-any-return]

    def _b_sismember(self, key: Key, member: Any) -> bool:
        return self._call("SISMEMBER", key, member) == 1

    def _b_scard(self, key: Key) -> int:
        return self._call("SCARD", key)  # type: ignore[no-any-return]


class Pipeline:
    """Queues commands and sends them in one write (``with client.pipeline() as p``).

    Queued methods return the pipeline, so calls can be chained. Leaving the
    ``with`` block sends the queue; ``execute()`` does the same explicitly and
    returns one result per command, in order. The set of commands is limited to
    set, get, delete, incr, hset, hget, lpush and rpush. Pipelines are not
    retried. If any reply is an error, the first one is raised after all replies
    have been read.
    """

    def __init__(self, client: Client) -> None:
        self._client = client
        self._cmds: List[Tuple[bytes, Callable[[Any], Any]]] = []
        self.results: List[Any] = []  # filled in when the ``with`` block exits cleanly

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        if exc_type is None:
            self.results = self.execute()
        else:
            self._cmds.clear()

    def __len__(self) -> int:
        return len(self._cmds)

    def _queue(self, args: Sequence[Any], convert: Callable[[Any], Any]) -> "Pipeline":
        self._cmds.append((_encode(args), convert))
        return self

    def set(
        self,
        key: Key,
        value: Any,
        ex: Optional[int] = None,
        px: Optional[int] = None,
        nx: bool = False,
        xx: bool = False,
    ) -> "Pipeline":
        _check_set_args(ex, px, nx, xx)
        args: List[Any] = ["SET", key, value]
        if ex is not None:
            args += ["EX", ex]
        if px is not None:
            args += ["PX", px]
        if nx:
            args.append("NX")
        if xx:
            args.append("XX")
        return self._queue(args, lambda r: r == "OK")

    def get(self, key: Key) -> "Pipeline":
        return self._queue(["GET", key], self._client._text)

    def delete(self, *keys: Key) -> "Pipeline":
        if not keys:
            raise ValueError("delete needs at least one key")
        return self._queue(["DEL", *keys], lambda r: r)

    def incr(self, key: Key, amount: int = 1) -> "Pipeline":
        return self._queue(["INCRBY", key, amount], lambda r: r)

    def hset(self, key: Key, field: Key, value: Any) -> "Pipeline":
        return self._queue(["HSET", key, field, value], lambda r: r > 0)

    def hget(self, key: Key, field: Key) -> "Pipeline":
        return self._queue(["HGET", key, field], self._client._text)

    def lpush(self, key: Key, *values: Any) -> "Pipeline":
        return self._queue(["LPUSH", key, *values], lambda r: r)

    def rpush(self, key: Key, *values: Any) -> "Pipeline":
        return self._queue(["RPUSH", key, *values], lambda r: r)

    def execute(self) -> List[Any]:
        """Sends the queued commands and returns their results in order."""
        cmds, self._cmds = self._cmds, []
        if not cmds:
            return []
        replies = self._client._exchange([data for data, _ in cmds], retry=False)
        for reply in replies:
            if isinstance(reply, _ErrorReply):
                raise _map_error(reply.message)
        return [convert(reply) for (_, convert), reply in zip(cmds, replies)]
