"""A scripted RESP2 server for client failure-mode tests.

The real server never drops a connection on cue, so retry and timeout behaviour
is exercised against this fake. ``handler(conn_index, command)`` returns the raw
reply bytes to send, or ``None`` to close the connection without replying.
"""

from __future__ import annotations

import socket
import threading
import time
from typing import IO, Callable, List, Optional

Handler = Callable[[int, List[bytes]], Optional[bytes]]


def read_command(stream: IO[bytes]) -> Optional[List[bytes]]:
    """Reads one RESP2 array of bulk strings, or returns ``None`` at EOF."""
    header = stream.readline()
    if not header:
        return None
    count = int(header[1:-2])
    args: List[bytes] = []
    for _ in range(count):
        size = int(stream.readline()[1:-2])
        args.append(stream.read(size + 2)[:-2])
    return args


class FakeServer:
    """Accepts TCP connections on 127.0.0.1 and answers them with ``handler``."""

    def __init__(self, handler: Handler) -> None:
        self._handler = handler
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(16)
        self.port: int = self._listener.getsockname()[1]
        self.received: List[List[bytes]] = []
        self.connections = 0
        self._closing = False
        self._accepter = threading.Thread(target=self._accept_loop, daemon=True)
        self._accepter.start()

    def _accept_loop(self) -> None:
        while not self._closing:
            try:
                conn, _ = self._listener.accept()
            except OSError:
                return
            if self._closing:
                conn.close()
                return
            index = self.connections
            self.connections += 1
            threading.Thread(target=self._serve, args=(conn, index), daemon=True).start()

    def _serve(self, conn: socket.socket, index: int) -> None:
        stream = conn.makefile("rb")
        try:
            while True:
                command = read_command(stream)
                if command is None:
                    break
                self.received.append(command)
                reply = self._handler(index, command)
                if reply is None:
                    break
                conn.sendall(reply)
        except OSError:
            pass
        finally:
            conn.close()

    def names(self) -> List[str]:
        """Command names received so far, upper-cased (e.g. ``["GET", "INCRBY"]``)."""
        return [c[0].decode().upper() for c in self.received]

    def close(self) -> None:
        self._closing = True
        try:
            wake = socket.create_connection(("127.0.0.1", self.port), timeout=1)
            wake.close()
        except OSError:
            pass
        self._listener.close()


def settle(seconds: float = 0.1) -> None:
    """Gives the fake server's threads time to record a command that was just sent."""
    time.sleep(seconds)
