"""``Server``: the native RESP2 server plus context-manager support."""

from __future__ import annotations

from types import TracebackType
from typing import Optional, Type

from ._pydirs import Server as _NativeServer

__all__ = ["Server"]


class Server(_NativeServer):
    """RESP2 server backed by a ``Cache``.

    Use it as a context manager to start on entry and stop on exit::

        with Server(cache, port=0) as srv:
            print(srv.port)
    """

    def __enter__(self) -> "Server":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        self.stop()
