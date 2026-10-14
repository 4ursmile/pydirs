"""Serializers for ``Cache(serializer=...)`` and the stdlib JSON helpers.

The core stores bytes only. A serializer turns Python objects into bytes on
write and back on read. Built-in choices are ``"raw"`` (no serializer) and
``"json"``. Any object with ``dumps(obj)`` and ``loads(data)`` also works.
"""

from __future__ import annotations

import json
from typing import Any, Optional, Protocol, Union, runtime_checkable

from . import _pydantic

__all__ = [
    "JsonSerializer",
    "PydanticSerializer",
    "Serializer",
    "json_dumps",
    "json_loads",
    "resolve_serializer",
]


@runtime_checkable
class Serializer(Protocol):
    """Interface for custom serializers passed as ``Cache(serializer=...)``."""

    def dumps(self, obj: Any) -> Any:
        """Encodes ``obj`` into a value the core accepts (bytes, str, int or float)."""
        ...

    def loads(self, data: bytes) -> Any:
        """Decodes bytes read from the core back into a Python object."""
        ...


def json_dumps(obj: Any) -> bytes:
    """Encodes ``obj`` as compact, strictly valid UTF-8 JSON.

    Raises ``TypeError`` for objects JSON cannot represent, and ``ValueError`` for
    NaN or Infinity.
    """
    text = json.dumps(obj, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return text.encode("utf-8")


def json_loads(data: Union[bytes, str]) -> Any:
    """Decodes JSON. Raises ``ValueError`` if ``data`` is not valid JSON."""
    try:
        return json.loads(data)
    except ValueError as exc:
        raise ValueError(f"stored value is not valid JSON: {exc}") from exc


class JsonSerializer:
    """The built-in ``serializer="json"``, based on the stdlib ``json`` module."""

    def dumps(self, obj: Any) -> bytes:
        return json_dumps(obj)

    def loads(self, data: Union[bytes, str]) -> Any:
        return json_loads(data)


class PydanticSerializer:
    """Serializer that uses pydantic's ``TypeAdapter`` (pydantic v2 only).

    ``PydanticSerializer(model)`` validates against ``model`` on read.
    ``PydanticSerializer()`` serializes by the runtime type of each object and
    reads back plain JSON. Requires pydantic (``pip install 'tinycache-rs[pydantic]'``).
    """

    def __init__(self, model: Any = None) -> None:
        _pydantic.import_pydantic()
        self.model = model

    def dumps(self, obj: Any) -> bytes:
        target = type(obj) if self.model is None else self.model
        return bytes(_pydantic.adapter(target).dump_json(obj))

    def loads(self, data: Union[bytes, str]) -> Any:
        if self.model is None:
            return json_loads(data)
        return _pydantic.adapter(self.model).validate_json(data)


_JSON = JsonSerializer()


def resolve_serializer(value: Any) -> Optional[Serializer]:
    """Maps the ``serializer`` argument to an object, or ``None`` for ``"raw"``.

    Raises ``ValueError`` for unknown names and for objects without ``dumps`` and ``loads``.
    """
    if isinstance(value, str):
        if value == "raw":
            return None
        if value == "json":
            return _JSON
        raise ValueError(
            f"unknown serializer {value!r}; expected 'raw', 'json' or an object "
            "with dumps() and loads() methods"
        )
    if callable(getattr(value, "dumps", None)) and callable(getattr(value, "loads", None)):
        return value  # type: ignore[no-any-return]
    raise ValueError(
        f"unknown serializer {value!r}; expected 'raw', 'json' or an object "
        "with dumps() and loads() methods"
    )
