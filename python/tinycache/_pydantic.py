"""Lazy access to pydantic.

Nothing in this module imports pydantic at import time, so ``import tinycache``
works without pydantic installed. Pydantic is imported only when a pydantic
feature is actually used.
"""

from __future__ import annotations

import functools
import sys
from typing import Any, Optional

__all__ = ["INSTALL_HINT", "adapter", "available", "import_pydantic", "model_json"]

INSTALL_HINT = (
    "pydantic support requires pydantic; install it with: "
    "pip install 'tinycache-rs[pydantic]'"
)


def import_pydantic() -> Any:
    """Imports and returns the ``pydantic`` module, or raises ``ImportError`` with a hint."""
    try:
        import pydantic
    except ImportError as exc:
        raise ImportError(INSTALL_HINT) from exc
    return pydantic


def available() -> bool:
    """True when pydantic can be imported."""
    try:
        import_pydantic()
    except ImportError:
        return False
    return True


@functools.lru_cache(maxsize=None)
def _cached_adapter(tp: Any) -> Any:
    from pydantic import TypeAdapter

    return TypeAdapter(tp)


def adapter(tp: Any) -> Any:
    """Returns a ``TypeAdapter`` for ``tp``, cached per type."""
    import_pydantic()
    try:
        return _cached_adapter(tp)
    except TypeError:
        # Unhashable annotations cannot be cached; build the adapter directly.
        from pydantic import TypeAdapter

        return TypeAdapter(tp)


def model_json(obj: Any) -> Optional[str]:
    """Returns ``obj.model_dump_json()`` for a pydantic model instance, else ``None``.

    Never imports pydantic: if it is not loaded, no model instance can exist.
    """
    pyd = sys.modules.get("pydantic")
    if pyd is None:
        return None
    base = getattr(pyd, "BaseModel", None)
    if base is not None and isinstance(obj, base):
        return str(obj.model_dump_json())
    return None
