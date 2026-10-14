"""Proves tinycache works without pydantic installed.

pydantic is blocked in a child interpreter with a meta_path finder, so the
import fails exactly as it would on a machine without it. The child process
keeps the test isolated from the pydantic installed in this environment.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

_CHILD = textwrap.dedent(
    """
    import sys

    class _Block:
        def find_spec(self, name, path=None, target=None):
            if name == "pydantic" or name.startswith("pydantic."):
                raise ImportError("pydantic blocked for test")
            return None

    sys.meta_path.insert(0, _Block())
    for mod in list(sys.modules):
        if mod == "pydantic" or mod.startswith("pydantic."):
            del sys.modules[mod]

    import tinycache
    from tinycache import Cache, Server

    assert "pydantic" not in sys.modules

    # Core and JSON features still work.
    with Cache(serializer="json") as c:
        c.set_json("k", {"a": [1, None]})
        assert c.get_json("k") == {"a": [1, None]}

    # Memoize falls back to the JSON serializer when pydantic is absent.
    cache = Cache(serializer="json")

    @cache.memoize(ttl=60)
    def build(n: int) -> dict:
        return {"n": n}

    assert build(2) == {"n": 2}
    assert build(2) == {"n": 2}

    # Pydantic features raise ImportError with an install hint.
    try:
        cache.set_model("m", {"x": 1})
    except ImportError as exc:
        assert "tinycache-rs[pydantic]" in str(exc), str(exc)
    else:
        raise AssertionError("set_model should need pydantic")

    cache.close()
    print("NO_PYDANTIC_OK")
    """
)


def test_package_works_without_pydantic() -> None:
    proc = subprocess.run(
        [sys.executable, "-c", _CHILD],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "NO_PYDANTIC_OK" in proc.stdout


def test_get_model_without_pydantic_hints_install() -> None:
    script = _CHILD.replace('cache.set_model("m", {"x": 1})', 'cache.get_model("m", dict)')
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "NO_PYDANTIC_OK" in proc.stdout
