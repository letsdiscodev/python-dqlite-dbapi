"""Pin: ``PrepareProtocol`` listed in ``dqlitedbapi.types.__all__``.

The class is the adapter-discovery protocol identity passed to
``__conform__`` per PEP 249 §3 / stdlib ``sqlite3.PrepareProtocol``.
The top-level ``dqlitedbapi.__init__`` already re-exports it (and
its ``__all__`` includes it), but ``dqlitedbapi.types.__all__`` was
inconsistent. Cross-driver code introspecting types module
``__all__`` (linters, ``from types import *`` patterns, runtime
discovery harnesses) saw the symbol as not advertised.

Pure surface tightening — no behavioural change.
"""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import types as _types


def test_prepareprotocol_in_types_all_pin() -> None:
    assert "PrepareProtocol" in _types.__all__


def test_prepareprotocol_top_level_import_pin() -> None:
    assert hasattr(dqlitedbapi, "PrepareProtocol")


def test_prepareprotocol_stdlib_parity_pin() -> None:
    """PEP 249 §3: PrepareProtocol is a class (the public name of the
    adapter-discovery protocol). Mirrors stdlib ``sqlite3.PrepareProtocol``."""
    assert isinstance(dqlitedbapi.PrepareProtocol, type)


def test_prepareprotocol_top_level_and_types_module_identity_match() -> None:
    """Single class object behind both the top-level and ``types``
    re-exports — cross-driver gating on identity (``protocol is
    dqlitedbapi.PrepareProtocol``) works through either path."""
    assert dqlitedbapi.PrepareProtocol is _types.PrepareProtocol
