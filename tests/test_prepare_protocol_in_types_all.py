"""Pin: ``PrepareProtocol`` listed in ``dqlitedbapi.types.__all__``."""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import types as _types


def test_prepareprotocol_in_types_all_pin() -> None:
    assert "PrepareProtocol" in _types.__all__


def test_prepareprotocol_top_level_import_pin() -> None:
    assert hasattr(dqlitedbapi, "PrepareProtocol")


def test_prepareprotocol_stdlib_parity_pin() -> None:
    """PrepareProtocol is a class, mirroring stdlib ``sqlite3.PrepareProtocol``."""
    assert isinstance(dqlitedbapi.PrepareProtocol, type)


def test_prepareprotocol_top_level_and_types_module_identity_match() -> None:
    """Same class object behind both the top-level and ``types`` re-exports."""
    assert dqlitedbapi.PrepareProtocol is _types.PrepareProtocol
