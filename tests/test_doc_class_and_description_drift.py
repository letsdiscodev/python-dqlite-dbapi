"""Pin documented behaviour for the ``Connection`` class docstring
sync/async asymmetry and the ``description`` first-non-NULL-row
resolver.

Both findings are doc-clarification scope (no behavioural change).
The pins lock the docstring claims so a future refactor cannot
quietly drift them back to a single-primitive universal claim or
hide the mixed-column flattening.
"""

from __future__ import annotations

import inspect

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


def test_connection_class_docstring_calls_out_sync_async_affinity_primitives() -> None:
    """The class docstring must name both ``_check_thread`` (sync) and
    ``_check_loop_binding`` / ``_check_loop_only`` (async) — the
    previous wording's universal ``_check_thread()`` claim was
    factually wrong for the async surface."""
    doc = Connection.__doc__ or ""
    assert "_check_thread" in doc
    assert "_check_loop" in doc


def _property_source(cls: type, name: str) -> str:
    """Return the source of the property getter on ``cls`` named
    ``name`` — works around mypy's ``Callable[[T], R]`` type for
    decorated properties by going through ``vars()`` (which preserves
    the property descriptor)."""
    descriptor = vars(cls)[name]
    assert isinstance(descriptor, property)
    fget = descriptor.fget
    assert fget is not None
    return inspect.getsource(fget)


def test_in_transaction_async_uses_loop_only_check_per_class_docstring() -> None:
    """Pin the async getter's affinity primitive against the documented
    contract — the implementation body must call ``_check_loop_only``."""
    src = _property_source(AsyncConnection, "in_transaction")
    assert "self._check_loop_only()" in src, src
    # Implementation body uses the loop-only primitive, not _check_thread.
    assert "self._check_thread()" not in src, src


def test_in_transaction_sync_uses_check_thread_per_class_docstring() -> None:
    src = _property_source(Connection, "in_transaction")
    assert "_check_thread" in src, src


def test_description_docstring_calls_out_mixed_type_column_flattening() -> None:
    """The ``description`` docstring must document the mixed-column
    flattening so callers using ``description`` for parser-shape
    decisions are warned to use per-row ``row_types[i]`` instead."""
    doc = Cursor.description.__doc__ or ""
    assert "Mixed-type" in doc or "mixed-type" in doc
    assert "first non-NULL" in doc.lower() or "FIRST non-NULL" in doc


def test_async_description_docstring_calls_out_mixed_type_column_flattening() -> None:
    doc = AsyncCursor.description.__doc__ or ""
    assert "Mixed-type" in doc or "mixed-type" in doc
