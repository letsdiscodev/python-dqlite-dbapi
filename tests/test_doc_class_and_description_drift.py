"""Lock docstring claims for the ``Connection`` sync/async affinity asymmetry and
the ``description`` first-non-NULL-row resolver against future drift.
"""

from __future__ import annotations

import inspect

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection


def _property_source(cls: type, name: str) -> str:
    """Source of property getter ``name`` via ``vars()`` (preserves the descriptor)."""
    descriptor = vars(cls)[name]
    assert isinstance(descriptor, property)
    fget = descriptor.fget
    assert fget is not None
    return inspect.getsource(fget)


def test_in_transaction_async_uses_loop_only_check_per_class_docstring() -> None:
    """The async getter body must call ``_check_loop_only``, not ``_check_thread``."""
    src = _property_source(AsyncConnection, "in_transaction")
    assert "self._check_loop_only()" in src, src
    assert "self._check_thread()" not in src, src


def test_in_transaction_sync_uses_check_thread_per_class_docstring() -> None:
    src = _property_source(Connection, "in_transaction")
    assert "_check_thread" in src, src


def test_async_description_docstring_calls_out_mixed_type_column_flattening() -> None:
    doc = AsyncCursor.description.__doc__ or ""
    assert "Mixed-type" in doc or "mixed-type" in doc
