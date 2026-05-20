"""Pin: assigning ``sqlite3.Row`` as a ``row_factory`` and then
fetching surfaces a ``DataError`` (a PEP 249 ``dbapi.Error``
subclass), not the bare ``TypeError`` that the previous setter
returned through.

Stdlib ``sqlite3.Row`` is a C-extension constructor that type-checks
``argument 1`` to be ``pysqlite_CursorType``. The dqlite ``Cursor``
fails that check, but the setter accepts the value, so the failure
deferred to the first fetch site and surfaced as bare ``TypeError`` —
outside the ``dbapi.Error`` hierarchy.

The fetch-site wrap converts to ``DataError`` so cross-driver code
catching ``dbapi.Error`` continues to match. Docstrings on both
``Connection.row_factory`` and ``Cursor.row_factory`` no longer
recommend ``sqlite3.Row`` and instead point at plain-callable
shapes.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import DataError


def _make_cursor_with_one_row() -> Cursor:
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._description = (("x", None, None, None, None, None, None),)
    cur._rows = [(1,)]
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    cur._lastrowid = None
    cur._connection = MagicMock()
    cur._connection._check_thread = MagicMock()
    cur._row_factory = sqlite3.Row
    return cur


def test_sqlite3_row_factory_fetchone_surfaces_dataerror() -> None:
    """The C-extension type-check failure on ``sqlite3.Row.__init__``'s
    first argument must surface as ``DataError``."""
    cur = _make_cursor_with_one_row()
    with pytest.raises(DataError, match="row_factory call failed"):
        cur.fetchone()


def test_sqlite3_row_factory_fetchall_surfaces_dataerror() -> None:
    """Sibling fetchall path. The snapshot-restore discipline still
    fires (index unchanged on the raise)."""
    cur = _make_cursor_with_one_row()
    with pytest.raises(DataError, match="row_factory call failed"):
        cur.fetchall()
    # Index unchanged — the snapshot-restore contract holds.
    assert cur._row_index == 0


def test_dataerror_chains_to_original_typeerror() -> None:
    """The original ``TypeError`` survives on ``__cause__`` so SA's
    ``_walk_cause_chain`` can still classify the root cause."""
    cur = _make_cursor_with_one_row()
    try:
        cur.fetchone()
    except DataError as exc:
        assert isinstance(exc.__cause__, TypeError)
        assert "argument" in str(exc.__cause__).lower()


def test_cursor_row_factory_docstring_no_longer_recommends_sqlite3_row_unqualified() -> None:
    """``Cursor.row_factory`` docstring must not present
    ``sqlite3.Row`` as a working factory without the rejection
    caveat."""
    doc = getattr(Cursor.row_factory, "fget", Cursor.row_factory).__doc__ or ""
    # The recommendation must be removed OR explicitly caveat the
    # rejection.
    if "sqlite3.Row" in doc:
        assert "do NOT work" in doc or "DataError" in doc, (
            "If sqlite3.Row remains mentioned, the doctring must "
            "explain that it surfaces DataError at first fetch"
        )


def test_connection_row_factory_docstring_no_longer_recommends_sqlite3_row_unqualified() -> None:
    """Mirror on ``Connection.row_factory``."""
    from dqlitedbapi.connection import Connection

    doc = getattr(Connection.row_factory, "fget", Connection.row_factory).__doc__ or ""
    if "sqlite3.Row" in doc:
        assert "do NOT work" in doc or "DataError" in doc
