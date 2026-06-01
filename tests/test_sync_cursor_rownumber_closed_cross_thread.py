"""Sync mirror: ``Cursor.rownumber`` returns None on a closed cursor regardless of thread.

The closed-state short-circuit must precede the thread-affinity check, else a cross-thread
read of a closed cursor raises ProgrammingError instead of returning None (contradicting the
documented closed -> None contract).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


def _make_cursor(closed: bool) -> Cursor:
    """Build a Cursor whose ``_connection._check_thread`` raises (foreign-thread access)."""
    cursor = Cursor.__new__(Cursor)
    cursor._closed = closed
    cursor._description = None
    cursor._row_index = 5
    cursor._rowcount = -1
    cursor._arraysize = 1
    cursor._lastrowid = None
    cursor._rows = []
    cursor._row_factory = None
    cursor._completed_iterations = 0
    cursor.messages = []

    def _check_thread() -> None:
        raise ProgrammingError("SQLite objects created in a thread can only be used in that thread")

    conn = MagicMock()
    conn._check_thread = _check_thread
    cursor._connection = conn
    return cursor


def test_closed_cursor_rownumber_returns_none_even_cross_thread() -> None:
    cursor = _make_cursor(closed=True)
    assert cursor.rownumber is None


def test_live_cursor_rownumber_still_thread_checked() -> None:
    """Negative pin: a live cursor's rownumber still runs the thread-affinity check."""
    cursor = _make_cursor(closed=False)
    with pytest.raises(ProgrammingError):
        _ = cursor.rownumber
