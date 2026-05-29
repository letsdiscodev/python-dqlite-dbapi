"""Assigning ``sqlite3.Row`` as a ``row_factory`` then fetching surfaces a
``DataError`` (a ``dbapi.Error``), not the bare ``TypeError`` the C-extension
constructor raises on the dqlite ``Cursor``."""

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
    """The C-extension type-check failure must surface as ``DataError``."""
    cur = _make_cursor_with_one_row()
    with pytest.raises(DataError, match="row_factory call failed"):
        cur.fetchone()


def test_sqlite3_row_factory_fetchall_surfaces_dataerror() -> None:
    """Sibling fetchall path; index unchanged on raise (snapshot-restore)."""
    cur = _make_cursor_with_one_row()
    with pytest.raises(DataError, match="row_factory call failed"):
        cur.fetchall()
    assert cur._row_index == 0


def test_dataerror_chains_to_original_typeerror() -> None:
    """The original ``TypeError`` survives on ``__cause__`` for SA's classifier."""
    cur = _make_cursor_with_one_row()
    try:
        cur.fetchone()
    except DataError as exc:
        assert isinstance(exc.__cause__, TypeError)
        assert "argument" in str(exc.__cause__).lower()


def test_cursor_row_factory_docstring_no_longer_recommends_sqlite3_row_unqualified() -> None:
    """``Cursor.row_factory`` docstring must caveat that ``sqlite3.Row`` fails."""
    doc = getattr(Cursor.row_factory, "fget", Cursor.row_factory).__doc__ or ""
    if "sqlite3.Row" in doc:
        assert "do NOT work" in doc or "DataError" in doc, (
            "If sqlite3.Row remains mentioned, the doctring must "
            "explain that it surfaces DataError at first fetch"
        )
