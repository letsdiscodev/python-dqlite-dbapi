"""Pin: under ``check_same_thread=False`` the ``arraysize``/``row_factory``
setters skip the thread check (by design — the user opted into sharing).
"""

from __future__ import annotations

import threading

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


def _make_cursor_with_check_same_thread_false() -> Cursor:
    import os

    conn = Connection.__new__(Connection)
    conn._check_same_thread = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._closed = False
    cursor = Cursor.__new__(Cursor)
    cursor._connection = conn
    cursor._closed = False
    cursor._arraysize = 1
    cursor._row_factory = None
    cursor.messages = []
    return cursor


def test_arraysize_setter_does_not_raise_from_foreign_thread_under_csf_false() -> None:
    """Under check_same_thread=False, a foreign-thread setter call must not raise."""
    cur = _make_cursor_with_check_same_thread_false()
    result: list[BaseException | None] = []

    def from_other_thread() -> None:
        try:
            cur.arraysize = 50
            result.append(None)
        except BaseException as e:
            result.append(e)

    t = threading.Thread(target=from_other_thread)
    t.start()
    t.join()

    assert result == [None], (
        f"arraysize setter from foreign thread under check_same_thread=False "
        f"must not raise; got: {result!r}"
    )
    assert cur._arraysize == 50


def test_row_factory_setter_does_not_raise_from_foreign_thread_under_csf_false() -> None:
    """Mirror pin for row_factory.setter."""
    cur = _make_cursor_with_check_same_thread_false()
    result: list[BaseException | None] = []

    def factory(_c: object, _r: object) -> object:
        return {}

    def from_other_thread() -> None:
        try:
            cur.row_factory = factory
            result.append(None)
        except BaseException as e:
            result.append(e)

    t = threading.Thread(target=from_other_thread)
    t.start()
    t.join()

    assert result == [None]
    assert cur._row_factory is factory


def test_arraysize_setter_raises_from_foreign_thread_under_csf_true() -> None:
    """Regression: under check_same_thread=True (default), the setter raises."""
    import os

    from dqlitedbapi.exceptions import ProgrammingError

    conn = Connection.__new__(Connection)
    conn._check_same_thread = True
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._closed = False
    cursor = Cursor.__new__(Cursor)
    cursor._connection = conn
    cursor._closed = False
    cursor._arraysize = 1
    cursor._row_factory = None
    cursor.messages = []

    result: list[BaseException | None] = []

    def from_other_thread() -> None:
        try:
            cursor.arraysize = 50
            result.append(None)
        except BaseException as e:
            result.append(e)

    t = threading.Thread(target=from_other_thread)
    t.start()
    t.join()

    assert result and isinstance(result[0], ProgrammingError), (
        f"under check_same_thread=True, foreign-thread setter must raise "
        f"ProgrammingError; got: {result!r}"
    )


_ = pytest  # suppress unused-import lint
