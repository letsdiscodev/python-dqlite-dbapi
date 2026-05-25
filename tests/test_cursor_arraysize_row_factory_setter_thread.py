"""Pin: under ``check_same_thread=False``, ``Cursor.arraysize.setter``
and ``Cursor.row_factory.setter`` delegate to
``Connection._check_thread`` which short-circuits — by design.
``check_same_thread=False`` is the explicit Connection-sharing
opt-in; cursor-sharing has no separate flag, so the cursor-per-
thread sub-contract becomes the caller's responsibility under
that relaxation.

This pin documents the behaviour so a future stricter cursor-
layer thread check (which would re-impose what the user opted
out of) is caught here as a regression.
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
    """Under check_same_thread=False, a foreign-thread setter call
    must NOT raise — the documented behaviour the user opted into.
    """
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
    """Regression: under check_same_thread=True (default), a foreign
    thread setter call DOES raise.
    """
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


# Suppress unused-pytest lint nag.
_ = pytest
