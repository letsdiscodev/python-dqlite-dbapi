"""Pin: PEP 249 §6.1.1 (messages cleared prior to the call) holds on the cross-thread-rejection
path of the sync cursor's secondary methods (setinputsizes/setoutputsize/callproc/nextset/scroll)
— each must clear ``messages`` BEFORE ``_check_thread()`` raises ProgrammingError."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest

from dqlitedbapi import Connection, ProgrammingError

_STALE_CURSOR: tuple[type[Exception], Exception] = (Warning, Warning("stale-cursor"))
_STALE_CONN: tuple[type[Exception], Exception] = (Warning, Warning("stale-conn"))


def _seed(cur: Any) -> None:
    """Seed both cursor- and connection-level ``messages`` so we can observe the clear."""
    cur.messages.append(_STALE_CURSOR)
    cur._connection.messages.append(_STALE_CONN)


def _expect_messages_cleared_after_cross_thread_call(invoke: Callable[[], None], cur: Any) -> None:
    """Run ``invoke`` from a foreign thread, expect ProgrammingError, assert messages cleared."""
    errors: list[BaseException] = []

    def _runner() -> None:
        try:
            invoke()
        except BaseException as e:
            errors.append(e)

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    assert errors, "expected ProgrammingError from cross-thread call"
    assert isinstance(errors[0], ProgrammingError), (
        f"expected ProgrammingError, got {type(errors[0]).__name__}"
    )
    assert list(cur.messages) == [], "Cursor.messages must be cleared before _check_thread raises"
    # Connection.messages and Cursor.messages are independent; cursor methods must not touch it.
    assert list(cur._connection.messages) == [_STALE_CONN]


@pytest.fixture
def cursor() -> Any:
    conn = Connection("127.0.0.1:9001")
    return conn.cursor()


def test_setinputsizes_clears_messages_before_thread_check(cursor: Any) -> None:
    _seed(cursor)
    _expect_messages_cleared_after_cross_thread_call(lambda: cursor.setinputsizes([None]), cursor)


def test_setoutputsize_clears_messages_before_thread_check(cursor: Any) -> None:
    _seed(cursor)
    _expect_messages_cleared_after_cross_thread_call(lambda: cursor.setoutputsize(64), cursor)


def test_callproc_clears_messages_before_thread_check(cursor: Any) -> None:
    _seed(cursor)
    _expect_messages_cleared_after_cross_thread_call(lambda: cursor.callproc("p"), cursor)


def test_nextset_clears_messages_before_thread_check(cursor: Any) -> None:
    _seed(cursor)
    _expect_messages_cleared_after_cross_thread_call(lambda: cursor.nextset(), cursor)


def test_scroll_clears_messages_before_thread_check(cursor: Any) -> None:
    _seed(cursor)
    _expect_messages_cleared_after_cross_thread_call(lambda: cursor.scroll(1), cursor)
