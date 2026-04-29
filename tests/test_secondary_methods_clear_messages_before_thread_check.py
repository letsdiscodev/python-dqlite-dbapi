"""Pin: PEP 249 §6.1.1's "messages cleared automatically by all standard
cursor method calls (prior to executing the call)" must hold even on
the cross-thread-rejection path of the sync cursor's secondary
methods (``setinputsizes`` / ``setoutputsize`` / ``callproc`` /
``nextset`` / ``scroll``).

The primary methods (``execute`` / ``executemany`` / ``fetchone`` /
``fetchmany`` / ``fetchall`` / ``close``) all clear ``messages``
BEFORE invoking ``_check_thread()`` — so the contract holds even
when the cross-thread guard rejects the call. Before this fix, the
five secondary methods invoked ``_check_thread()`` first; the clear
was reachable only on the well-formed path. Severity is low because
``messages`` is empty in practice today (per ISSUE-644), but a future
code path that begins populating ``messages`` would silently retain
stale entries on the rejected cross-thread path.

This module pins the ordering symmetry: from a non-creator thread,
each secondary method must clear ``messages`` BEFORE raising
``ProgrammingError``.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

import pytest

from dqlitedbapi import Connection, ProgrammingError


def _seed(cur: Any) -> None:
    """Seed both cursor- and connection-level ``messages`` lists so
    we can observe the clear."""
    cur.messages.append((Warning, "stale-cursor"))
    cur._connection.messages.append((Warning, "stale-conn"))


def _expect_messages_cleared_after_cross_thread_call(invoke: Callable[[], None], cur: Any) -> None:
    """Run ``invoke`` from a foreign thread, expect
    ``ProgrammingError``, then assert messages were cleared."""
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
    # The PEP 249 §6.1.1 contract: messages cleared "prior to
    # executing the call" — must hold on the rejected path.
    assert list(cur.messages) == [], "Cursor.messages must be cleared before _check_thread raises"
    assert list(cur._connection.messages) == [], (
        "Connection.messages must be cleared before _check_thread raises"
    )


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
