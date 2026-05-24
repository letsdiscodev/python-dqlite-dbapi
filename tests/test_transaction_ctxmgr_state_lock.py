"""Pin: ``Connection.transaction()`` ctxmgr's read-check-RESERVE
of ``_transaction_owner`` is atomic under ``_state_lock``.

Under ``check_same_thread=False``, two threads concurrently
entering ``with conn.transaction():`` previously both passed the
``is not None`` check (the read was on line 3328, the write on
line 3345; the cursor() + BEGIN happened in between). The second
thread's BEGIN would then surface a server-side
``OperationalError("cannot start a transaction within a
transaction")``. That's loud but server-emitted; the lock turns it
into a local Python-side ``InterfaceError`` at the offending
caller's frame.

Pinned behaviours:
- Concurrent entry: one thread wins (reserves), the other raises
  ``InterfaceError`` locally with the owner thread id.
- Sequential transactions: both succeed in order.
- BEGIN failure: the outer finally clears the slot so the next
  caller sees it free.
- Same-thread re-entry under default still raises (backward
  compat).
"""

from __future__ import annotations

import threading
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.exceptions import InterfaceError


def _make_conn(**kwargs: object) -> Connection:
    return Connection("localhost:9999", **kwargs)  # type: ignore[arg-type]


def test_concurrent_transaction_entry_one_wins_other_raises_local_interface_error() -> None:
    """Under ``check_same_thread=False``: two threads racing
    ``with conn.transaction():`` — one reserves the slot, the other
    sees the reservation and raises ``InterfaceError`` (local
    Python-side, not the wire's ``OperationalError``)."""
    conn = _make_conn(check_same_thread=False)

    # Mock the cursor + BEGIN so we don't need a wire. The first
    # thread to acquire _state_lock reserves the slot and proceeds
    # to BEGIN; we make BEGIN sleep so the second thread races into
    # the lock and sees the reservation.
    def slow_execute(sql: str, params: object = None) -> None:
        if sql == "BEGIN":
            time.sleep(0.05)  # let the second thread race in
        return None

    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock(side_effect=slow_execute)
    mock_cursor.close = MagicMock()
    conn.cursor = MagicMock(return_value=mock_cursor)

    results: dict[str, Any] = {"won": [], "raised": []}

    def worker(label: str) -> None:
        try:
            with conn.transaction():
                results["won"].append(label)
        except InterfaceError as e:
            results["raised"].append((label, e))

    barrier = threading.Barrier(2)

    def runner(label: str) -> None:
        barrier.wait()  # synchronize start
        worker(label)

    t1 = threading.Thread(target=runner, args=("A",))
    t2 = threading.Thread(target=runner, args=("B",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Exactly one thread should have won; exactly one should have
    # raised a local InterfaceError.
    assert len(results["won"]) == 1
    assert len(results["raised"]) == 1
    # The raised error mentions "Nested" so an operator reading the
    # traceback understands the cause.
    raised_msg = str(results["raised"][0][1])
    assert "Nested" in raised_msg or "owner" in raised_msg.lower()


def test_sequential_transactions_both_succeed_under_flag() -> None:
    """``check_same_thread=False``: sequential ``with
    conn.transaction()`` blocks both succeed. The first clears the
    slot in its finally; the second reserves cleanly."""
    conn = _make_conn(check_same_thread=False)

    mock_cursor = MagicMock()
    mock_cursor.close = MagicMock()
    conn.cursor = MagicMock(return_value=mock_cursor)

    completed: list[str] = []

    def worker(label: str) -> None:
        with conn.transaction():
            completed.append(label)

    t1 = threading.Thread(target=worker, args=("A",))
    t1.start()
    t1.join()
    t2 = threading.Thread(target=worker, args=("B",))
    t2.start()
    t2.join()

    assert completed == ["A", "B"]
    # Slot is clear after both complete.
    assert conn._transaction_owner is None


def test_begin_failure_clears_owner_slot() -> None:
    """If ``BEGIN`` raises, the outer finally clears the slot so
    the next caller sees it free. Reservation-before-BEGIN means
    the slot is set BEFORE BEGIN executes; without the finally
    clear, a failed BEGIN would leave the slot pinned forever."""
    conn = _make_conn(check_same_thread=False)

    def failing_execute(sql: str, params: object = None) -> None:
        if sql == "BEGIN":
            raise RuntimeError("simulated BEGIN failure")
        return None

    mock_cursor = MagicMock()
    mock_cursor.execute = MagicMock(side_effect=failing_execute)
    mock_cursor.close = MagicMock()
    conn.cursor = MagicMock(return_value=mock_cursor)

    with pytest.raises(RuntimeError, match="simulated BEGIN failure"), conn.transaction():
        pass  # unreachable; BEGIN raises before yield

    # Slot is clear.
    assert conn._transaction_owner is None


def test_default_true_unchanged_nested_raises_same_thread() -> None:
    """Backward compat: under default ``check_same_thread=True``,
    same-thread nested ``with conn.transaction()`` still raises
    ``InterfaceError("Nested ...")``. The state lock is uncontended
    on this path."""
    conn = _make_conn()

    mock_cursor = MagicMock()
    mock_cursor.close = MagicMock()
    conn.cursor = MagicMock(return_value=mock_cursor)

    with conn.transaction(), pytest.raises(InterfaceError, match="Nested"), conn.transaction():
        pass


def test_state_lock_attribute_exists_on_connection() -> None:
    """Pin the presence of ``_state_lock`` as a threading.Lock on
    every Connection instance — future Phase 2 issues reuse the
    same lock for autocommit/isolation_level setters."""
    conn = _make_conn()
    assert isinstance(conn._state_lock, type(threading.Lock()))


def test_state_lock_does_not_block_cursor_creation() -> None:
    """``_state_lock`` is held only briefly across the read-check-
    reserve in transaction()'s entry. Other operations
    (``conn.cursor()``, ``conn.commit()`` outside the body) MUST
    NOT acquire the lock and thus MUST NOT block on it."""
    conn = _make_conn(check_same_thread=False)

    # Hold _state_lock manually on the main thread.
    with conn._state_lock:
        # cursor() must succeed without blocking. We patch out the
        # actual cursor construction since this Connection is
        # never-connected.
        with patch.object(Connection, "_check_thread"):
            cur = conn.cursor()
        assert cur is not None
