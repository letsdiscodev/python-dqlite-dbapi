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
from unittest.mock import MagicMock

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


def test_state_lock_release_between_transaction_and_cursor() -> None:
    """``_state_lock`` is RELEASED before ``cursor.execute("BEGIN")``
    so the wire round-trip doesn't hold the lock. Sibling threads
    can observe the reserved owner slot AND proceed to acquire the
    lock themselves (to see the reservation and raise).

    Verify by inspection: the ``with _state_lock:`` block in
    ``Connection.transaction`` covers only the owner-check and
    owner-reserve, not the subsequent ``cursor.execute("BEGIN")``
    that runs through ``_op_lock``."""
    import inspect
    import textwrap

    import dqlitedbapi.connection as conn_mod

    src = textwrap.dedent(inspect.getsource(conn_mod.Connection.transaction))
    # The function's body contains a ``with _state_lock:`` block;
    # the actual BEGIN call should appear AFTER (outside) that
    # block — they're at the same indent inside the outer try, so
    # the ``with`` block exits before BEGIN runs. Parse the AST so
    # the test isn't fooled by comment text that mentions
    # cursor.execute("BEGIN").
    import ast

    tree = ast.parse(src)
    func = tree.body[0]
    assert isinstance(func, ast.FunctionDef)

    # Find the FIRST With node whose context is _state_lock — the
    # owner-check/owner-reserve block at the head of the ctxmgr.
    # Subsequent ``with _state_lock:`` blocks around COMMIT/ROLLBACK
    # park the _OWNER_INTERNAL_BUSY sentinel and are intentionally
    # outside the BEGIN flow, so they should NOT be picked here.
    state_lock_with_line: int | None = None
    begin_call_line: int | None = None
    for node in ast.walk(func):
        if isinstance(node, ast.With):
            for item in node.items:
                ctx = item.context_expr
                if (
                    isinstance(ctx, ast.Name)
                    and ctx.id == "_state_lock"
                    and (state_lock_with_line is None or node.lineno < state_lock_with_line)
                ):
                    state_lock_with_line = node.lineno
        if isinstance(node, ast.Call):
            # Match cursor.execute("BEGIN", ...) calls (the actual
            # method call, not a string in a comment).
            f = node.func
            if (
                isinstance(f, ast.Attribute)
                and f.attr == "execute"
                and isinstance(f.value, ast.Name)
                and f.value.id == "cursor"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "BEGIN"
            ):
                begin_call_line = node.lineno

    assert state_lock_with_line is not None, "expected `with _state_lock:` in transaction()"
    assert begin_call_line is not None, 'expected `cursor.execute("BEGIN")` in transaction()'
    assert begin_call_line > state_lock_with_line, (
        f"BEGIN call (line {begin_call_line}) must come AFTER the "
        f"_state_lock release (line {state_lock_with_line}) so the "
        f"lock isn't held across the wire round-trip"
    )
