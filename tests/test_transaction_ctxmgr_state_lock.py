"""``transaction()``'s read-check-RESERVE of ``_transaction_owner`` is atomic under
``_state_lock`` — without it two ``check_same_thread=False`` threads both pass the ``is not
None`` check and the loser hits a server-side OperationalError instead of a local
InterfaceError."""

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
    """Two ``check_same_thread=False`` threads racing entry: one reserves, the other raises a
    local ``InterfaceError`` (not the wire's ``OperationalError``)."""
    conn = _make_conn(check_same_thread=False)

    # BEGIN sleeps so the second thread races into the lock and sees the reservation.
    def slow_execute(sql: str, params: object = None) -> None:
        if sql == "BEGIN":
            time.sleep(0.05)
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
        barrier.wait()
        worker(label)

    t1 = threading.Thread(target=runner, args=("A",))
    t2 = threading.Thread(target=runner, args=("B",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(results["won"]) == 1
    assert len(results["raised"]) == 1
    raised_msg = str(results["raised"][0][1])
    assert "Nested" in raised_msg or "owner" in raised_msg.lower()


def test_sequential_transactions_both_succeed_under_flag() -> None:
    """``check_same_thread=False``: sequential transaction blocks both succeed."""
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
    assert conn._transaction_owner is None


def test_begin_failure_clears_owner_slot() -> None:
    """If ``BEGIN`` raises, the outer finally clears the slot — reservation happens before
    BEGIN, so without the clear a failed BEGIN would pin the slot forever."""
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

    assert conn._transaction_owner is None


def test_default_true_unchanged_nested_raises_same_thread() -> None:
    """Backward compat: under default ``check_same_thread=True`` same-thread nesting still
    raises ``InterfaceError("Nested ...")``."""
    conn = _make_conn()

    mock_cursor = MagicMock()
    mock_cursor.close = MagicMock()
    conn.cursor = MagicMock(return_value=mock_cursor)

    with conn.transaction(), pytest.raises(InterfaceError, match="Nested"), conn.transaction():
        pass


def test_state_lock_attribute_exists_on_connection() -> None:
    """``_state_lock`` is a threading.Lock on every Connection instance."""
    conn = _make_conn()
    assert isinstance(conn._state_lock, type(threading.Lock()))


def test_state_lock_release_between_transaction_and_cursor() -> None:
    """``_state_lock`` is RELEASED before ``cursor.execute("BEGIN")`` so the wire round-trip
    doesn't hold it — verified via AST so comment text mentioning BEGIN can't fool the check."""
    import inspect
    import textwrap

    import dqlitedbapi.connection as conn_mod

    src = textwrap.dedent(inspect.getsource(conn_mod.Connection.transaction))
    import ast

    tree = ast.parse(src)
    func = tree.body[0]
    assert isinstance(func, ast.FunctionDef)

    # First With on _state_lock is the owner-check/reserve block; the COMMIT/ROLLBACK
    # sentinel-park blocks are intentionally outside the BEGIN flow and must not be picked.
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
