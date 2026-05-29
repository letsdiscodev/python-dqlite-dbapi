"""Pin: ``AsyncConnection.transaction()`` sets ``_transaction_owner`` INSIDE
the ``try:`` frame, and ``close()`` clears the slot as a backstop, so a
BaseException at the assignment site cannot leak the slot to a dying task.
Mirrors the ``_executing_task`` set-inside-try fix in ``cursor.py``."""

from __future__ import annotations

import asyncio

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_transaction_owner_assignment_inside_try_frame_source_pin() -> None:
    """Source-level pin: ``self._transaction_owner = token`` is INSIDE a
    ``try:`` frame. The bytecode-boundary race can't be injected from Python,
    so we pin the source structure instead."""
    import ast
    import inspect
    import textwrap

    from dqlitedbapi.aio import connection as conn_mod

    src = textwrap.dedent(inspect.getsource(conn_mod.AsyncConnection.transaction))
    tree = ast.parse(src)

    def find_owner_assign_in_try(node: ast.AST) -> bool:
        for child in ast.walk(node):
            if isinstance(child, ast.Try):
                for stmt in child.body:
                    if isinstance(stmt, ast.Assign):
                        for tgt in stmt.targets:
                            if isinstance(tgt, ast.Attribute) and tgt.attr == "_transaction_owner":
                                return True
        return False

    assert find_owner_assign_in_try(tree), (
        "AsyncConnection.transaction() must set self._transaction_owner "
        "INSIDE a try: frame so the finally clears the slot under any "
        "BaseException that arrives at the assignment site."
    )


async def test_transaction_owner_cleared_after_body_baseexception() -> None:
    """A BaseException out of the body lets the finally clear the slot."""
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        await cur.execute("SELECT 1")
        cur.close()

        with pytest.raises(KeyboardInterrupt):
            async with conn.transaction():
                raise KeyboardInterrupt()
        assert conn._transaction_owner is None
        async with conn.transaction():
            cur = conn.cursor()
            await cur.execute("SELECT 1")
            cur.close()
    finally:
        await conn.close()


async def test_close_clears_transaction_owner_as_backstop() -> None:
    """Even if a future bug pins the slot, close() clears it for instance reuse."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    await cur.execute("SELECT 1")
    cur.close()
    # Synthetic stale pin — simulates a leaked slot from a prior signal-window race.
    conn._transaction_owner = asyncio.current_task()
    await conn.close()
    assert conn._transaction_owner is None


def test_close_clear_runs_after_underlying_teardown_source_pin() -> None:
    """Source-level pin: ``self._transaction_owner = None`` is NOT in close()'s
    synchronous prologue (before teardown), else the concurrent-task race
    returns — the clear races with tasks holding the slot via transaction()."""
    import inspect
    import textwrap

    from dqlitedbapi.aio import connection as conn_mod

    src = textwrap.dedent(inspect.getsource(conn_mod.AsyncConnection.close))
    # Assert no clear precedes the prologue's ``del self.messages[:]``.
    del_marker = src.index("del self.messages[:]")
    prologue = src[: del_marker + len("del self.messages[:]")]
    assert "self._transaction_owner = None" not in prologue, (
        "AsyncConnection.close() must not clear _transaction_owner in "
        "the synchronous prologue — the clear races with concurrent "
        "tasks holding the slot via transaction(). Defer until after "
        "the underlying close has completed."
    )


async def test_nested_transaction_still_rejected_regression_guard() -> None:
    """The set-inside-try refactor must not weaken the nested-transaction guard."""
    conn = AsyncConnection("localhost:9001")
    try:
        async with conn.transaction():
            with pytest.raises(dqlitedbapi.InterfaceError, match="Nested"):
                async with conn.transaction():
                    pass
    finally:
        await conn.close()
