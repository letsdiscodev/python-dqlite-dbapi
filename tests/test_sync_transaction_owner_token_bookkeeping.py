"""Pin: sync ``Connection.transaction()`` ctxmgr token-bookkeeping
discipline.

The sync ``transaction()`` ctxmgr at ``connection.py:2160-2246`` is
covered today only by the integration suite. The async sibling at
``aio/connection.py`` has a mock-only unit-level pin
(``test_async_transaction_owner_set_inside_try_frame``). This file
brings the sync surface to parity by pinning four load-bearing
invariants with mock-only unit tests so a refactor regression lights
up locally rather than only in the cluster fixture:

1. ``self._transaction_owner = token`` is INSIDE the outer ``try:``
   frame (source-level pin, mirrors the async sibling AST pin).
2. The BaseException arm clears the owner slot BEFORE running
   ``ROLLBACK`` so the cursor-layer commit/rollback path does not
   trip the owner-token guard.
3. The BaseException arm restores the slot back to ``token`` before
   re-raising so the outer ``finally``'s ``== token`` guard fires.
4. The outermost ``finally`` swallows any exception raised by
   ``cursor.close()`` (``contextlib.suppress(Exception)``) so the
   user's exception (if any) is not masked.
"""

from __future__ import annotations

import ast
import inspect
import os
import textwrap
import threading
from unittest.mock import MagicMock

import pytest

from dqlitedbapi import connection as conn_mod
from dqlitedbapi.connection import Connection


def _bare_connection() -> Connection:
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = MagicMock()  # truthy but unused (cursor() is mocked)
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._transaction_owner = None
    conn.messages = []
    return conn


def test_sync_transaction_owner_assignment_inside_try_frame_source_pin() -> None:
    """Source-level pin: ``self._transaction_owner = token`` lives
    inside a ``try:`` frame. Mirrors the async sibling's AST pin —
    a ``BaseException`` between the STORE_ATTR and the SETUP_FINALLY
    is impossible to inject from Python, so the structural pin is
    the right contract layer."""
    src = textwrap.dedent(inspect.getsource(conn_mod.Connection.transaction))
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
        "Connection.transaction() must set self._transaction_owner "
        "INSIDE a try: frame so the finally clears the slot under "
        "any BaseException that arrives at the assignment site."
    )


def test_sync_transaction_baseexception_clears_owner_before_rollback() -> None:
    """When the body raises, the owner slot is cleared BEFORE
    ``ROLLBACK`` runs so the cursor-layer commit/rollback guard
    cannot trip on a pinned slot."""
    conn = _bare_connection()
    cursor = MagicMock()

    observed: list[int | None] = []

    def execute_side_effect(sql: str) -> None:
        if sql == "ROLLBACK":
            # Snapshot the slot at the moment ROLLBACK is dispatched.
            observed.append(conn._transaction_owner)

    cursor.execute.side_effect = execute_side_effect
    conn.cursor = MagicMock(return_value=cursor)

    class _BodyError(Exception):
        pass

    with pytest.raises(_BodyError), conn.transaction():
        raise _BodyError("synthetic")

    assert observed == [None], (
        f"Expected owner slot to be None at ROLLBACK dispatch; observed transitions: {observed!r}"
    )


def test_sync_transaction_baseexception_restores_owner_then_finally_clears() -> None:
    """After ``ROLLBACK`` runs the BaseException arm's inner finally
    restores the slot to the token; the outer finally then clears it
    via the ``== token`` guard. End-state observed by the caller is
    ``None`` — pin via cursor.close() side-effect."""
    conn = _bare_connection()
    cursor = MagicMock()

    close_observed: list[int | None] = []

    def close_side_effect() -> None:
        close_observed.append(conn._transaction_owner)

    cursor.close.side_effect = close_side_effect
    conn.cursor = MagicMock(return_value=cursor)

    class _BodyError(Exception):
        pass

    with pytest.raises(_BodyError), conn.transaction():
        raise _BodyError("synthetic")

    # close() runs in the outermost finally AFTER the ``== token``
    # clear; observation should be None.
    assert close_observed == [None]
    # And the post-context state matches.
    assert conn._transaction_owner is None


def test_sync_transaction_finally_swallows_cursor_close_exception() -> None:
    """The outermost ``finally`` uses ``contextlib.suppress(Exception)``
    around ``cursor.close()`` so a misbehaving close cannot mask the
    user's clean exit nor poison the post-context state."""
    conn = _bare_connection()
    cursor = MagicMock()
    cursor.close.side_effect = OSError("simulated close failure")
    conn.cursor = MagicMock(return_value=cursor)

    # No exception — the clean exit should NOT surface the close()
    # OSError; the suppress arm absorbs it.
    with conn.transaction():
        pass
    assert conn._transaction_owner is None


def test_sync_transaction_begin_failure_leaves_owner_unset() -> None:
    """If ``BEGIN`` itself raises, the ctxmgr never enters its body
    and the owner slot stays clear so the caller's error-handling can
    still issue commit/rollback. The ``self._transaction_owner = token``
    line lives AFTER the BEGIN execute, which is the source-level
    contract this test pins."""
    conn = _bare_connection()
    cursor = MagicMock()

    class _BeginFailure(Exception):
        pass

    def execute_side_effect(sql: str) -> None:
        if sql == "BEGIN":
            raise _BeginFailure("synthetic")

    cursor.execute.side_effect = execute_side_effect
    conn.cursor = MagicMock(return_value=cursor)

    with pytest.raises(_BeginFailure), conn.transaction():
        pytest.fail("body should not be entered if BEGIN raises")
    assert conn._transaction_owner is None
