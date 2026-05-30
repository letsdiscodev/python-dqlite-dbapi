"""Sync ``transaction()`` ctxmgr token-bookkeeping discipline (mock-only unit pins)."""

from __future__ import annotations

import os
import threading
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


def _bare_connection() -> Connection:
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = MagicMock()  # truthy but unused; cursor() is mocked
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn._transaction_owner = None
    conn.messages = []
    return conn


def test_sync_transaction_baseexception_clears_owner_before_rollback() -> None:
    """When the body raises, the owner slot is parked at ``_OWNER_INTERNAL_BUSY`` BEFORE
    ``ROLLBACK`` runs so the cursor guard can't trip and no tier-2 sibling sees a free slot."""
    from dqlitedbapi.connection import _OWNER_INTERNAL_BUSY

    conn = _bare_connection()
    cursor = MagicMock()

    observed: list[object] = []

    def execute_side_effect(sql: str) -> None:
        if sql == "ROLLBACK":
            observed.append(conn._transaction_owner)

    cursor.execute.side_effect = execute_side_effect
    conn.cursor = MagicMock(return_value=cursor)

    class _BodyError(Exception):
        pass

    with pytest.raises(_BodyError), conn.transaction():
        raise _BodyError("synthetic")

    assert observed == [_OWNER_INTERNAL_BUSY], (
        "Expected owner slot to be parked at _OWNER_INTERNAL_BUSY at "
        f"ROLLBACK dispatch; observed transitions: {observed!r}"
    )


def test_sync_transaction_baseexception_restores_owner_then_finally_clears() -> None:
    """The BaseException arm restores the slot to the token, then the outer finally clears it
    via the ``== token`` guard — observed end-state at cursor.close() is ``None``."""
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

    assert close_observed == [None]
    assert conn._transaction_owner is None


def test_sync_transaction_finally_swallows_cursor_close_exception() -> None:
    """The outermost finally suppresses ``cursor.close()`` exceptions so a misbehaving close
    can't mask the user's clean exit or poison post-context state."""
    conn = _bare_connection()
    cursor = MagicMock()
    cursor.close.side_effect = OSError("simulated close failure")
    conn.cursor = MagicMock(return_value=cursor)

    # Clean exit must not surface the close() OSError; the suppress arm absorbs it.
    with conn.transaction():
        pass
    assert conn._transaction_owner is None


def test_sync_transaction_begin_failure_leaves_owner_unset() -> None:
    """If ``BEGIN`` raises, the body is never entered and the owner slot stays clear (the
    ``_transaction_owner = token`` line lives AFTER the BEGIN execute)."""
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
