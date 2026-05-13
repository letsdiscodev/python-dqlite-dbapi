"""Pin: sync ``Connection.commit()`` / ``Connection.rollback()`` stray-
inside-``conn.transaction()`` guard discipline.

The integration test
``tests/integration/test_sync_transaction_ctxmgr_rejects_stray_commit``
covers the happy-path rejection only. Three load-bearing sub-
invariants in the guard at ``connection.py:2032-2038`` (commit) and
``connection.py:2116-2123`` (rollback) lack mock-only pins:

1. ``getattr`` fallback — Connection.__new__-built stubs without
   ``_transaction_owner`` must NOT crash with AttributeError.
2. Thread-id mismatch path — a token from a different thread does
   NOT trip the guard.
3. Pre-check ordering — the closed-state diagnostic precedes the
   ctxmgr-owns-boundaries diagnostic so the operator-facing primary
   error surfaces first.

The async sibling has equivalent mock-only pins; this is symmetric.
"""

from __future__ import annotations

import os
import threading

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import InterfaceError


def _bare_connection_without_tx_owner() -> Connection:
    """Connection built via __new__ deliberately MISSING the
    ``_transaction_owner`` slot, to exercise the getattr fallback."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = None  # short-circuit before any wire I/O
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn.messages = []
    # Intentionally do NOT set _transaction_owner; tests must not
    # trip an AttributeError.
    return conn


def _bare_connection_with_owner(owner: int | None) -> Connection:
    conn = _bare_connection_without_tx_owner()
    conn._transaction_owner = owner
    return conn


# --- commit() ----------------------------------------------------------------


def test_commit_inside_ctxmgr_guard_uses_getattr_fallback() -> None:
    """No ``_transaction_owner`` slot present: ``getattr`` returns
    ``None`` and the guard short-circuits silently — no
    AttributeError, no InterfaceError."""
    conn = _bare_connection_without_tx_owner()
    # _async_conn is None → returns silently (no spurious connect).
    conn.commit()


def test_commit_inside_ctxmgr_guard_skips_for_foreign_thread_token() -> None:
    """``_transaction_owner`` set to a foreign thread id must NOT
    trip the guard. Defended by ``_check_thread`` further down today,
    but the explicit ``threading.get_ident()`` comparison is the
    guard's own discipline arm and is pinned here."""
    foreign = threading.get_ident() + 1
    conn = _bare_connection_with_owner(foreign)
    # Must not raise — guard skips on thread-id mismatch.
    conn.commit()


def test_commit_inside_ctxmgr_guard_runs_after_closed_check() -> None:
    """Closed AND _transaction_owner = current thread: the
    closed-state diagnostic must win — pin the ordering so a future
    refactor that moves the ctxmgr guard above the closed check
    surfaces as a test failure."""
    conn = _bare_connection_with_owner(threading.get_ident())
    conn._closed = True
    with pytest.raises(InterfaceError, match="Connection is closed"):
        conn.commit()


def test_commit_inside_ctxmgr_owner_match_raises_interface_error() -> None:
    """Positive pin: when the guard's conditions ARE all met, the
    documented InterfaceError fires with the ctxmgr-owns-boundaries
    message."""
    conn = _bare_connection_with_owner(threading.get_ident())
    with pytest.raises(
        InterfaceError,
        match="commit\\(\\) cannot be issued inside conn.transaction\\(\\)",
    ):
        conn.commit()


# --- rollback() --------------------------------------------------------------


def test_rollback_inside_ctxmgr_guard_uses_getattr_fallback() -> None:
    conn = _bare_connection_without_tx_owner()
    conn.rollback()


def test_rollback_inside_ctxmgr_guard_skips_for_foreign_thread_token() -> None:
    foreign = threading.get_ident() + 1
    conn = _bare_connection_with_owner(foreign)
    conn.rollback()


def test_rollback_inside_ctxmgr_guard_runs_after_closed_check() -> None:
    conn = _bare_connection_with_owner(threading.get_ident())
    conn._closed = True
    with pytest.raises(InterfaceError, match="Connection is closed"):
        conn.rollback()


def test_rollback_inside_ctxmgr_owner_match_raises_interface_error() -> None:
    conn = _bare_connection_with_owner(threading.get_ident())
    with pytest.raises(
        InterfaceError,
        match="rollback\\(\\) cannot be issued inside conn.transaction\\(\\)",
    ):
        conn.rollback()
