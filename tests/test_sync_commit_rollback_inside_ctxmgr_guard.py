"""Sync commit()/rollback() stray-inside-``conn.transaction()`` guard: getattr fallback,
foreign-thread-token skip, and closed-check-precedes-ctxmgr-guard ordering."""

from __future__ import annotations

import os
import threading

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import InterfaceError


def _bare_connection_without_tx_owner() -> Connection:
    """Connection missing ``_transaction_owner`` slot, to exercise the getattr fallback."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._async_conn = None  # short-circuit before any wire I/O
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn.messages = []
    # Deliberately do NOT set _transaction_owner.
    return conn


def _bare_connection_with_owner(owner: int | None) -> Connection:
    conn = _bare_connection_without_tx_owner()
    conn._transaction_owner = owner
    return conn


def test_commit_inside_ctxmgr_guard_uses_getattr_fallback() -> None:
    """No ``_transaction_owner`` slot: guard short-circuits silently, no AttributeError."""
    conn = _bare_connection_without_tx_owner()
    conn.commit()


def test_commit_inside_ctxmgr_guard_skips_for_foreign_thread_token() -> None:
    """A foreign-thread ``_transaction_owner`` must not trip the guard."""
    foreign = threading.get_ident() + 1
    conn = _bare_connection_with_owner(foreign)
    conn.commit()


def test_commit_inside_ctxmgr_guard_runs_after_closed_check() -> None:
    """Closed conn owned by current thread: the closed diagnostic wins over the ctxmgr guard."""
    conn = _bare_connection_with_owner(threading.get_ident())
    conn._closed = True
    with pytest.raises(InterfaceError, match="Connection is closed"):
        conn.commit()


def test_commit_inside_ctxmgr_owner_match_raises_interface_error() -> None:
    conn = _bare_connection_with_owner(threading.get_ident())
    with pytest.raises(
        InterfaceError,
        match="commit\\(\\) cannot be issued inside conn.transaction\\(\\)",
    ):
        conn.commit()


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
