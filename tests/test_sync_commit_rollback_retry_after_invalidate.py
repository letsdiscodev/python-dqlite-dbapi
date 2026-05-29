"""Sync commit()/rollback() raise InterfaceError when retried against an inner client
invalidated by a prior cancel-mid-flight, rather than silently no-op'ing and hiding the
partial-commit ambiguity. The "invalidated" lexeme matches SA's ``is_disconnect`` substring."""

from __future__ import annotations

import os
import threading
from unittest.mock import MagicMock

import pytest

import dqlitedbapi.exceptions as _dbapi_exc
from dqlitedbapi import Connection


def _prime_invalidated() -> Connection:
    """Sync Connection wrapping an inner client with ``_protocol`` None (invalidated)."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = None  # invalidated
    conn._async_conn.in_transaction = False  # also cleared by invalidate
    return conn


def test_commit_raises_interface_error_on_invalidated_inner() -> None:
    """Retry of commit() against an invalidated inner must raise, not silently no-op."""
    conn = _prime_invalidated()
    with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
        conn.commit()


def test_rollback_raises_interface_error_on_invalidated_inner() -> None:
    conn = _prime_invalidated()
    with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
        conn.rollback()


def test_commit_silent_noop_when_async_conn_is_none() -> None:
    """Never-used connection (``_async_conn is None``) short-circuits silently; the
    invalidation guard must not fire here."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn.messages = []
    conn._async_conn = None
    conn.commit()
    conn.rollback()


def test_commit_silent_noop_when_in_transaction_false_alive() -> None:
    """Alive ``_protocol`` with ``in_transaction=False`` is the autocommit no-op path; the
    invalidation guard must not misfire on it."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = object()  # alive
    conn._async_conn.in_transaction = False
    conn.commit()
    conn.rollback()
