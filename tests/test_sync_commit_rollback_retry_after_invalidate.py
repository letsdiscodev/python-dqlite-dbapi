"""Pin: sync ``Connection.commit()`` / ``rollback()`` raise
``InterfaceError`` when retried against an inner client connection
that was invalidated by a prior cancel-mid-flight.

Sibling of ``test_async_commit_rollback_retry_after_invalidate``.
Without this guard, the retry hit ``getattr(inner, 'in_transaction',
False)`` → False (cleared by ``_invalidate``) → silent return,
hiding the partial-commit ambiguity (the cancelled COMMIT may or may
not have reached the leader). The sync version doesn't need an
in-lock recheck (no async race window — ``_check_thread`` makes the
sync caller single-threaded relative to itself); the pre-check at
the top of ``commit()`` / ``rollback()`` is sufficient.

The new ``InterfaceError("Connection invalidated (id=...)")`` text
mirrors the async sibling's lexeme so SA's ``is_disconnect``
predicate (which keys off the substring) recognises both arms.
"""

from __future__ import annotations

import os
import threading
from unittest.mock import MagicMock

import pytest

import dqlitedbapi.exceptions as _dbapi_exc
from dqlitedbapi import Connection


def _prime_invalidated() -> Connection:
    """Build a sync Connection wrapping an inner client whose
    ``_protocol`` is None (the sentinel for invalidated state)."""
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
    """A retry of commit() against an invalidated inner client conn
    must NOT silently no-op; raise InterfaceError so caller code
    cannot mistakenly treat the retry as a clean commit."""
    conn = _prime_invalidated()
    with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
        conn.commit()


def test_rollback_raises_interface_error_on_invalidated_inner() -> None:
    conn = _prime_invalidated()
    with pytest.raises(_dbapi_exc.InterfaceError, match="invalidated"):
        conn.rollback()


def test_commit_silent_noop_when_async_conn_is_none() -> None:
    """Regression: never-used connection (``_async_conn is None``)
    short-circuits silently. The new invalidation guard must NOT
    fire here."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn.messages = []
    conn._async_conn = None
    conn.commit()  # silent no-op
    conn.rollback()  # silent no-op


def test_commit_silent_noop_when_in_transaction_false_alive() -> None:
    """Regression: alive ``_protocol`` with ``in_transaction=False``
    is the documented PEP 249 / autocommit-by-default no-op path.
    The invalidation guard must NOT misfire on this happy path."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._creator_thread = threading.get_ident()
    conn._creator_pid = os.getpid()
    conn.messages = []
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = object()  # alive
    conn._async_conn.in_transaction = False
    conn.commit()  # silent no-op
    conn.rollback()  # silent no-op
