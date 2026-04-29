"""Pin: sync ``Connection.commit`` / ``rollback`` clear ``messages``
both pre-lock AND inside ``_op_lock`` (mirroring the async sibling).

PEP 249 §6.1.1 says ``Connection.messages`` is cleared by every
method call on the connection. Clearing only pre-lock leaves a
window where a sibling thread can append directly to
``conn.messages`` between the pre-lock clear and the COMMIT round-
trip; the in-lock clear in ``_commit_async`` / ``_rollback_async``
(which run on the loop thread inside ``_run_sync``'s ``_op_lock``
critical section) defends that window and matches the
already-hardened async surface.
"""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.connection import Connection


def _build_conn_with_mocked_async() -> Connection:
    """Build a Connection with no real socket: bypass ``__init__`` and
    wire only the fields ``_commit_async`` / ``_rollback_async`` touch.
    """
    conn = Connection.__new__(Connection)
    conn.messages = []
    fake = MagicMock()
    fake.execute = AsyncMock(return_value=(0, 0))
    fake.in_transaction = True
    conn._async_conn = fake
    return conn


class TestCommitAsyncClearsMessagesInLock:
    def test_commit_async_clears_messages(self) -> None:
        conn = _build_conn_with_mocked_async()
        conn.messages.append((RuntimeError, "synthetic"))
        asyncio.run(conn._commit_async())
        assert conn.messages == []

    def test_commit_async_clears_messages_before_execute(self) -> None:
        """The clear runs before ``execute("COMMIT")`` so even an
        exception from the wire round-trip leaves ``messages`` empty
        — consistent with PEP 249 "cleared prior to executing the
        call"."""
        conn = _build_conn_with_mocked_async()
        conn.messages.append((RuntimeError, "stale"))
        # Make the COMMIT raise; messages should still be cleared.
        from dqlitedbapi import OperationalError

        conn._async_conn.execute = AsyncMock(  # type: ignore[union-attr]
            side_effect=OperationalError("boom")
        )
        with contextlib.suppress(OperationalError):
            asyncio.run(conn._commit_async())
        assert conn.messages == []


class TestRollbackAsyncClearsMessagesInLock:
    def test_rollback_async_clears_messages(self) -> None:
        conn = _build_conn_with_mocked_async()
        conn.messages.append((RuntimeError, "synthetic"))
        asyncio.run(conn._rollback_async())
        assert conn.messages == []

    def test_rollback_async_clears_messages_before_execute(self) -> None:
        conn = _build_conn_with_mocked_async()
        conn.messages.append((RuntimeError, "stale"))
        from dqlitedbapi import OperationalError

        conn._async_conn.execute = AsyncMock(  # type: ignore[union-attr]
            side_effect=OperationalError("boom")
        )
        with contextlib.suppress(OperationalError):
            asyncio.run(conn._rollback_async())
        assert conn.messages == []
