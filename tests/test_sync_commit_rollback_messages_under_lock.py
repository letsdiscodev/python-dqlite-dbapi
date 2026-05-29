"""Sync commit/rollback clear ``messages`` both pre-lock and inside ``_op_lock``: the
in-lock clear defends the window where a sibling could append between pre-lock clear
and the COMMIT round-trip (PEP 249 §6.1.1)."""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock, MagicMock

from dqlitedbapi.connection import Connection


def _build_conn_with_mocked_async() -> Connection:
    """Build a Connection wiring only the fields commit/rollback_async touch."""
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
        conn.messages.append((RuntimeError, RuntimeError("synthetic")))
        asyncio.run(conn._commit_async())
        assert conn.messages == []

    def test_commit_async_clears_messages_before_execute(self) -> None:
        """The clear runs before ``execute("COMMIT")`` so a failing round-trip still
        leaves ``messages`` empty (PEP 249 "cleared prior to executing the call")."""
        conn = _build_conn_with_mocked_async()
        conn.messages.append((RuntimeError, RuntimeError("stale")))
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
        conn.messages.append((RuntimeError, RuntimeError("synthetic")))
        asyncio.run(conn._rollback_async())
        assert conn.messages == []

    def test_rollback_async_clears_messages_before_execute(self) -> None:
        conn = _build_conn_with_mocked_async()
        conn.messages.append((RuntimeError, RuntimeError("stale")))
        from dqlitedbapi import OperationalError

        conn._async_conn.execute = AsyncMock(  # type: ignore[union-attr]
            side_effect=OperationalError("boom")
        )
        with contextlib.suppress(OperationalError):
            asyncio.run(conn._rollback_async())
        assert conn.messages == []
