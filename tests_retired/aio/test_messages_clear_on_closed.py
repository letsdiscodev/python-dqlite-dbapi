"""PEP 249 §6.1.1: ``Connection.messages`` is cleared by every standard
method before the call — pin that async commit/rollback clear even on
the closed-connection (raising) branch."""

from __future__ import annotations

import pytest

from dqlitedbapi import InterfaceError
from dqlitedbapi.aio import AsyncConnection


async def test_async_commit_clears_messages_when_closed() -> None:
    conn = AsyncConnection("localhost:9001")
    conn.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    conn._closed = True

    with pytest.raises(InterfaceError):
        await conn.commit()

    assert list(conn.messages) == []


async def test_async_rollback_clears_messages_when_closed() -> None:
    conn = AsyncConnection("localhost:9001")
    conn.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    conn._closed = True

    with pytest.raises(InterfaceError):
        await conn.rollback()

    assert list(conn.messages) == []


async def test_async_commit_clears_messages_when_never_connected() -> None:
    conn = AsyncConnection("localhost:9001")
    conn.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    # Never-connected branch returns silently, but must still clear messages.
    assert conn._async_conn is None
    await conn.commit()
    assert list(conn.messages) == []


async def test_async_rollback_clears_messages_when_never_connected() -> None:
    conn = AsyncConnection("localhost:9001")
    conn.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    assert conn._async_conn is None
    await conn.rollback()
    assert list(conn.messages) == []
