"""PEP 249 §6.1.1 says ``Connection.messages`` is cleared by every
standard connection method "prior to executing the call".

The sync sibling (``Connection.commit/rollback``) clears unconditionally
as the very first statement. The async siblings used to raise
``InterfaceError("Connection is closed")`` without clearing —
violating the contract on the closed-connection path. Pin that the
clear now happens regardless of which branch executes.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import InterfaceError
from dqlitedbapi.aio import AsyncConnection


@pytest.mark.asyncio
async def test_async_commit_clears_messages_when_closed() -> None:
    conn = AsyncConnection("localhost:9001")
    conn.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    conn._closed = True

    with pytest.raises(InterfaceError):
        await conn.commit()

    assert list(conn.messages) == []


@pytest.mark.asyncio
async def test_async_rollback_clears_messages_when_closed() -> None:
    conn = AsyncConnection("localhost:9001")
    conn.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    conn._closed = True

    with pytest.raises(InterfaceError):
        await conn.rollback()

    assert list(conn.messages) == []


@pytest.mark.asyncio
async def test_async_commit_clears_messages_when_never_connected() -> None:
    conn = AsyncConnection("localhost:9001")
    conn.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    # _async_conn stays None until first use — commit returns silently
    # in this branch, but the messages clear must still happen.
    assert conn._async_conn is None
    await conn.commit()
    assert list(conn.messages) == []


@pytest.mark.asyncio
async def test_async_rollback_clears_messages_when_never_connected() -> None:
    conn = AsyncConnection("localhost:9001")
    conn.messages.append(("sentinel", Warning("noop")))  # type: ignore[arg-type]
    assert conn._async_conn is None
    await conn.rollback()
    assert list(conn.messages) == []
