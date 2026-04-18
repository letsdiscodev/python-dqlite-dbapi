"""AsyncConnection.close serializes with in-flight operations (ISSUE-32).

Previously close() called await self._async_conn.close() without
acquiring _op_lock; a concurrent task mid-execute would find the
protocol torn down underneath it. Now close() acquires the lock so
in-flight operations drain cleanly before the socket is closed.
"""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


@pytest.mark.asyncio
async def test_close_waits_for_in_flight_execute() -> None:
    """If task A is mid-execute (holding _op_lock), task B's close
    awaits that task before tearing down the protocol."""
    conn = AsyncConnection("localhost:19001", database="x")

    order: list[str] = []

    async def slow_execute(_sql: str, _params: object) -> tuple[int, int]:
        order.append("execute:start")
        await asyncio.sleep(0.05)
        order.append("execute:end")
        return (0, 0)

    async def fake_query_raw_typed(_sql: str, _params: object) -> tuple[list, list, list]:
        return ([], [], [])

    with patch("dqlitedbapi.aio.connection.DqliteConnection") as MockDqliteConn:
        mock_instance = AsyncMock()
        mock_instance.connect = AsyncMock()
        mock_instance.execute = slow_execute
        mock_instance.query_raw_typed = fake_query_raw_typed

        async def fake_close() -> None:
            order.append("close:start")
            order.append("close:end")

        mock_instance.close = fake_close
        MockDqliteConn.return_value = mock_instance

        await conn.connect()

        async def run_execute() -> None:
            cursor = conn.cursor()
            await cursor.execute("INSERT INTO t VALUES (1)")

        async def run_close() -> None:
            await asyncio.sleep(0.01)  # let execute start first
            await conn.close()

        await asyncio.gather(run_execute(), run_close())

    # execute must complete before close starts tearing down.
    assert order == [
        "execute:start",
        "execute:end",
        "close:start",
        "close:end",
    ], f"actual order: {order}"
