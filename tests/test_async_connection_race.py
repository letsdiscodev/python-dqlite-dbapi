"""Tests for race condition in async connection initialization."""

import asyncio
from unittest.mock import AsyncMock, patch

from dqlitedbapi.aio.connection import AsyncConnection


class TestAsyncConnectionRace:
    async def test_concurrent_ensure_connection_waits_for_connect(self) -> None:
        """A second _ensure_connection() call must not return before connect() finishes."""
        conn = AsyncConnection("localhost:9001")

        connect_started = asyncio.Event()
        connect_finished = asyncio.Event()

        async def slow_connect() -> None:
            connect_started.set()
            await asyncio.sleep(0.1)
            connect_finished.set()

        with patch("dqlitedbapi.connection.DqliteConnection") as MockDqliteConn:
            mock_instance = AsyncMock()
            mock_instance.connect = slow_connect
            mock_instance._protocol = AsyncMock()
            mock_instance._db_id = 0
            MockDqliteConn.return_value = mock_instance

            async def second_caller() -> bool:
                await connect_started.wait()
                await conn._ensure_connection()
                return connect_finished.is_set()

            first_task = asyncio.create_task(conn._ensure_connection())
            second_task = asyncio.create_task(second_caller())

            await first_task
            connect_was_finished = await second_task

            assert connect_was_finished, "Second caller got connection before connect() finished"
