"""Tests for protocol operation serialization.

The dqlite wire protocol is single-request-at-a-time per connection.
Concurrent protocol operations must be serialized to prevent wire corruption.
"""

import asyncio
import threading
from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


class TestAsyncProtocolSerialization:
    """Test that concurrent async operations are serialized."""

    @pytest.mark.asyncio
    async def test_concurrent_execute_is_serialized(self) -> None:
        """Two concurrent execute() calls must not overlap on the wire.

        Without serialization, both coroutines would run concurrently
        on the same protocol socket, corrupting the TCP stream. With
        serialization via _op_lock, they must run one after the other.
        """
        conn = AsyncConnection("localhost:9001")

        call_log: list[tuple[str, str]] = []  # (operation, phase) pairs

        async def mock_query_raw_typed(sql: str, params: object) -> tuple:
            call_log.append((sql, "start"))
            await asyncio.sleep(0.05)  # Simulate network I/O
            call_log.append((sql, "end"))
            return (["id"], [1], [[1]], [[1]])  # names, col_types, row_types, rows

        async def mock_execute(sql: str, params: object) -> tuple:
            call_log.append((sql, "start"))
            await asyncio.sleep(0.05)
            call_log.append((sql, "end"))
            return (0, 1)

        with patch("dqlitedbapi.connection.DqliteConnection") as MockDqliteConn:
            mock_instance = AsyncMock()
            mock_instance.connect = AsyncMock()
            mock_instance.query_raw_typed = mock_query_raw_typed
            mock_instance.execute = mock_execute
            MockDqliteConn.return_value = mock_instance

            await conn.connect()

            cursor1 = conn.cursor()
            cursor2 = conn.cursor()

            # Run two operations concurrently
            await asyncio.gather(
                cursor1.execute("SELECT 1"),
                cursor2.execute("INSERT INTO t VALUES (1)"),
            )

            # With proper serialization, operations must not interleave.
            # The call_log should show: start/end of one op, then start/end of the other.
            # NOT: start/start/end/end (interleaved).
            assert len(call_log) == 4
            # First operation must complete before second starts
            assert call_log[0][1] == "start"
            assert call_log[1][1] == "end"
            assert call_log[2][1] == "start"
            assert call_log[3][1] == "end"


class TestSyncProtocolSerialization:
    """Test that concurrent sync operations from wrong threads are rejected."""

    def test_cross_thread_execute_raises_programming_error(self) -> None:
        """Threads sharing a connection must get ProgrammingError.

        The thread-identity check (like sqlite3) prevents cross-thread
        access before it reaches the protocol layer.
        """
        from dqlitedbapi.exceptions import ProgrammingError

        conn = Connection("localhost:9001", timeout=5.0)
        cursor = Cursor(conn)

        errors: list[Exception] = []

        def thread_work() -> None:
            try:
                cursor.execute("SELECT 1")
            except Exception as e:
                errors.append(e)

        t = threading.Thread(target=thread_work)
        t.start()
        t.join(timeout=5)

        assert len(errors) == 1
        assert isinstance(errors[0], ProgrammingError)
        conn.close()
