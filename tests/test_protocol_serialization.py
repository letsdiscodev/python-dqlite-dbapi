"""Tests for protocol operation serialization.

The dqlite wire protocol is single-request-at-a-time per connection.
Concurrent protocol operations must be serialized to prevent wire corruption.
"""

import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


class TestAsyncProtocolSerialization:
    """Test that concurrent async operations are serialized."""

    @pytest.mark.asyncio
    async def test_concurrent_execute_is_serialized(self) -> None:
        """Two concurrent execute() calls must not overlap on the wire.

        Without serialization, both coroutines would call query_sql/exec_sql
        concurrently, corrupting the TCP stream. With serialization, they
        must run one after the other.
        """
        conn = AsyncConnection("localhost:9001")

        call_log: list[tuple[str, str]] = []  # (operation, phase) pairs

        async def mock_query_sql(db_id: int, sql: str, params: object) -> tuple:
            call_log.append((sql, "start"))
            await asyncio.sleep(0.05)  # Simulate network I/O
            call_log.append((sql, "end"))
            return (["id"], [[1]])

        async def mock_exec_sql(db_id: int, sql: str, params: object) -> tuple:
            call_log.append((sql, "start"))
            await asyncio.sleep(0.05)
            call_log.append((sql, "end"))
            return (0, 1)

        with patch("dqlitedbapi.aio.connection.DqliteConnection") as MockDqliteConn:
            mock_instance = AsyncMock()
            mock_instance.connect = AsyncMock()
            mock_instance._protocol = MagicMock()
            mock_instance._protocol.query_sql = mock_query_sql
            mock_instance._protocol.exec_sql = mock_exec_sql
            mock_instance._db_id = 0
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
    """Test that concurrent sync operations are serialized."""

    def test_concurrent_run_sync_is_serialized(self) -> None:
        """Two threads calling _run_sync must not overlap on the event loop.

        Without serialization, both threads submit coroutines concurrently
        to the same event loop, where they interleave at await points.
        """
        conn = Connection("localhost:9001", timeout=5.0)

        call_log: list[tuple[str, str]] = []
        log_lock = threading.Lock()

        async def mock_query_sql(db_id: int, sql: str, params: object) -> tuple:
            with log_lock:
                call_log.append((sql, "start"))
            await asyncio.sleep(0.05)
            with log_lock:
                call_log.append((sql, "end"))
            return (["id"], [[1]])

        async def mock_exec_sql(db_id: int, sql: str, params: object) -> tuple:
            with log_lock:
                call_log.append((sql, "start"))
            await asyncio.sleep(0.05)
            with log_lock:
                call_log.append((sql, "end"))
            return (0, 1)

        with patch("dqlitedbapi.connection.DqliteConnection") as MockDqliteConn:
            mock_instance = AsyncMock()
            mock_instance.connect = AsyncMock()
            mock_instance._protocol = MagicMock()
            mock_instance._protocol.query_sql = mock_query_sql
            mock_instance._protocol.exec_sql = mock_exec_sql
            mock_instance._db_id = 0
            MockDqliteConn.return_value = mock_instance

            cursor1 = Cursor(conn)
            cursor2 = Cursor(conn)

            barrier = threading.Barrier(2)
            errors: list[Exception] = []

            def thread_work(cursor: Cursor, sql: str) -> None:
                try:
                    barrier.wait(timeout=5)
                    cursor.execute(sql)
                except Exception as e:
                    errors.append(e)

            t1 = threading.Thread(target=thread_work, args=(cursor1, "SELECT 1"))
            t2 = threading.Thread(target=thread_work, args=(cursor2, "INSERT INTO t VALUES (1)"))
            t1.start()
            t2.start()
            t1.join(timeout=10)
            t2.join(timeout=10)

            assert not errors, f"Threads raised errors: {errors}"

            # Operations must be serialized: one completes before the other starts
            assert len(call_log) == 4
            assert call_log[0][1] == "start"
            assert call_log[1][1] == "end"
            assert call_log[2][1] == "start"
            assert call_log[3][1] == "end"

            conn.close()


class TestSyncConnectionLazyInitRace:
    """Test that _get_async_connection doesn't create duplicate connections."""

    def test_concurrent_first_use_creates_single_connection(self) -> None:
        """Two threads using a connection for the first time must not
        create two underlying DqliteConnection instances."""
        conn = Connection("localhost:9001", timeout=5.0)

        connect_count = 0
        count_lock = threading.Lock()

        async def slow_connect() -> None:
            nonlocal connect_count
            with count_lock:
                connect_count += 1
            await asyncio.sleep(0.05)

        with patch("dqlitedbapi.connection.DqliteConnection") as MockDqliteConn:
            mock_instance = AsyncMock()
            mock_instance.connect = slow_connect
            mock_instance._protocol = MagicMock()
            mock_instance._protocol.query_sql = AsyncMock(return_value=(["id"], [[1]]))
            mock_instance._db_id = 0
            MockDqliteConn.return_value = mock_instance

            barrier = threading.Barrier(2)
            errors: list[Exception] = []

            def thread_work() -> None:
                try:
                    barrier.wait(timeout=5)
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                except Exception as e:
                    errors.append(e)

            t1 = threading.Thread(target=thread_work)
            t2 = threading.Thread(target=thread_work)
            t1.start()
            t2.start()
            t1.join(timeout=10)
            t2.join(timeout=10)

            assert not errors, f"Threads raised errors: {errors}"
            # Only one DqliteConnection should have been created
            assert MockDqliteConn.call_count == 1
            assert connect_count == 1

            conn.close()
