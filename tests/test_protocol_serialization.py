"""Protocol operation serialization: the wire is single-request-at-a-time per
connection, so concurrent operations must serialize to avoid stream corruption."""

import asyncio
import threading
from unittest.mock import AsyncMock, patch

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor


class TestAsyncProtocolSerialization:
    async def test_concurrent_execute_is_serialized(self) -> None:
        """Two concurrent execute() calls must not overlap on the wire (_op_lock)."""
        conn = AsyncConnection("localhost:9001")

        call_log: list[tuple[str, str]] = []  # (operation, phase) pairs

        async def mock_query_raw_typed(sql: str, params: object) -> tuple:  # type: ignore[type-arg]
            call_log.append((sql, "start"))
            await asyncio.sleep(0.05)
            call_log.append((sql, "end"))
            return (["id"], [1], [[1]], [[1]])  # names, col_types, row_types, rows

        async def mock_execute(sql: str, params: object) -> tuple:  # type: ignore[type-arg]
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

            await asyncio.gather(
                cursor1.execute("SELECT 1"),
                cursor2.execute("INSERT INTO t VALUES (1)"),
            )

            # Serialized: start/end of one op, then the other (not interleaved).
            assert len(call_log) == 4
            assert call_log[0][1] == "start"
            assert call_log[1][1] == "end"
            assert call_log[2][1] == "start"
            assert call_log[3][1] == "end"


class TestSyncProtocolSerialization:
    def test_cross_thread_execute_raises_programming_error(self) -> None:
        """Cross-thread use of a shared connection raises ProgrammingError (like sqlite3)."""
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
