"""Tests that close() cleans up resources even if async close fails."""

from unittest.mock import AsyncMock, patch

from dqlitedbapi.connection import Connection


class TestCloseResourceLeak:
    def test_close_cleans_up_loop_even_if_async_close_fails(self) -> None:
        """Event loop and thread must be cleaned up even if async close raises."""
        conn = Connection("localhost:9001")

        # Force the connection to create a loop and async connection
        with patch("dqlitedbapi.connection.DqliteConnection") as MockDqliteConn:
            mock_instance = AsyncMock()
            mock_instance.connect = AsyncMock()
            mock_instance._protocol = AsyncMock()
            mock_instance._db_id = 0
            # Make close() raise
            mock_instance.close = AsyncMock(side_effect=RuntimeError("close failed"))
            MockDqliteConn.return_value = mock_instance

            # Establish connection
            conn._run_sync(conn._get_async_connection())
            assert conn._loop is not None
            assert conn._thread is not None
            assert conn._thread.is_alive()

            # close() should not raise, and should clean up the loop
            conn.close()

            assert conn._closed
            assert conn._loop is None
            assert conn._thread is None
            assert conn._async_conn is None
