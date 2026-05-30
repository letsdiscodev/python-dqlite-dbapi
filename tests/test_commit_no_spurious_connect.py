"""commit()/rollback() on an unused connection must be a no-op, not a new TCP dial."""

from unittest.mock import AsyncMock, patch

from dqlitedbapi.connection import Connection


class TestCommitNoSpuriousConnect:
    def test_commit_on_unused_connection_is_noop(self) -> None:
        conn = Connection("localhost:9001", timeout=2.0)

        with patch.object(conn, "_get_async_connection") as mock_get:
            mock_get.return_value = AsyncMock()
            conn.commit()
            mock_get.assert_not_called()

        conn.close()

    def test_rollback_on_unused_connection_is_noop(self) -> None:
        conn = Connection("localhost:9001", timeout=2.0)

        with patch.object(conn, "_get_async_connection") as mock_get:
            mock_get.return_value = AsyncMock()
            conn.rollback()
            mock_get.assert_not_called()

        conn.close()
