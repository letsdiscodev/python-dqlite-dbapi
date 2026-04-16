"""Tests that commit/rollback don't create spurious connections.

If commit() or rollback() is called on a connection that was never used
(no execute() was called), it should be a no-op. It should NOT create a
new TCP connection just to send COMMIT/ROLLBACK.
"""

from unittest.mock import AsyncMock, patch

from dqlitedbapi.connection import Connection


class TestCommitNoSpuriousConnect:
    def test_commit_on_unused_connection_is_noop(self) -> None:
        """commit() should not create a connection if none exists."""
        conn = Connection("localhost:9001", timeout=2.0)

        with patch.object(conn, "_get_async_connection") as mock_get:
            mock_get.return_value = AsyncMock()
            conn.commit()
            mock_get.assert_not_called()

        conn.close()

    def test_rollback_on_unused_connection_is_noop(self) -> None:
        """rollback() should not create a connection if none exists."""
        conn = Connection("localhost:9001", timeout=2.0)

        with patch.object(conn, "_get_async_connection") as mock_get:
            mock_get.return_value = AsyncMock()
            conn.rollback()
            mock_get.assert_not_called()

        conn.close()


class TestCommitRollbackAsyncNoSpuriousConnect:
    def test_commit_async_does_not_call_get_async_connection(self) -> None:
        """_commit_async should check _async_conn directly, not call _get_async_connection."""
        import ast
        import inspect
        import textwrap

        source = textwrap.dedent(inspect.getsource(Connection._commit_async))
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Attribute) and func.attr == "_get_async_connection":
                    raise AssertionError(
                        "_commit_async calls _get_async_connection which creates "
                        "new connections. It should check _async_conn directly."
                    )

    def test_rollback_async_does_not_call_get_async_connection(self) -> None:
        """_rollback_async should check _async_conn directly, not call _get_async_connection."""
        import ast
        import inspect
        import textwrap

        source = textwrap.dedent(inspect.getsource(Connection._rollback_async))
        tree = ast.parse(source)

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Attribute) and func.attr == "_get_async_connection":
                    raise AssertionError(
                        "_rollback_async calls _get_async_connection which creates "
                        "new connections. It should check _async_conn directly."
                    )
