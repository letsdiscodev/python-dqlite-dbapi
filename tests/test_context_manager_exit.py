"""Context-manager __exit__ surfaces clean-exit commit failures; on rollback failure the body
exception still wins (rollback failure attached as __context__)."""

from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import OperationalError


def _make_conn_with_failing_commit() -> Connection:
    """Build a Connection whose underlying _async_conn.execute raises."""
    conn = Connection("localhost:9001", timeout=2.0)
    mock_async_conn = AsyncMock()
    mock_async_conn.execute = AsyncMock(side_effect=OperationalError("disk full"))
    conn._async_conn = mock_async_conn  # pretend already connected
    return conn


def _eager_enter_connect() -> object:
    # No-op connect: tests pre-set a mocked _async_conn, so __enter__'s eager dial is bypassed.
    return patch.object(Connection, "connect")


class TestExitPropagatesCommitFailure:
    def test_clean_exit_commit_failure_propagates(self) -> None:
        conn = _make_conn_with_failing_commit()
        try:
            with (
                _eager_enter_connect(),  # type: ignore[attr-defined]
                pytest.raises(OperationalError, match="disk full"),
                conn,
            ):
                pass
        finally:
            conn.close()

    def test_body_exception_wins_over_rollback_failure(self) -> None:
        conn = _make_conn_with_failing_commit()
        body_error = ValueError("user bug")
        try:
            with (
                _eager_enter_connect(),  # type: ignore[attr-defined]
                pytest.raises(ValueError, match="user bug"),
                conn,
            ):
                raise body_error
        finally:
            conn.close()


class TestExitOnUnusedConnection:
    def test_unused_connection_exit_is_silent(self) -> None:
        """Connected but no query ran: __exit__ is a no-op and the connection stays reusable
        (stdlib sqlite3 parity)."""
        conn = Connection("localhost:9001", timeout=2.0)
        assert conn._async_conn is None
        with _eager_enter_connect(), conn:  # type: ignore[attr-defined]
            pass
        assert not conn._closed
        conn.close()


class TestCommitNoTransactionSwallowed:
    """The 'no transaction is active' server error stays swallowed (stdlib sqlite3 parity)."""

    def test_commit_swallows_no_tx_error(self) -> None:
        conn = Connection("localhost:9001", timeout=2.0)
        mock_async_conn = AsyncMock()
        # Swallowed only when both code (SQLITE_ERROR=1) and wording match.
        mock_async_conn.execute = AsyncMock(
            side_effect=OperationalError("cannot commit - no transaction is active", code=1)
        )
        conn._async_conn = mock_async_conn
        try:
            conn.commit()
        finally:
            conn.close()
