"""Context-manager ``__exit__`` should not silently swallow commit/rollback
failures.

Previous behavior: ``except Exception: pass`` around commit/rollback
masked real production errors (network drops, disk-full, etc.). Now
the body's exception still wins on rollback failure (attached as
``__context__``), but a clean-exit commit failure surfaces cleanly
instead of disappearing.
"""

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
    # ``Connection.__enter__`` eagerly calls ``connect()`` (matching
    # ``AsyncConnection.__aenter__``). These tests pre-set the mocked
    # ``_async_conn`` directly, so the unit-test flow wants a no-op
    # connect rather than a real TCP dial.
    return patch.object(Connection, "connect")


class TestExitPropagatesCommitFailure:
    def test_clean_exit_commit_failure_propagates(self) -> None:
        """No body exception → commit failure surfaces, not swallowed."""
        conn = _make_conn_with_failing_commit()
        try:
            with (
                _eager_enter_connect(),
                pytest.raises(OperationalError, match="disk full"),
                conn,
            ):
                pass  # clean body → __exit__ calls commit, which raises
        finally:
            conn.close()

    def test_body_exception_wins_over_rollback_failure(self) -> None:
        """Body raised; rollback also fails → body's exception is what the
        caller sees. Rollback failure is attached as __context__ so it's
        not lost entirely.
        """
        conn = _make_conn_with_failing_commit()
        body_error = ValueError("user bug")
        try:
            with (
                _eager_enter_connect(),
                pytest.raises(ValueError, match="user bug"),
                conn,
            ):
                raise body_error
        finally:
            conn.close()


class TestExitOnUnusedConnection:
    def test_unused_connection_exit_is_silent(self) -> None:
        """If ``connect()`` succeeds but no query ran, ``__exit__`` is
        a no-op: no commit/rollback is attempted and the connection
        is not closed (stdlib sqlite3 parity — it remains reusable).
        ``__enter__`` eagerly connects so we mock that; the point of
        this case is the post-enter / pre-query shape."""
        conn = Connection("localhost:9001", timeout=2.0)
        assert conn._async_conn is None
        # Clean exit, no body exception, no TCP connection ever made.
        with _eager_enter_connect(), conn:
            pass
        assert not conn._closed
        conn.close()


class TestCommitNoTransactionSwallowed:
    """The 'no transaction is active' server error is still swallowed
    (matches stdlib sqlite3 semantics), even after we removed the
    blanket ``except Exception: pass``.
    """

    def test_commit_swallows_no_tx_error(self) -> None:
        conn = Connection("localhost:9001", timeout=2.0)
        mock_async_conn = AsyncMock()
        mock_async_conn.execute = AsyncMock(
            side_effect=OperationalError("cannot commit - no transaction is active")
        )
        conn._async_conn = mock_async_conn
        try:
            conn.commit()  # no raise — silent success
        finally:
            conn.close()
