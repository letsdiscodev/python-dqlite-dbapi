"""A failed execute must reset the cursor to the no-result-set baseline
(clear description/rows/rowcount), matching stdlib sqlite3. But
_lastrowid is connection-scoped per SQLite and MUST survive — the
preservation pins guard a future refactor from clearing it."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.exceptions import InterfaceError, OperationalError


def _build_sync_connection_with_mock_protocol() -> tuple[dqlitedbapi.Connection, MagicMock]:
    """Sync Connection with a mocked async protocol for deterministic
    success/failure of each execute."""
    conn = dqlitedbapi.Connection("localhost:9001")
    mock_proto = MagicMock()
    mock_proto.query_raw_typed = AsyncMock()
    mock_proto.execute = AsyncMock()

    async def _get_proto() -> MagicMock:
        return mock_proto

    conn._get_async_connection = _get_proto
    return conn, mock_proto


def _build_async_connection_with_mock_protocol() -> tuple[
    dqlitedbapi.aio.AsyncConnection, MagicMock
]:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    mock_proto = MagicMock()
    mock_proto.query_raw_typed = AsyncMock()
    mock_proto.execute = AsyncMock()

    async def _ensure_conn() -> MagicMock:
        return mock_proto

    aconn._ensure_connection = _ensure_conn
    return aconn, mock_proto


class TestResetExecuteStateHelperContract:
    """Pin the exact fields _reset_execute_state touches: the indirect
    coverage below would mask a refactor clearing _lastrowid/_arraysize
    since execute re-sets both on the success path."""

    def test_sync_helper_touches_only_the_documented_fields(self) -> None:
        conn = dqlitedbapi.Connection("localhost:9001")
        try:
            cur = conn.cursor()
            cur._description = [("a", 3, None, None, None, None, None)]  # type: ignore[assignment]
            cur._rows = [(1,), (2,)]
            cur._row_index = 1
            cur._rowcount = 7
            cur._lastrowid = 99
            cur._arraysize = 42

            cur._reset_execute_state()

            assert cur._description is None
            assert cur._rows == []
            assert cur._row_index == 0
            assert cur._rowcount == -1
            # Connection-scoped: MUST survive per SQLite semantics.
            assert cur._lastrowid == 99
            assert cur._arraysize == 42
        finally:
            conn.close()

    async def test_async_helper_touches_only_the_documented_fields(
        self,
    ) -> None:
        aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
        try:
            cur = aconn.cursor()
            cur._description = [("a", 3, None, None, None, None, None)]  # type: ignore[assignment]
            cur._rows = [(1,), (2,)]
            cur._row_index = 1
            cur._rowcount = 7
            cur._lastrowid = 99
            cur._arraysize = 42

            cur._reset_execute_state()

            assert cur._description is None
            assert cur._rows == []
            assert cur._row_index == 0
            assert cur._rowcount == -1
            assert cur._lastrowid == 99
            assert cur._arraysize == 42
        finally:
            await aconn.close()


class TestSyncCursorStateResetOnFailure:
    def test_select_then_failed_select_clears_description(self) -> None:
        conn, proto = _build_sync_connection_with_mock_protocol()
        try:
            proto.query_raw_typed.return_value = (
                ["a"],
                [3],
                [[3]],
                [(1,)],
            )
            cur = conn.cursor()
            cur.execute("SELECT a FROM t")
            assert cur.description is not None
            assert cur.rowcount == 1

            proto.query_raw_typed.side_effect = OperationalError("boom", code=1)
            with pytest.raises(OperationalError):
                cur.execute("SELECT a FROM bogus")

            assert cur.description is None
            assert cur.rowcount == -1
            # Stdlib parity: empty value, not a raise, on no-result-set.
            assert cur.fetchall() == []
            assert cur.fetchone() is None
            assert cur.fetchmany(5) == []
        finally:
            conn.close()

    def test_select_then_failed_dml_clears_description(self) -> None:
        conn, proto = _build_sync_connection_with_mock_protocol()
        try:
            proto.query_raw_typed.return_value = (
                ["a"],
                [3],
                [[3]],
                [(1,)],
            )
            cur = conn.cursor()
            cur.execute("SELECT a FROM t")
            assert cur.description is not None

            proto.execute.side_effect = OperationalError("constraint", code=19)
            with pytest.raises(OperationalError):
                cur.execute("INSERT INTO t VALUES (1)")

            assert cur.description is None
            assert cur.rowcount == -1
        finally:
            conn.close()

    def test_insert_success_then_failed_select_preserves_lastrowid(
        self,
    ) -> None:
        conn, proto = _build_sync_connection_with_mock_protocol()
        try:
            proto.execute.return_value = (42, 1)  # (last_insert_id, affected)
            cur = conn.cursor()
            cur.execute("INSERT INTO t VALUES (1)")
            assert cur.lastrowid == 42
            assert cur.rowcount == 1

            proto.query_raw_typed.side_effect = OperationalError("bad", code=1)
            with pytest.raises(OperationalError):
                cur.execute("SELECT a FROM bogus")

            # lastrowid MUST survive — it reflects the connection's last
            # INSERT per SQLite semantics.
            assert cur.rowcount == -1
            assert cur.description is None
            assert cur.lastrowid == 42
        finally:
            conn.close()

    def test_closed_cursor_execute_raises_before_clearing(self) -> None:
        """Closed-cursor execute raises InterfaceError; the prologue must
        not clear state before the closed guard."""
        conn, proto = _build_sync_connection_with_mock_protocol()
        try:
            proto.query_raw_typed.return_value = (
                ["a"],
                [3],
                [[3]],
                [(99,)],
            )
            cur = conn.cursor()
            cur.execute("SELECT a FROM t")
            prior_description = cur.description
            assert prior_description is not None
            cur.close()

            with pytest.raises(InterfaceError, match="Cursor is closed"):
                cur.execute("SELECT 1")
        finally:
            conn.close()

    def test_closed_cursor_executemany_raises_with_dml(self) -> None:
        """executemany sibling: the closed-cursor guard runs before any
        work, pinned symmetrically with execute."""
        conn, proto = _build_sync_connection_with_mock_protocol()
        try:
            proto.query_raw_typed.return_value = (
                ["a"],
                [3],
                [[3]],
                [(99,)],
            )
            cur = conn.cursor()
            cur.execute("SELECT a FROM t")
            cur.close()

            with pytest.raises(InterfaceError, match="Cursor is closed"):
                cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
        finally:
            conn.close()

    def test_closed_cursor_executemany_raises_before_row_returning_rejection(
        self,
    ) -> None:
        """The closed-cursor guard must win over the row-returning
        rejection: a SELECT-shaped statement on a closed cursor surfaces
        InterfaceError, not ProgrammingError."""
        conn, _proto = _build_sync_connection_with_mock_protocol()
        try:
            cur = conn.cursor()
            cur.close()

            with pytest.raises(InterfaceError, match="Cursor is closed"):
                cur.executemany("SELECT ?", [(1,)])
        finally:
            conn.close()


class TestAsyncCursorStateResetOnFailure:
    async def test_select_then_failed_select_clears_description(self) -> None:
        aconn, proto = _build_async_connection_with_mock_protocol()
        try:
            proto.query_raw_typed.return_value = (
                ["a"],
                [3],
                [[3]],
                [(1,)],
            )
            cur = aconn.cursor()
            await cur.execute("SELECT a FROM t")
            assert cur.description is not None
            assert cur.rowcount == 1

            proto.query_raw_typed.side_effect = OperationalError("boom", code=1)
            with pytest.raises(OperationalError):
                await cur.execute("SELECT a FROM bogus")

            assert cur.description is None
            assert cur.rowcount == -1
            # Stdlib parity: empty value, not a raise, on no-result-set.
            assert await cur.fetchall() == []
            assert await cur.fetchone() is None
            assert await cur.fetchmany(5) == []
        finally:
            await aconn.close()

    async def test_select_then_failed_dml_clears_description(self) -> None:
        aconn, proto = _build_async_connection_with_mock_protocol()
        try:
            proto.query_raw_typed.return_value = (
                ["a"],
                [3],
                [[3]],
                [(1,)],
            )
            cur = aconn.cursor()
            await cur.execute("SELECT a FROM t")
            assert cur.description is not None

            proto.execute.side_effect = OperationalError("constraint", code=19)
            with pytest.raises(OperationalError):
                await cur.execute("INSERT INTO t VALUES (1)")

            assert cur.description is None
            assert cur.rowcount == -1
        finally:
            await aconn.close()

    async def test_insert_success_then_failed_select_preserves_lastrowid(
        self,
    ) -> None:
        aconn, proto = _build_async_connection_with_mock_protocol()
        try:
            proto.execute.return_value = (42, 1)
            cur = aconn.cursor()
            await cur.execute("INSERT INTO t VALUES (1)")
            assert cur.lastrowid == 42
            assert cur.rowcount == 1

            proto.query_raw_typed.side_effect = OperationalError("bad", code=1)
            with pytest.raises(OperationalError):
                await cur.execute("SELECT a FROM bogus")

            assert cur.rowcount == -1
            assert cur.description is None
            assert cur.lastrowid == 42
        finally:
            await aconn.close()

    async def test_cancellederror_mid_execute_clears_description(self) -> None:
        """Cancellation mid-execute still leaves a clean baseline: the
        prologue runs before the wire call."""
        import asyncio

        aconn, proto = _build_async_connection_with_mock_protocol()
        try:
            proto.query_raw_typed.return_value = (
                ["a"],
                [3],
                [[3]],
                [(1,)],
            )
            cur = aconn.cursor()
            await cur.execute("SELECT a FROM t")
            assert cur.description is not None
            assert cur.rowcount == 1

            async def _slow_raise(*_args: Any, **_kwargs: Any) -> Any:
                await asyncio.sleep(10)

            proto.query_raw_typed.side_effect = _slow_raise
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(cur.execute("SELECT a FROM t"), timeout=0.01)

            assert cur.description is None
            assert cur.rowcount == -1
        finally:
            await aconn.close()

    async def test_closed_cursor_executemany_raises_with_dml(self) -> None:
        """Async sibling: same closed-cursor guard contract."""
        aconn, proto = _build_async_connection_with_mock_protocol()
        try:
            proto.query_raw_typed.return_value = (
                ["a"],
                [3],
                [[3]],
                [(99,)],
            )
            cur = aconn.cursor()
            await cur.execute("SELECT a FROM t")
            cur.close()

            with pytest.raises(InterfaceError, match="Cursor is closed"):
                await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
        finally:
            await aconn.close()

    async def test_closed_cursor_executemany_raises_before_row_returning_rejection(
        self,
    ) -> None:
        """Async sibling: closed-check wins over the row-returning
        rejection."""
        aconn, _proto = _build_async_connection_with_mock_protocol()
        try:
            cur = aconn.cursor()
            cur.close()

            with pytest.raises(InterfaceError, match="Cursor is closed"):
                await cur.executemany("SELECT ?", [(1,)])
        finally:
            await aconn.close()
