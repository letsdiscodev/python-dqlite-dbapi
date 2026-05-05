"""A failed ``Cursor.execute`` / ``AsyncCursor.execute`` must leave the
cursor in the "no result set" baseline.

PEP 249 defines ``description`` as "column descriptions for the last
query the cursor executed." After a raised ``execute()``, the "last
query" produced no result set — so reporting the prior query's
description is a silent lie that breaks callers who correctly catch
the exception and then inspect ``description`` / ``fetchall()`` /
``rowcount``.

stdlib ``sqlite3.Cursor`` (in ``pysqlite_cursor_execute_impl``) resets
``description`` to ``None`` before preparing the statement — this pin
matches that behaviour.

``_lastrowid`` is connection-scoped per SQLite / :attr:`Cursor.lastrowid`
docstring and MUST NOT be cleared by a per-cursor execute; the
preservation pin below guards against a future refactor "helpfully"
clearing it in the prologue.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.exceptions import InterfaceError, OperationalError


def _build_sync_connection_with_mock_protocol() -> tuple[dqlitedbapi.Connection, MagicMock]:
    """Build a sync Connection whose underlying async protocol is mocked
    so we control success / failure of each execute call deterministically.
    """
    conn = dqlitedbapi.Connection("localhost:9001")
    mock_proto = MagicMock()
    mock_proto.query_raw_typed = AsyncMock()
    mock_proto.execute = AsyncMock()

    async def _get_proto() -> MagicMock:
        return mock_proto

    # Bypass the real async connection bring-up; inject the mock directly.
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
    """Directly pin what ``_reset_execute_state`` does and does not touch.

    Integration tests below exercise the helper through ``execute`` and
    assert its effect on the cursor after a failed call. That indirect
    coverage would silently pass a refactor that "helpfully" adds
    ``self._lastrowid = None`` or ``self._arraysize = 1`` — because a
    subsequent ``execute()`` re-sets both of those on the success path.
    These direct tests pin the exact field set so a future edit of the
    helper is a visible contract change.
    """

    def test_sync_helper_touches_only_the_documented_fields(self) -> None:
        conn = dqlitedbapi.Connection("localhost:9001")
        try:
            cur = conn.cursor()
            # Seed every field the helper might touch so we can
            # observe which it actually does clear.
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
            # Not per-execute state.
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
            # Stdlib parity (fetchone/fetchmany/fetchall all return
            # the empty value rather than raising on no-result-set).
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

            # rowcount resets, description clears, BUT lastrowid
            # MUST survive — it reflects the CONNECTION's last INSERT
            # per SQLite semantics.
            assert cur.rowcount == -1
            assert cur.description is None
            assert cur.lastrowid == 42
        finally:
            conn.close()

    def test_closed_cursor_execute_raises_before_clearing(self) -> None:
        """Executing on a closed cursor must still raise the sharp
        ``InterfaceError("Cursor is closed")`` — the prologue must
        not clear state before the closed guard.
        """
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
        """Sibling to test_closed_cursor_execute_raises_before_clearing
        for executemany. The closed-cursor guard runs before any work
        on the executemany call too — pin the contract symmetrically
        so a refactor that loses the guard on one method while keeping
        it on the other is caught."""
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
        """The closed-cursor guard MUST run before the row-returning
        rejection. A SELECT-shaped statement on an OPEN cursor would
        raise ``ProgrammingError`` ("can only execute DML statements");
        on a CLOSED cursor, the closed-check must win and surface
        ``InterfaceError("Cursor is closed")``. Pin the ordering so a
        refactor that swaps the two checks is caught."""
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
            # Stdlib parity (fetchone/fetchmany/fetchall all return
            # the empty value rather than raising on no-result-set).
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
        """Cancelling the task during ``_call_client`` must still leave
        the cursor in a clean baseline — the prologue runs before the
        wire call, so the cursor is cleaned regardless of how the call
        raises.
        """
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
        """Async sibling of TestSyncCursorStateResetOnFailure.
        test_closed_cursor_executemany_raises_with_dml — pin the same
        closed-cursor guard contract on the async path."""
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
            await cur.close()

            with pytest.raises(InterfaceError, match="Cursor is closed"):
                await cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
        finally:
            await aconn.close()

    async def test_closed_cursor_executemany_raises_before_row_returning_rejection(
        self,
    ) -> None:
        """Async sibling of the sync ordering pin: closed-check MUST
        run before the row-returning rejection on the async path
        too. SELECT-shaped statement on a closed cursor surfaces
        InterfaceError, not ProgrammingError."""
        aconn, _proto = _build_async_connection_with_mock_protocol()
        try:
            cur = aconn.cursor()
            await cur.close()

            with pytest.raises(InterfaceError, match="Cursor is closed"):
                await cur.executemany("SELECT ?", [(1,)])
        finally:
            await aconn.close()
