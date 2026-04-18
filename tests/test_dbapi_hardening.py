"""Regression tests for DB-API hardening.

Covers:
- Cursor.rownumber optional PEP 249 attribute
- fetchmany rejects negative size
- *FromTicks wrap stdlib errors as DataError
- _call_client catch-all for unknown DqliteError subclasses
- execute() rejects str/bytes as parameters (iterable trap)
- Time()/Timestamp() accept optional microsecond/tzinfo
- _datetime_from_iso8601 wraps parse failures as DataError
- _datetime_from_unixtime wraps bad server values as DataError
- _is_row_returning helper (shared between sync/async)
"""

import asyncio
import datetime

import pytest

import dqlitedbapi
from dqlitedbapi.cursor import _call_client, _is_row_returning
from dqlitedbapi.exceptions import DataError, InterfaceError, ProgrammingError


class TestRownumberProperty:
    def test_rownumber_none_without_result_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from dqlitedbapi.cursor import Cursor

        class _FakeConn:
            def _check_thread(self) -> None: ...
            def _run_sync(self, coro) -> None:  # noqa: ANN001
                coro.close()

        cursor = Cursor(_FakeConn())  # type: ignore[arg-type]
        assert cursor.rownumber is None

    def test_async_rownumber_none_without_result_set(self) -> None:
        from dqlitedbapi.aio.cursor import AsyncCursor

        class _FakeAsyncConn:
            pass

        cursor = AsyncCursor(_FakeAsyncConn())  # type: ignore[arg-type]
        assert cursor.rownumber is None

    def test_rownumber_increments_with_fetchone(self) -> None:
        """After each fetchone(), rownumber points at the next row."""
        from dqlitedbapi.cursor import Cursor

        class _FakeConn:
            def _check_thread(self) -> None: ...

        cursor = Cursor(_FakeConn())  # type: ignore[arg-type]
        cursor._description = [("x", None, None, None, None, None, None)]
        cursor._rows = [(1,), (2,), (3,)]

        assert cursor.rownumber == 0  # before any fetch, cursor points at row 0

        assert cursor.fetchone() == (1,)
        assert cursor.rownumber == 1

        assert cursor.fetchone() == (2,)
        assert cursor.rownumber == 2

        assert cursor.fetchone() == (3,)
        assert cursor.rownumber == 3  # past the end

        # Further fetches return None and do not advance rownumber past len
        assert cursor.fetchone() is None
        assert cursor.rownumber == 3

    def test_rownumber_after_fetchall(self) -> None:
        """fetchall advances rownumber to end of result set."""
        from dqlitedbapi.cursor import Cursor

        class _FakeConn:
            def _check_thread(self) -> None: ...

        cursor = Cursor(_FakeConn())  # type: ignore[arg-type]
        cursor._description = [("x", None, None, None, None, None, None)]
        cursor._rows = [(i,) for i in range(5)]
        assert cursor.rownumber == 0

        rows = cursor.fetchall()
        assert len(rows) == 5
        assert cursor.rownumber == 5

    def test_rownumber_after_fetchmany(self) -> None:
        """fetchmany advances rownumber by the number of rows fetched."""
        from dqlitedbapi.cursor import Cursor

        class _FakeConn:
            def _check_thread(self) -> None: ...

        cursor = Cursor(_FakeConn())  # type: ignore[arg-type]
        cursor._description = [("x", None, None, None, None, None, None)]
        cursor._rows = [(i,) for i in range(10)]

        cursor.fetchmany(3)
        assert cursor.rownumber == 3

        cursor.fetchmany(4)
        assert cursor.rownumber == 7


class TestFetchmanyNegativeSize:
    def test_sync_fetchmany_rejects_negative(self) -> None:
        from dqlitedbapi.cursor import Cursor

        class _FakeConn:
            def _check_thread(self) -> None: ...

        cursor = Cursor(_FakeConn())  # type: ignore[arg-type]
        cursor._description = [("x", None, None, None, None, None, None)]
        cursor._rows = [(1,), (2,)]
        with pytest.raises(ProgrammingError, match="non-negative"):
            cursor.fetchmany(-5)
        # 0 is allowed per PEP 249.
        assert cursor.fetchmany(0) == []

    def test_async_fetchmany_rejects_negative(self) -> None:
        from dqlitedbapi.aio.cursor import AsyncCursor

        class _FakeAsyncConn:
            pass

        cursor = AsyncCursor(_FakeAsyncConn())  # type: ignore[arg-type]
        cursor._description = [("x", None, None, None, None, None, None)]
        cursor._rows = [(1,), (2,)]

        async def _run() -> None:
            with pytest.raises(ProgrammingError, match="non-negative"):
                await cursor.fetchmany(-3)
            assert await cursor.fetchmany(0) == []

        asyncio.run(_run())


class TestFromTicksValidation:
    def test_timestamp_from_ticks_rejects_nan(self) -> None:
        with pytest.raises(DataError):
            dqlitedbapi.TimestampFromTicks(float("nan"))

    def test_timestamp_from_ticks_rejects_inf(self) -> None:
        with pytest.raises(DataError):
            dqlitedbapi.TimestampFromTicks(float("inf"))
        with pytest.raises(DataError):
            dqlitedbapi.TimestampFromTicks(float("-inf"))

    def test_date_from_ticks_rejects_out_of_range(self) -> None:
        # 2^63 seconds is far outside platform time_t.
        with pytest.raises(DataError):
            dqlitedbapi.DateFromTicks(2**63)

    def test_time_from_ticks_rejects_nan(self) -> None:
        with pytest.raises(DataError):
            dqlitedbapi.TimeFromTicks(float("nan"))


class TestTimestampMicrosecondTzinfo:
    def test_time_accepts_microsecond_and_tzinfo(self) -> None:
        t = dqlitedbapi.Time(12, 30, 45, 123456, tzinfo=datetime.UTC)
        assert t.microsecond == 123456
        assert t.tzinfo is datetime.UTC

    def test_timestamp_accepts_microsecond_and_tzinfo(self) -> None:
        ts = dqlitedbapi.Timestamp(2025, 1, 1, 12, 30, 45, 123456, tzinfo=datetime.UTC)
        assert ts.microsecond == 123456
        assert ts.tzinfo is datetime.UTC

    def test_defaults_preserve_backwards_compat(self) -> None:
        assert dqlitedbapi.Time(12, 30, 45) == datetime.time(12, 30, 45)
        assert dqlitedbapi.Timestamp(2025, 1, 1, 12, 30, 45) == datetime.datetime(
            2025, 1, 1, 12, 30, 45
        )


class TestCallClientCatchAll:
    def test_unknown_dqlite_error_is_wrapped(self) -> None:
        import dqliteclient.exceptions as _client_exc

        class _NovelClientError(_client_exc.DqliteError):
            pass

        async def raiser() -> None:
            raise _NovelClientError("something new")

        async def _run() -> None:
            with pytest.raises(InterfaceError, match="unrecognized client error"):
                await _call_client(raiser())

        asyncio.run(_run())


class TestExecuteRejectsStringParams:
    def test_reject_non_sequence_rejects_str(self) -> None:
        from dqlitedbapi.cursor import _reject_non_sequence_params

        with pytest.raises(ProgrammingError, match="tuple"):
            _reject_non_sequence_params("abc")

    def test_reject_non_sequence_rejects_bytes(self) -> None:
        from dqlitedbapi.cursor import _reject_non_sequence_params

        with pytest.raises(ProgrammingError, match="tuple"):
            _reject_non_sequence_params(b"abc")

    def test_reject_non_sequence_accepts_bytes_inside_tuple(self) -> None:
        from dqlitedbapi.cursor import _reject_non_sequence_params

        # (b"abc",) is a tuple of one BLOB-like value; must not raise.
        _reject_non_sequence_params((b"abc",))

    def test_reject_non_sequence_rejects_memoryview(self) -> None:
        from dqlitedbapi.cursor import _reject_non_sequence_params

        with pytest.raises(ProgrammingError, match="tuple"):
            _reject_non_sequence_params(memoryview(b"abc"))


class TestDatetimeFromIso8601Wrapping:
    def test_malformed_iso8601_raises_data_error(self) -> None:
        from dqlitedbapi.types import _datetime_from_iso8601

        with pytest.raises(DataError):
            _datetime_from_iso8601("not a real timestamp")


class TestDatetimeFromUnixtimeWrapping:
    def test_bad_value_raises_data_error(self) -> None:
        from dqlitedbapi.types import _datetime_from_unixtime

        # String (from hypothetical MitM or server bug) must wrap as DataError
        # rather than escape as TypeError.
        with pytest.raises(DataError):
            _datetime_from_unixtime("not an int")  # type: ignore[arg-type]

    def test_out_of_range_raises_data_error(self) -> None:
        from dqlitedbapi.types import _datetime_from_unixtime

        with pytest.raises(DataError):
            _datetime_from_unixtime(2**63)


class TestIsNoTransactionError:
    """commit/rollback no-op is gated on SQLite result code first.

    A malicious/impostor server must not be able to silence an unrelated
    error just by crafting a ``message`` that contains the magic
    substring.
    """

    def test_substring_only_with_sqlite_error_is_no_tx(self) -> None:
        from dqliteclient.exceptions import OperationalError as ClientOpError
        from dqlitedbapi.connection import _is_no_transaction_error

        # code=1 (SQLITE_ERROR) + substring → true
        err = ClientOpError(1, "cannot commit - no transaction is active")
        assert _is_no_transaction_error(err)

        # code=21 (SQLITE_MISUSE) + substring → true
        err = ClientOpError(21, "misuse: no transaction is active")
        assert _is_no_transaction_error(err)

    def test_disk_full_with_matching_substring_is_not_silenced(self) -> None:
        from dqliteclient.exceptions import OperationalError as ClientOpError
        from dqlitedbapi.connection import _is_no_transaction_error

        # Code 13 (SQLITE_FULL) must NOT be silenced even if the message
        # happens to contain the magic substring (attacker-controlled).
        err = ClientOpError(13, "disk full — but no transaction is active")
        assert not _is_no_transaction_error(err)

    def test_constraint_violation_is_not_silenced(self) -> None:
        from dqliteclient.exceptions import OperationalError as ClientOpError
        from dqlitedbapi.connection import _is_no_transaction_error

        err = ClientOpError(19, "constraint: no transaction is active anywhere")
        assert not _is_no_transaction_error(err)


class TestConnectForwardsMaxTotalRows:
    """Module-level connect() forwards max_total_rows.

    These tests only verify parameter plumbing — they do not open a
    socket. Connection.__init__ is pure state machine; no cluster
    needed. The previous implementation called conn.close() which
    required a running cluster for the event-loop thread to wind down
    cleanly.
    """

    @pytest.mark.parametrize(
        "max_total_rows,expected",
        [(500, 500), (None, None), (10_000, 10_000)],
    )
    def test_connect_forwards_max_total_rows(
        self, max_total_rows: int | None, expected: int | None
    ) -> None:
        from dqlitedbapi import connect
        from dqlitedbapi.connection import Connection

        # connect() does NOT actually connect to the server — it
        # instantiates a Connection with the given address and defers
        # the real TCP until first use. Inspect the attribute and then
        # skip close() because close() on a never-connected connection
        # is a silent no-op (no loop thread was started).
        conn = connect("localhost:19999", max_total_rows=max_total_rows)
        assert isinstance(conn, Connection)
        assert conn._max_total_rows == expected
        # conn.close() on an unused connection is a no-op; no cluster
        # contact happens.
        conn.close()


class TestIsRowReturning:
    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1",
            "  SELECT *",
            "  -- comment\nSELECT 1",
            "/* hello */ PRAGMA table_info(x)",
            "EXPLAIN QUERY PLAN SELECT 1",
            "WITH cte AS (SELECT 1) SELECT * FROM cte",
            "INSERT INTO t VALUES (?) RETURNING id",
            "UPDATE t SET x = 1 WHERE id = ? RETURNING *",
        ],
    )
    def test_detects_row_returning(self, sql: str) -> None:
        assert _is_row_returning(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "INSERT INTO t VALUES (?)",
            "UPDATE t SET x = 1",
            "DELETE FROM t",
            "CREATE TABLE t (x INT)",
            "VACUUM",
        ],
    )
    def test_detects_non_row_returning(self, sql: str) -> None:
        assert not _is_row_returning(sql)
