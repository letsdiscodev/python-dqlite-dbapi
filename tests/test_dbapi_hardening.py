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
import sys

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
        cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
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
        cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
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
        cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
        cursor._rows = [(i,) for i in range(10)]

        cursor.fetchmany(3)
        assert cursor.rownumber == 3

        cursor.fetchmany(4)
        assert cursor.rownumber == 7


class TestFetchmanyNegativeSize:
    def test_sync_fetchmany_negative_returns_all(self) -> None:
        # Stdlib ``sqlite3.Cursor.fetchmany`` parity: negative size
        # returns all remaining rows.
        from dqlitedbapi.cursor import Cursor

        class _FakeConn:
            def _check_thread(self) -> None: ...

        cursor = Cursor(_FakeConn())  # type: ignore[arg-type]
        cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
        cursor._rows = [(1,), (2,)]
        assert cursor.fetchmany(-5) == [(1,), (2,)]

    def test_sync_fetchmany_zero_returns_empty(self) -> None:
        from dqlitedbapi.cursor import Cursor

        class _FakeConn:
            def _check_thread(self) -> None: ...

        cursor = Cursor(_FakeConn())  # type: ignore[arg-type]
        cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
        cursor._rows = [(1,), (2,)]
        # 0 is allowed per PEP 249.
        assert cursor.fetchmany(0) == []

    def test_async_fetchmany_negative_returns_all(self) -> None:
        from dqlitedbapi.aio.cursor import AsyncCursor

        class _FakeAsyncConn:
            # AsyncCursor's fetch* methods route through the
            # non-binding ``_check_loop_binding`` for the loop-binding
            # diagnostic without lazy-binding the loop on a fresh
            # cursor; the mock must satisfy that contract with a no-op.
            def _check_loop_binding(self) -> None:
                return None

        cursor = AsyncCursor(_FakeAsyncConn())  # type: ignore[arg-type]
        cursor._description = [("x", None, None, None, None, None, None)]  # type: ignore[assignment]
        cursor._rows = [(1,), (2,)]

        async def _run() -> None:
            assert await cursor.fetchmany(-3) == [(1,), (2,)]
            cursor._row_index = 0
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

    def test_extreme_in_range_int64_wraps_as_dataerror_with_value(self) -> None:
        """A valid int64 large enough to overflow ``datetime.fromtimestamp``
        on every supported platform (1<<62 ≈ 4.6e18 seconds, year ~146
        billion) must funnel through ``DataError`` with the offending
        value in the message — not leak the raw stdlib exception
        (``OSError`` / ``OverflowError`` / ``ValueError`` depending on
        platform). A future "cleanup" refactor that narrows the catch
        tuple in ``_datetime_from_unixtime`` would silently leak the
        platform exception to PEP 249 callers; this guaranteed-fail
        value pins the funnel.
        """
        from dqlitedbapi.types import _datetime_from_unixtime

        value = 1 << 62
        with pytest.raises(DataError) as ei:
            _datetime_from_unixtime(value)
        assert repr(value) in str(ei.value), (
            f"DataError message must echo the offending value; got {ei.value!s}"
        )

    @pytest.mark.skipif(
        not sys.platform.startswith("linux"),
        reason="OSError raise mode is Linux-specific",
    )
    def test_int64_max_wraps_oserror_as_dataerror_on_linux(self) -> None:
        """On Linux, ``datetime.fromtimestamp((1<<63)-1)`` raises
        ``OSError [Errno 75]``. This pin guards the catch tuple's
        ``OSError`` entry — the most-likely-to-be-deleted entry on a
        narrowing refactor — on the most common deployment platform.
        """
        from dqlitedbapi.types import _datetime_from_unixtime

        with pytest.raises(DataError):
            _datetime_from_unixtime((1 << 63) - 1)

    def test_negative_unixtime_raises_uniform_data_error(self) -> None:
        """Negative UNIXTIME (pre-1970) is platform-inconsistent at
        ``datetime.fromtimestamp`` (Linux glibc accepts; Windows
        ``_gmtime64_s`` rejects). The decoder must surface a uniform
        ``DataError`` regardless of platform so a test passing on
        Linux does not silently fail on Windows.

        dqlite servers do not emit pre-1970 UNIXTIME today; the bound
        is permissive defense-in-depth, not a behavior change.
        """
        from dqlitedbapi.types import _datetime_from_unixtime

        with pytest.raises(DataError, match="out of representable range"):
            _datetime_from_unixtime(-1)
        with pytest.raises(DataError, match="out of representable range"):
            _datetime_from_unixtime(-(1 << 32))

    def test_post_year_9999_unixtime_raises_uniform_data_error(self) -> None:
        """UNIXTIME values past year 9999 (datetime.MAX) are equally
        platform-inconsistent. Surface as ``DataError`` uniformly."""
        from dqlitedbapi.types import _MAX_UNIXTIME_SECONDS, _datetime_from_unixtime

        with pytest.raises(DataError, match="out of representable range"):
            _datetime_from_unixtime(_MAX_UNIXTIME_SECONDS + 1)

    def test_zero_unixtime_decodes_to_unix_epoch(self) -> None:
        """Boundary: 0 → 1970-01-01T00:00:00Z."""
        from dqlitedbapi.types import _datetime_from_unixtime

        result = _datetime_from_unixtime(0)
        assert result == datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)

    def test_modern_unixtime_decodes_correctly(self) -> None:
        """A real-world value (2 billion s ≈ 2033) round-trips."""
        from dqlitedbapi.types import _datetime_from_unixtime

        result = _datetime_from_unixtime(2_000_000_000)
        assert result.tzinfo == datetime.UTC
        assert result.year == 2033


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

        # code=21 (SQLITE_MISUSE) is NOT in the whitelist — the dqlite
        # server never returns it on the COMMIT/ROLLBACK path. A real
        # misuse must surface as a real error, not a silent no-op.
        err = ClientOpError(21, "misuse: no transaction is active")
        assert not _is_no_transaction_error(err)

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
            # VALUES (...) is a valid top-level row-returning SQLite statement.
            "VALUES (1), (2), (3)",
            "values (1, 'a')",
            "  -- preamble\nVALUES (1)",
            # Leading paren on a row-returning form.
            "(SELECT 1)",
            "(SELECT a FROM t) UNION (SELECT b FROM u)",
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
            # Comment-only / malformed-comment input must not be
            # classified as row-returning — the helper either strips
            # to an empty string (single-line comment with no newline)
            # or leaves the input unchanged (unterminated block
            # comment), neither of which matches a row-returning
            # prefix.
            "-- just a comment to end of file",
            "/* unterminated block with SELECT inside",
            "/* header */-- trailing single-line only",
        ],
    )
    def test_detects_non_row_returning(self, sql: str) -> None:
        assert not _is_row_returning(sql)


class TestStripLeadingComments:
    """Pin the pure-function helper that feeds ``_is_row_returning``.

    The two edge-case return paths — single-line ``--`` comment with
    no trailing newline (returns ``""``) and unterminated ``/* …``
    block (returns the input unchanged) — are only indirectly covered
    through ``_is_row_returning`` today. Pinning them at the helper
    level prevents a future regex-based rewrite from silently
    changing the contract without an integration run that exercises
    malformed SQL.
    """

    # Private helper pinned directly because ``_is_row_returning`` only
    # exercises the terminated-comment branches.
    @pytest.mark.parametrize(
        ("sql", "expected"),
        [
            # Pass-through regression anchors.
            ("SELECT 1", "SELECT 1"),
            ("  SELECT 1  ", "SELECT 1"),
            # Terminated single-line and block comments.
            ("-- header\nSELECT 1", "SELECT 1"),
            ("/* header */SELECT 1", "SELECT 1"),
            ("   \n  -- c\n  SELECT 1", "SELECT 1"),
            # Mixed: terminated SL then block, stripped down to SELECT.
            ("-- a\n/* b */SELECT 1", "SELECT 1"),
            # Edge 1: single-line comment with no trailing newline.
            # Helper returns "" (empty string).
            ("-- just a comment", ""),
            ("  -- leading ws and trailing comment only", ""),
            # Edge 2: unterminated block comment — collapses to empty
            # (mirrors the unterminated ``--`` branch's "consumes
            # everything" semantics; SQLite parse-rejects the input,
            # so signaling "no usable verb" is correct).
            ("/* oops no close", ""),
            ("  /* also unterminated with leading ws", ""),
            ("/* unterminated with SELECT inside", ""),
            # Edge 3: mixed — terminated SL then unterminated block.
            ("-- ok\n/* then unterminated", ""),
            # Edge 4: mixed — terminated block then trailing SL
            # comment to EOF. The SL branch returns "".
            ("/* header */-- trailing only", ""),
        ],
    )
    def test_strip_leading_comments(self, sql: str, expected: str) -> None:
        from dqlitedbapi.cursor import _strip_leading_comments

        assert _strip_leading_comments(sql) == expected


class TestOperationalErrorCode:
    """dbapi OperationalError carries the SQLite extended error code
    forwarded from the client layer. Consumers like
    ``_is_no_transaction_error`` and the SQLAlchemy dialect's
    ``is_disconnect`` key on ``getattr(exc, 'code', None)``.
    """

    def test_default_code_is_none(self) -> None:
        from dqlitedbapi.exceptions import OperationalError

        e = OperationalError("boom")
        assert e.code is None
        assert str(e) == "boom"

    def test_explicit_code_preserved(self) -> None:
        from dqlitedbapi.exceptions import OperationalError

        e = OperationalError("not leader", code=10250)
        assert e.code == 10250

    def test_call_client_preserves_client_code(self) -> None:
        import asyncio

        import dqliteclient.exceptions as _client_exc
        from dqlitedbapi.cursor import _call_client
        from dqlitedbapi.exceptions import OperationalError

        async def raiser() -> None:
            raise _client_exc.OperationalError(10250, "not the leader")

        with pytest.raises(OperationalError) as excinfo:
            asyncio.run(_call_client(raiser()))

        assert excinfo.value.code == 10250
