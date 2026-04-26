"""Integration tests for the DBAPI datetime conversion layer.

The DBAPI contract is PEP 249: date/time columns return Python ``datetime``
objects. The wire layer deals only in primitives (ISO8601 → str, UNIXTIME →
int64); the DBAPI is where that gets turned into real ``datetime`` values.
These tests exercise the end-to-end path against a live cluster.
"""

import asyncio
import datetime

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitewire.constants import ValueType


@pytest.mark.integration
class TestDateTimeRoundTrip:
    """ISO8601 column round-trips through cursor.execute."""

    def test_naive_datetime_stays_naive(self, cluster_address: str) -> None:
        """A naive datetime written via bind param round-trips as naive.

        The DBAPI must not silently assume UTC on either side — that was
        the pre-fix wire-layer bug and is explicitly rejected here.
        """
        dt = datetime.datetime(2024, 1, 15, 10, 30, 45)  # noqa: DTZ001 - naive is the point
        with connect(cluster_address, database="test_dt_naive") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS dt_naive (id INTEGER PRIMARY KEY, ts DATETIME)"
            )
            cursor.execute("DELETE FROM dt_naive")
            cursor.execute("INSERT INTO dt_naive (ts) VALUES (?)", [dt])
            cursor.execute("SELECT ts FROM dt_naive")
            (value,) = cursor.fetchone()  # type: ignore[misc]
            assert isinstance(value, datetime.datetime)
            assert value.tzinfo is None, f"expected naive, got tzinfo={value.tzinfo}"
            assert value == dt
            cursor.execute("DROP TABLE dt_naive")

    def test_aware_datetime_preserves_offset(self, cluster_address: str) -> None:
        """An aware datetime round-trips with its original tz offset."""
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        dt = datetime.datetime(2024, 6, 15, 12, 30, 45, tzinfo=tz)
        with connect(cluster_address, database="test_dt_aware") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS dt_aware (id INTEGER PRIMARY KEY, ts DATETIME)"
            )
            cursor.execute("DELETE FROM dt_aware")
            cursor.execute("INSERT INTO dt_aware (ts) VALUES (?)", [dt])
            cursor.execute("SELECT ts FROM dt_aware")
            (value,) = cursor.fetchone()  # type: ignore[misc]
            assert isinstance(value, datetime.datetime)
            assert value == dt
            assert value.utcoffset() == datetime.timedelta(hours=5, minutes=30)
            cursor.execute("DROP TABLE dt_aware")

    def test_microseconds_preserved(self, cluster_address: str) -> None:
        """6-digit microseconds survive the round-trip."""
        dt = datetime.datetime(2024, 1, 15, 10, 30, 45, 123456)  # noqa: DTZ001
        with connect(cluster_address, database="test_dt_us") as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS dt_us (id INTEGER PRIMARY KEY, ts DATETIME)")
            cursor.execute("DELETE FROM dt_us")
            cursor.execute("INSERT INTO dt_us (ts) VALUES (?)", [dt])
            cursor.execute("SELECT ts FROM dt_us")
            (value,) = cursor.fetchone()  # type: ignore[misc]
            assert value.microsecond == 123456
            cursor.execute("DROP TABLE dt_us")

    def test_null_datetime_returns_none(self, cluster_address: str) -> None:
        """NULL in a DATETIME column is None on read (not an exception)."""
        with connect(cluster_address, database="test_dt_null") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS dt_null (id INTEGER PRIMARY KEY, ts DATETIME)"
            )
            cursor.execute("DELETE FROM dt_null")
            cursor.execute("INSERT INTO dt_null (ts) VALUES (NULL)")
            cursor.execute("SELECT ts FROM dt_null")
            (value,) = cursor.fetchone()  # type: ignore[misc]
            assert value is None
            cursor.execute("DROP TABLE dt_null")

    def test_date_bind_param(self, cluster_address: str) -> None:
        """A ``datetime.date`` bind param is stringified by the DBAPI.

        The C server tags DATE columns as ISO8601 so the DBAPI returns a
        ``datetime.datetime``. Narrowing back to ``date`` is the SQLAlchemy
        dialect's job — at the raw DBAPI level we just verify the value
        round-trips and contains the expected calendar date.
        """
        d = datetime.date(2024, 3, 14)
        with connect(cluster_address, database="test_dt_date") as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS dt_date (id INTEGER PRIMARY KEY, d DATE)")
            cursor.execute("DELETE FROM dt_date")
            cursor.execute("INSERT INTO dt_date (d) VALUES (?)", [d])
            cursor.execute("SELECT d FROM dt_date")
            (value,) = cursor.fetchone()  # type: ignore[misc]
            assert isinstance(value, datetime.datetime)
            assert value.date() == d
            cursor.execute("DROP TABLE dt_date")


@pytest.mark.integration
class TestTimeBindParam:
    """``datetime.time`` bind parameters stringify to ISO 8601 so the
    driver's own ``Time()`` / ``TimeFromTicks()`` constructors round-trip
    through the wire. The server stores the value as TEXT; readback is
    the ISO string (the decoder has no TIME affinity).
    """

    def test_naive_time_round_trips_as_string(self, cluster_address: str) -> None:
        import dqlitedbapi

        t = dqlitedbapi.Time(12, 30, 45)
        with connect(cluster_address, database="test_time_naive") as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS t_naive (v TEXT)")
            cursor.execute("DELETE FROM t_naive")
            cursor.execute("INSERT INTO t_naive (v) VALUES (?)", [t])
            cursor.execute("SELECT v FROM t_naive")
            (value,) = cursor.fetchone()  # type: ignore[misc]
            assert value == "12:30:45"
            cursor.execute("DROP TABLE t_naive")

    def test_time_with_microseconds_and_utc(self, cluster_address: str) -> None:
        t = datetime.time(12, 30, 45, 123456, tzinfo=datetime.UTC)
        with connect(cluster_address, database="test_time_us") as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS t_us (v TEXT)")
            cursor.execute("DELETE FROM t_us")
            cursor.execute("INSERT INTO t_us (v) VALUES (?)", [t])
            cursor.execute("SELECT v FROM t_us")
            (value,) = cursor.fetchone()  # type: ignore[misc]
            assert value == "12:30:45.123456+00:00"
            cursor.execute("DROP TABLE t_us")


@pytest.mark.integration
class TestUnixtimeColumn:
    """INTEGER values in DATETIME-typed columns come back as datetime.

    The C server (dqlite-upstream/src/query.c) tags INTEGER-valued cells of
    DATETIME/DATE/TIMESTAMP columns as DQLITE_UNIXTIME. The DBAPI must
    recognize that tag and convert epoch seconds → UTC-aware datetime.
    """

    def test_integer_value_in_datetime_column_decodes_as_datetime(
        self, cluster_address: str
    ) -> None:
        expected = datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=datetime.UTC)
        epoch = int(expected.timestamp())
        with connect(cluster_address, database="test_unixtime") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS ut_test (id INTEGER PRIMARY KEY, ts DATETIME)"
            )
            cursor.execute("DELETE FROM ut_test")
            # Bind an int — SQLite stores it as INTEGER affinity; server then
            # tags the column as DQLITE_UNIXTIME on readback.
            cursor.execute("INSERT INTO ut_test (ts) VALUES (?)", [epoch])
            cursor.execute("SELECT ts FROM ut_test")
            (value,) = cursor.fetchone()  # type: ignore[misc]
            assert isinstance(value, datetime.datetime)
            assert value == expected
            cursor.execute("DROP TABLE ut_test")


@pytest.mark.integration
class TestDescriptionTypeCode:
    """cursor.description[i][1] carries the wire ValueType integer code."""

    def test_iso8601_column_description(self, cluster_address: str) -> None:
        with connect(cluster_address, database="test_desc_iso") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS desc_iso (id INTEGER PRIMARY KEY, ts DATETIME)"
            )
            cursor.execute("DELETE FROM desc_iso")
            cursor.execute("INSERT INTO desc_iso (ts) VALUES (?)", ["2024-01-15 10:30:45"])
            cursor.execute("SELECT id, ts FROM desc_iso")
            cursor.fetchall()
            assert cursor.description is not None
            names = [c[0] for c in cursor.description]
            type_codes = [c[1] for c in cursor.description]
            assert names == ["id", "ts"]
            assert type_codes[0] == int(ValueType.INTEGER)
            assert type_codes[1] == int(ValueType.ISO8601)
            cursor.execute("DROP TABLE desc_iso")


@pytest.mark.integration
class TestAsyncCursorDateTime:
    """AsyncCursor goes through the same conversion layer."""

    def test_async_datetime_roundtrip(self, cluster_address: str) -> None:
        dt = datetime.datetime(2024, 6, 15, 12, 30, 45)  # noqa: DTZ001 - naive is the point

        async def scenario() -> datetime.datetime:
            async with AsyncConnection(cluster_address, database="test_async_dt") as conn:
                cursor = conn.cursor()
                await cursor.execute(
                    "CREATE TABLE IF NOT EXISTS async_dt (id INTEGER PRIMARY KEY, ts DATETIME)"
                )
                await cursor.execute("DELETE FROM async_dt")
                await cursor.execute("INSERT INTO async_dt (ts) VALUES (?)", [dt])
                await cursor.execute("SELECT ts FROM async_dt")
                row = await cursor.fetchone()
                await cursor.execute("DROP TABLE async_dt")
                assert row is not None
                return row[0]  # type: ignore[no-any-return]

        value = asyncio.run(scenario())
        assert isinstance(value, datetime.datetime)
        assert value == dt
        assert value.tzinfo is None


@pytest.mark.integration
class TestHeterogeneousPerRowTypes:
    """SQLite is dynamically typed. When a ``DATETIME`` column stores
    rows with different underlying storage classes — a TEXT-stored
    ISO8601 value next to an INTEGER-stored UNIX epoch — the server
    tags each row with its own wire ``ValueType``. The cursor must
    dispatch per row, not collapse to row 0's type.

    Without per-row dispatch, row 1 (UNIXTIME-tagged) would be sent
    through ``_datetime_from_iso8601`` because row 0 was ISO8601 —
    which either raises ``DataError`` or returns nonsense, depending
    on the value.
    """

    def test_datetime_column_mixes_iso8601_and_unixtime(self, cluster_address: str) -> None:
        """Row 0 stored as ISO8601 TEXT, row 1 as INTEGER. Each must
        decode through the converter the server tagged it with.
        """
        with connect(cluster_address, database="test_per_row_types") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS per_row_mix (id INTEGER PRIMARY KEY, v DATETIME)"
            )
            cursor.execute("DELETE FROM per_row_mix")
            cursor.execute("INSERT INTO per_row_mix (v) VALUES (?)", ["2024-01-15 10:30:45"])
            cursor.execute("INSERT INTO per_row_mix (v) VALUES (?)", [42])
            cursor.execute("SELECT v FROM per_row_mix ORDER BY id")
            rows = cursor.fetchall()
            cursor.execute("DROP TABLE per_row_mix")

            assert len(rows) == 2

            # Row 0: ISO8601 text → naive datetime.
            assert isinstance(rows[0][0], datetime.datetime), (
                f"row 0 should decode as datetime, got {type(rows[0][0]).__name__}: {rows[0][0]!r}"
            )
            assert rows[0][0] == datetime.datetime(2024, 1, 15, 10, 30, 45)  # noqa: DTZ001
            assert rows[0][0].tzinfo is None

            # Row 1: UNIXTIME integer → UTC-aware datetime at epoch+42s.
            # With pre-fix first-row dispatch, the cursor would have
            # tried to parse an int through ``_datetime_from_iso8601``
            # and raised DataError.
            assert isinstance(rows[1][0], datetime.datetime), (
                f"row 1 should decode as datetime, got {type(rows[1][0]).__name__}: {rows[1][0]!r}"
            )
            expected = datetime.datetime(1970, 1, 1, 0, 0, 42, tzinfo=datetime.UTC)
            assert rows[1][0] == expected

    def test_datetime_column_reverse_order(self, cluster_address: str) -> None:
        """Inverse order: row 0 stored as INTEGER, row 1 as ISO8601
        TEXT. If the cursor collapsed to row 0's types, row 1 would
        remain a string instead of becoming a datetime.
        """
        with connect(cluster_address, database="test_per_row_types_inv") as conn:
            cursor = conn.cursor()
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS per_row_mix_inv (id INTEGER PRIMARY KEY, v DATETIME)"
            )
            cursor.execute("DELETE FROM per_row_mix_inv")
            cursor.execute("INSERT INTO per_row_mix_inv (v) VALUES (?)", [100])
            cursor.execute("INSERT INTO per_row_mix_inv (v) VALUES (?)", ["2024-06-15 08:00:00"])
            cursor.execute("SELECT v FROM per_row_mix_inv ORDER BY id")
            rows = cursor.fetchall()
            cursor.execute("DROP TABLE per_row_mix_inv")

            assert len(rows) == 2
            # Row 0: UNIXTIME → aware datetime at epoch+100s.
            assert isinstance(rows[0][0], datetime.datetime)
            assert rows[0][0] == datetime.datetime(1970, 1, 1, 0, 1, 40, tzinfo=datetime.UTC)
            # Row 1: ISO8601 text → naive datetime.
            assert isinstance(rows[1][0], datetime.datetime), (
                f"row 1 should decode as datetime, got {type(rows[1][0]).__name__}: {rows[1][0]!r}"
            )
            assert rows[1][0] == datetime.datetime(2024, 6, 15, 8, 0, 0)  # noqa: DTZ001
            assert rows[1][0].tzinfo is None
