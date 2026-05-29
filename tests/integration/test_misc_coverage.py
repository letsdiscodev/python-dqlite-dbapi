"""Integration tests for previously-uncovered territory."""

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.integration
class TestRowcountAfterSelect:
    """SELECT rowcount reports ``len(rows)``; diverges from stdlib sqlite3's ``-1``."""

    def test_rowcount_after_select_reports_row_count(self, cluster_address: str) -> None:
        with connect(cluster_address, database="test_rowcount_sel") as conn:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS rc")
            c.execute("CREATE TABLE rc (x INTEGER)")
            c.execute("INSERT INTO rc VALUES (1), (2), (3)")
            conn.commit()

            c.execute("SELECT x FROM rc ORDER BY x")
            assert c.rowcount == 3, (
                "SELECT rowcount pins the 'rows produced' reading, not sqlite3's -1."
            )
            c.execute("DROP TABLE rc")

    def test_rowcount_after_empty_select_is_zero(self, cluster_address: str) -> None:
        with connect(cluster_address, database="test_rowcount_empty") as conn:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS rc_empty")
            c.execute("CREATE TABLE rc_empty (x INTEGER)")
            conn.commit()

            c.execute("SELECT x FROM rc_empty")
            assert c.rowcount == 0
            c.execute("DROP TABLE rc_empty")

    async def test_async_rowcount_after_select_reports_row_count(
        self, cluster_address: str
    ) -> None:
        async with await aconnect(cluster_address, database="test_rowcount_aio") as conn:
            c = conn.cursor()
            await c.execute("DROP TABLE IF EXISTS rc_aio")
            await c.execute("CREATE TABLE rc_aio (x INTEGER)")
            await c.execute("INSERT INTO rc_aio VALUES (1), (2), (3), (4)")
            await conn.commit()

            await c.execute("SELECT x FROM rc_aio ORDER BY x")
            assert c.rowcount == 4
            await c.execute("DROP TABLE rc_aio")


@pytest.mark.integration
class TestLargeData:
    def test_large_result_set_round_trips(self, cluster_address: str) -> None:
        """Insert 5k rows and read them back; exercises continuation frames."""
        with connect(cluster_address, database="test_large") as conn:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS many")
            c.execute("CREATE TABLE many (i INTEGER PRIMARY KEY, s TEXT)")
            for i in range(5000):
                c.execute("INSERT INTO many (i, s) VALUES (?, ?)", [i, f"row-{i}"])
            conn.commit()

            c.execute("SELECT i, s FROM many ORDER BY i")
            rows = c.fetchall()
            assert len(rows) == 5000
            assert rows[0] == (0, "row-0")
            assert rows[-1] == (4999, "row-4999")
            c.execute("DROP TABLE many")

    def test_multi_megabyte_blob(self, cluster_address: str) -> None:
        """A 2 MiB BLOB round-trips byte-for-byte."""
        payload = bytes(range(256)) * 8192  # 2 MiB of 0x00..0xFF pattern
        assert len(payload) == 2 * 1024 * 1024
        with connect(cluster_address, database="test_blob") as conn:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS big_blob")
            c.execute("CREATE TABLE big_blob (id INTEGER PRIMARY KEY, data BLOB)")
            c.execute("INSERT INTO big_blob (data) VALUES (?)", [payload])
            conn.commit()
            c.execute("SELECT data FROM big_blob")
            (value,) = c.fetchone()  # type: ignore[misc]
            assert value == payload
            c.execute("DROP TABLE big_blob")

    def test_binary_constructor_blob_round_trip(self, cluster_address: str) -> None:
        """``Binary(...)`` (= ``memoryview``) round-trips as BLOB; readback is ``bytes``."""
        import dqlitedbapi

        payload = b"binary-constructor-round-trip" * 40
        with connect(cluster_address, database="test_binary_ctor") as conn:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS bin_ctor")
            c.execute("CREATE TABLE bin_ctor (id INTEGER PRIMARY KEY, data BLOB)")
            c.execute("INSERT INTO bin_ctor (data) VALUES (?)", [dqlitedbapi.Binary(payload)])
            conn.commit()
            c.execute("SELECT data FROM bin_ctor")
            (value,) = c.fetchone()  # type: ignore[misc]
            assert value == payload
            c.execute("DROP TABLE bin_ctor")


@pytest.mark.integration
class TestUnicode:
    def test_unicode_identifier_and_emoji_value(self, cluster_address: str) -> None:
        with connect(cluster_address, database="test_unicode") as conn:
            c = conn.cursor()
            c.execute('DROP TABLE IF EXISTS "café"')
            c.execute('CREATE TABLE "café" (id INTEGER PRIMARY KEY, "☕" TEXT)')
            c.execute('INSERT INTO "café" ("☕") VALUES (?)', ["hello 🚀 world"])
            conn.commit()
            c.execute('SELECT "☕" FROM "café"')
            (value,) = c.fetchone()  # type: ignore[misc]
            assert value == "hello 🚀 world"
            assert c.description is not None
            assert c.description[0][0] == "☕"
            c.execute('DROP TABLE "café"')

    def test_non_bmp_codepoint_round_trip(self, cluster_address: str) -> None:
        """4-byte UTF-8 codepoint survives the wire round-trip."""
        grinning = "\U0001f600"  # 😀
        payload = grinning * 1000
        with connect(cluster_address, database="test_non_bmp") as conn:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS nbmp")
            c.execute("CREATE TABLE nbmp (id INTEGER PRIMARY KEY, s TEXT)")
            c.execute("INSERT INTO nbmp (s) VALUES (?)", [payload])
            conn.commit()
            c.execute("SELECT s FROM nbmp")
            (value,) = c.fetchone()  # type: ignore[misc]
            assert value == payload
            c.execute("DROP TABLE nbmp")


@pytest.mark.integration
class TestMultiStatementRejection:
    def test_semicolon_separated_select_rejected(self, cluster_address: str) -> None:
        """Multi-statement SQL is rejected client-side as ProgrammingError before any
        wire round-trip (deviates from stdlib sqlite3, which allows it)."""
        with connect(cluster_address, database="test_multi_stmt") as conn:
            c = conn.cursor()
            with pytest.raises(ProgrammingError, match="one statement at a time"):
                c.execute("SELECT 1; SELECT 2;")


@pytest.mark.integration
class TestUnsupportedBindParameterTypes:
    """Types the wire codec cannot encode must surface as ``DataError``, not a
    wire-layer ``EncodeError`` or silent misencoding."""

    def test_decimal_rejected_as_data_error(self, cluster_address: str) -> None:
        from decimal import Decimal

        from dqlitedbapi.exceptions import DataError

        with connect(cluster_address, database="test_bind_types") as conn:
            c = conn.cursor()
            with pytest.raises(DataError):
                c.execute("SELECT ?", [Decimal("3.14")])

    def test_fraction_rejected_as_data_error(self, cluster_address: str) -> None:
        from fractions import Fraction

        from dqlitedbapi.exceptions import DataError

        with connect(cluster_address, database="test_bind_types") as conn:
            c = conn.cursor()
            with pytest.raises(DataError):
                c.execute("SELECT ?", [Fraction(1, 3)])

    def test_complex_rejected_as_data_error(self, cluster_address: str) -> None:
        from dqlitedbapi.exceptions import DataError

        with connect(cluster_address, database="test_bind_types") as conn:
            c = conn.cursor()
            with pytest.raises(DataError):
                c.execute("SELECT ?", [complex(1, 2)])

    def test_uuid_rejected_as_data_error(self, cluster_address: str) -> None:
        """UUID is not wire-recognized; must surface as DataError (psycopg/asyncpg accept it)."""
        from uuid import UUID

        from dqlitedbapi.exceptions import DataError

        with connect(cluster_address, database="test_bind_types") as conn:
            c = conn.cursor()
            with pytest.raises(DataError):
                c.execute(
                    "SELECT ?",
                    [UUID("12345678-1234-5678-1234-567812345678")],
                )

    def test_path_rejected_as_data_error(self, cluster_address: str) -> None:
        """``pathlib.Path`` must surface as DataError so the caller is steered to ``str(path)``."""
        from pathlib import Path

        from dqlitedbapi.exceptions import DataError

        with connect(cluster_address, database="test_bind_types") as conn:
            c = conn.cursor()
            with pytest.raises(DataError):
                c.execute("SELECT ?", [Path("/tmp/foo")])

    def test_array_array_rejected_as_data_error(self, cluster_address: str) -> None:
        """``array.array`` is bytes-like but not an accepted BLOB input; must be DataError."""
        from array import array

        from dqlitedbapi.exceptions import DataError

        with connect(cluster_address, database="test_bind_types") as conn:
            c = conn.cursor()
            with pytest.raises(DataError):
                c.execute("SELECT ?", [array("b", b"hello")])

    def test_intenum_round_trips_as_int(self, cluster_address: str) -> None:
        """``enum.IntEnum`` is an ``int`` subclass; the wire encoder accepts it as INTEGER."""
        from enum import IntEnum

        class _Color(IntEnum):
            RED = 1

        with connect(cluster_address, database="test_bind_types") as conn:
            c = conn.cursor()
            c.execute("SELECT ?", [_Color.RED])
            row = c.fetchone()
            assert row == (1,) or row == (int(_Color.RED),)


@pytest.mark.integration
class TestBindBoundaryDataErrors:
    """Wire-cap boundary inputs must surface as ``DataError``, not a raw
    ValueError / EncodeError leaking from the wire layer."""

    @pytest.mark.parametrize(
        "value",
        [2**63, -(2**63) - 1, 2**63 + 1, 2**100, -(2**100)],
    )
    def test_bind_int_over_int64_raises_data_error(self, cluster_address: str, value: int) -> None:
        from dqlitedbapi.exceptions import DataError

        with connect(cluster_address, database="test_bind_overflow") as conn:
            c = conn.cursor()
            with pytest.raises(DataError):
                c.execute("SELECT ?", [value])

    @pytest.mark.parametrize("value", [2**63 - 1, -(2**63), 0, 1, -1])
    def test_bind_int_at_int64_boundary_succeeds(self, cluster_address: str, value: int) -> None:
        with connect(cluster_address, database="test_bind_overflow") as conn:
            c = conn.cursor()
            c.execute("SELECT ?", [value])
            assert c.fetchone() == (value,)

    def test_bind_blob_over_cap_raises_data_error(self, cluster_address: str) -> None:
        """A bind value over the wire BLOB cap must surface as ``DataError``,
        never a raw EncodeError or silent truncation."""
        from dqlitedbapi.exceptions import DataError
        from dqlitewire.types import _MAX_BLOB_SIZE

        big = b"x" * (_MAX_BLOB_SIZE + 1)
        with connect(cluster_address, database="test_bind_overflow") as conn:
            c = conn.cursor()
            with pytest.raises(DataError):
                c.execute("SELECT ?", [big])


@pytest.mark.integration
class TestCursorDescriptionEdgeCases:
    """``cursor.description`` must reflect column shape even for empty / all-NULL result sets."""

    def test_description_populated_for_empty_resultset(self, cluster_address: str) -> None:
        """Zero-row SELECT still populates description; type_code is UNKNOWN since
        the wire sources it from the (absent) first row's type header."""
        from dqlitedbapi import UNKNOWN

        with connect(cluster_address, database="test_desc_empty") as conn:
            c = conn.cursor()
            c.execute("CREATE TABLE IF NOT EXISTS desc_empty (a INTEGER, b TEXT)")
            c.execute("DELETE FROM desc_empty")
            c.execute("SELECT a, b FROM desc_empty WHERE 1=0")

            assert c.description is not None
            assert len(c.description) == 2
            assert [col[0] for col in c.description] == ["a", "b"]
            assert [col[1] for col in c.description] == [UNKNOWN, UNKNOWN]
            assert c.fetchall() == []

    def test_description_typecode_when_only_row_is_all_null(self, cluster_address: str) -> None:
        """All-NULL columns map to the UNKNOWN Type Object sentinel: NULL is not one of
        PEP 249's five Type Objects, and a raw None type_code violates §6.1.2."""
        from dqlitedbapi import UNKNOWN

        with connect(cluster_address, database="test_desc_nulls") as conn:
            c = conn.cursor()
            c.execute("CREATE TABLE IF NOT EXISTS desc_nulls (a INTEGER, b TEXT)")
            c.execute("DELETE FROM desc_nulls")
            c.execute("INSERT INTO desc_nulls (a, b) VALUES (NULL, NULL)")
            conn.commit()
            c.execute("SELECT a, b FROM desc_nulls")

            assert c.description is not None
            assert len(c.description) == 2
            assert [col[0] for col in c.description] == ["a", "b"]
            assert [col[1] for col in c.description] == [UNKNOWN, UNKNOWN]
            assert c.fetchall() == [(None, None)]


@pytest.mark.integration
class TestDescriptionNoneAfterDML:
    """PEP 249: ``cursor.description`` must be ``None`` after any non-row-returning
    statement (CREATE / INSERT / UPDATE / DELETE / DROP)."""

    def test_description_is_none_after_dml(self, cluster_address: str) -> None:
        with connect(cluster_address, database="test_desc_dml") as conn:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS dt")
            c.execute("CREATE TABLE dt (x INTEGER)")
            assert c.description is None, "CREATE TABLE must leave description None"
            c.execute("INSERT INTO dt VALUES (1)")
            assert c.description is None, "INSERT (no RETURNING) must leave description None"
            c.execute("UPDATE dt SET x = 2")
            assert c.description is None, "UPDATE must leave description None"
            c.execute("DELETE FROM dt WHERE x = 2")
            assert c.description is None, "DELETE must leave description None"
            c.execute("DROP TABLE dt")
            assert c.description is None, "DROP TABLE must leave description None"

    async def test_async_description_is_none_after_dml(self, cluster_address: str) -> None:
        async with await aconnect(cluster_address, database="test_desc_dml_aio") as conn:
            c = conn.cursor()
            await c.execute("DROP TABLE IF EXISTS dt_aio")
            await c.execute("CREATE TABLE dt_aio (x INTEGER)")
            assert c.description is None
            await c.execute("INSERT INTO dt_aio VALUES (1)")
            assert c.description is None
            await c.execute("UPDATE dt_aio SET x = 2")
            assert c.description is None
            await c.execute("DELETE FROM dt_aio WHERE x = 2")
            assert c.description is None
            await c.execute("DROP TABLE dt_aio")
            assert c.description is None
