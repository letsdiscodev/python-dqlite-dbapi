"""Integration tests for previously-uncovered territory.

- Large result set (forces continuation frames) + large BLOB.
- Unicode in identifiers + emoji in TEXT.
- Multi-statement SQL is rejected with a specific error.
- rowcount semantics after SELECT / empty SELECT.
"""

import pytest

import dqliteclient.exceptions
from dqlitedbapi import connect
from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import OperationalError


@pytest.mark.integration
class TestRowcountAfterSelect:
    """Pins the rowcount contract for SELECT statements.

    dqlite knows the full result set at execute time (rows arrive in
    the same response + continuation frames, not lazily), so the
    driver reports ``len(rows)`` — the PEP 249 literal reading of
    "number of rows that the last execute*() produced". This diverges
    intentionally from stdlib ``sqlite3``'s ``-1`` convention and must
    stay pinned: a refactor of the execute path that flipped to ``-1``
    would be a silent semantics change.
    """

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
        """Insert 5k rows and read them all back; exercises continuation frames
        on most server chunk sizes."""
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
            (value,) = c.fetchone()
            assert value == payload
            c.execute("DROP TABLE big_blob")


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
            (value,) = c.fetchone()
            assert value == "hello 🚀 world"
            assert c.description is not None
            assert c.description[0][0] == "☕"
            c.execute('DROP TABLE "café"')

    def test_non_bmp_codepoint_round_trip(self, cluster_address: str) -> None:
        """4-byte UTF-8 codepoint survives the wire round-trip."""
        grinning = "\U0001f600"  # 😀
        payload = grinning * 1000  # 4000 bytes of non-BMP codepoints
        with connect(cluster_address, database="test_non_bmp") as conn:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS nbmp")
            c.execute("CREATE TABLE nbmp (id INTEGER PRIMARY KEY, s TEXT)")
            c.execute("INSERT INTO nbmp (s) VALUES (?)", [payload])
            conn.commit()
            c.execute("SELECT s FROM nbmp")
            (value,) = c.fetchone()
            assert value == payload
            c.execute("DROP TABLE nbmp")


@pytest.mark.integration
class TestMultiStatementRejection:
    def test_semicolon_separated_select_rejected(self, cluster_address: str) -> None:
        """dqlite rejects multi-statement SQL — a real deviation from stdlib
        sqlite3 that applications commonly trip over. Pinning the error so
        regressions don't silently change the behavior."""
        # The error class is dqliteclient.OperationalError today (the
        # DBAPI doesn't wrap); either is acceptable for now.
        expected = (OperationalError, dqliteclient.exceptions.OperationalError)
        with connect(cluster_address, database="test_multi_stmt") as conn:
            c = conn.cursor()
            with pytest.raises(expected, match="nonempty statement tail"):
                c.execute("SELECT 1; SELECT 2;")


@pytest.mark.integration
class TestUnsupportedBindParameterTypes:
    """Types the wire codec cannot encode (Decimal, Fraction, complex,
    custom objects) must surface at the DBAPI boundary as ``DataError``,
    not as a wire-layer ``EncodeError`` and not as a silent misencoding.
    Pins the PEP 249 "all DB errors funnel through Error" contract through
    the DqliteConnection._run_protocol wrapper, which maps EncodeError
    into the client-layer DataError that the DBAPI re-exports.
    """

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


@pytest.mark.integration
class TestCursorDescriptionEdgeCases:
    """``cursor.description`` invariants after queries whose result sets
    are either empty or contain only NULLs. PEP 249 requires description
    to reflect the query's column shape regardless of row count.
    """

    def test_description_populated_for_empty_resultset(self, cluster_address: str) -> None:
        """A SELECT that returns zero rows still populates description with
        column names. ``type_code`` is None for each column because the
        wire layer sources it from the first row's type header and there
        are no rows — this is the current contract; pin it.
        """
        with connect(cluster_address, database="test_desc_empty") as conn:
            c = conn.cursor()
            c.execute("CREATE TABLE IF NOT EXISTS desc_empty (a INTEGER, b TEXT)")
            c.execute("DELETE FROM desc_empty")
            c.execute("SELECT a, b FROM desc_empty WHERE 1=0")

            assert c.description is not None
            assert len(c.description) == 2
            assert [col[0] for col in c.description] == ["a", "b"]
            # No rows → no per-row type header → type_code is None on every column.
            assert [col[1] for col in c.description] == [None, None]
            assert c.fetchall() == []

    def test_description_typecode_when_only_row_is_all_null(self, cluster_address: str) -> None:
        """A row of all-NULLs sets every column's type nibble to NULL in the
        wire frame, which propagates to ``description[i][1]`` as
        ``ValueType.NULL`` (= 5). Locked in so a future refactor (e.g.
        falling back to declared column types when the first row is all
        NULLs) is a deliberate decision, not a silent drift.
        """
        from dqlitewire.constants import ValueType

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
            # Current contract: per-row NULL override flips the type
            # nibble, so declared column types are lost when the sample
            # row is all-NULLs. ValueType.NULL == 5.
            assert [col[1] for col in c.description] == [int(ValueType.NULL), int(ValueType.NULL)]
            assert c.fetchall() == [(None, None)]
