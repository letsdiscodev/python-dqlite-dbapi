"""A BOOLEAN column returns Python ``bool`` for non-NULL cells; NULL reads back as ``None``,
never ``False``."""

import asyncio

import pytest

from dqlitedbapi import NUMBER, connect
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitewire.constants import ValueType


@pytest.mark.integration
class TestBooleanRoundTrip:
    def test_boolean_null_true_false_roundtrip(self, cluster_address: str) -> None:
        with connect(cluster_address, database="test_bool") as conn:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS bool_rt (id INTEGER PRIMARY KEY, b BOOLEAN)")
            cursor.execute("DELETE FROM bool_rt")
            cursor.execute("INSERT INTO bool_rt (id, b) VALUES (1, NULL)")
            cursor.execute("INSERT INTO bool_rt (id, b) VALUES (2, 1)")
            cursor.execute("INSERT INTO bool_rt (id, b) VALUES (3, 0)")
            cursor.execute("SELECT b FROM bool_rt ORDER BY id")
            rows = cursor.fetchall()
            assert cursor.description is not None
            # The BOOLEAN wire code resolves through the NUMBER Type Object.
            assert cursor.description[0][1] == int(ValueType.BOOLEAN)
            assert cursor.description[0][1] == NUMBER
            cursor.execute("DROP TABLE bool_rt")

            assert len(rows) == 3
            # Identity (`is`) checks: a 0/1 int regression would pass under ``==``.
            assert rows[0][0] is None
            assert rows[1][0] is True
            assert rows[2][0] is False


@pytest.mark.integration
class TestAsyncBooleanRoundTrip:
    def test_async_boolean_null_true_false_roundtrip(self, cluster_address: str) -> None:
        async def scenario() -> tuple[object, list[tuple[object, ...]]]:
            async with AsyncConnection(cluster_address, database="test_bool_async") as conn:
                cursor = conn.cursor()
                await cursor.execute(
                    "CREATE TABLE IF NOT EXISTS bool_rt_async (id INTEGER PRIMARY KEY, b BOOLEAN)"
                )
                await cursor.execute("DELETE FROM bool_rt_async")
                await cursor.execute("INSERT INTO bool_rt_async (id, b) VALUES (1, NULL)")
                await cursor.execute("INSERT INTO bool_rt_async (id, b) VALUES (2, 1)")
                await cursor.execute("INSERT INTO bool_rt_async (id, b) VALUES (3, 0)")
                await cursor.execute("SELECT b FROM bool_rt_async ORDER BY id")
                rows = await cursor.fetchall()
                type_code = cursor.description[0][1] if cursor.description else None
                await cursor.execute("DROP TABLE bool_rt_async")
                return type_code, rows

        type_code, rows = asyncio.run(scenario())
        assert type_code == int(ValueType.BOOLEAN)
        assert len(rows) == 3
        assert rows[0][0] is None
        assert rows[1][0] is True
        assert rows[2][0] is False
