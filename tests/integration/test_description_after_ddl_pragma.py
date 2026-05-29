"""``cursor.description is None`` after CREATE/DROP/ALTER/REINDEX DDL;
``PRAGMA table_info`` is the positive case (row-returning, description non-None)."""

from __future__ import annotations

import os
from collections.abc import Generator

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect


@pytest.fixture
def conn() -> Generator[dqlitedbapi.Connection]:
    address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
    c = dqlitedbapi.connect(address, timeout=5.0)
    try:
        yield c
    finally:
        c.close()


@pytest.mark.integration
class TestDescriptionAfterDdlPragma:
    def test_description_is_none_after_ddl(self, conn: dqlitedbapi.Connection) -> None:
        cur = conn.cursor()
        try:
            cur.execute("DROP TABLE IF EXISTS t_ddl_pragma")
            assert cur.description is None

            cur.execute("CREATE TABLE t_ddl_pragma (id INTEGER PRIMARY KEY, a INTEGER)")
            assert cur.description is None

            cur.execute("CREATE INDEX ix_t_ddl_pragma_a ON t_ddl_pragma(a)")
            assert cur.description is None

            cur.execute("ALTER TABLE t_ddl_pragma ADD COLUMN b INTEGER")
            assert cur.description is None

            cur.execute("REINDEX t_ddl_pragma")
            assert cur.description is None

            cur.execute("DROP INDEX ix_t_ddl_pragma_a")
            assert cur.description is None

            cur.execute("PRAGMA table_info(t_ddl_pragma)")
            assert cur.description is not None
            cur.fetchall()

            cur.execute("DROP TABLE t_ddl_pragma")
            assert cur.description is None
        finally:
            cur.close()


@pytest.mark.integration
class TestAsyncDescriptionAfterDdlPragma:
    async def test_description_is_none_after_ddl(self) -> None:
        address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
        conn = await aconnect(address, timeout=5.0)
        try:
            cur = conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS t_ddl_pragma_async")
            assert cur.description is None

            await cur.execute("CREATE TABLE t_ddl_pragma_async (id INTEGER PRIMARY KEY, a INTEGER)")
            assert cur.description is None

            await cur.execute("CREATE INDEX ix_t_ddl_pragma_async_a ON t_ddl_pragma_async(a)")
            assert cur.description is None

            await cur.execute("ALTER TABLE t_ddl_pragma_async ADD COLUMN b INTEGER")
            assert cur.description is None

            await cur.execute("REINDEX t_ddl_pragma_async")
            assert cur.description is None

            await cur.execute("DROP INDEX ix_t_ddl_pragma_async_a")
            assert cur.description is None

            await cur.execute("PRAGMA table_info(t_ddl_pragma_async)")
            assert cur.description is not None
            await cur.fetchall()

            await cur.execute("DROP TABLE t_ddl_pragma_async")
            assert cur.description is None
            cur.close()
        finally:
            await conn.close()
