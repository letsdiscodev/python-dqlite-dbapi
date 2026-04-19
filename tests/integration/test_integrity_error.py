"""Server-side constraint violations must surface as PEP 249 IntegrityError."""

import pytest

import dqlitedbapi
from dqlitedbapi import IntegrityError
from dqlitedbapi.aio import aconnect


@pytest.mark.integration
def test_sync_unique_violation_raises_integrity_error(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS integ_unique")
        cur.execute("CREATE TABLE integ_unique (id INTEGER PRIMARY KEY, x TEXT UNIQUE)")
        cur.execute("INSERT INTO integ_unique (x) VALUES ('a')")
        with pytest.raises(IntegrityError) as exc_info:
            cur.execute("INSERT INTO integ_unique (x) VALUES ('a')")
        # SQLite constraint codes share ``code & 0xFF == 19``.
        code = getattr(exc_info.value, "code", None)
        assert code is not None
        assert code & 0xFF == 19
        conn.rollback()


@pytest.mark.integration
def test_sync_not_null_violation_raises_integrity_error(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS integ_notnull")
        cur.execute("CREATE TABLE integ_notnull (id INTEGER PRIMARY KEY, x TEXT NOT NULL)")
        with pytest.raises(IntegrityError):
            cur.execute("INSERT INTO integ_notnull (x) VALUES (NULL)")
        conn.rollback()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_unique_violation_raises_integrity_error(cluster_address: str) -> None:
    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS integ_unique_async")
        await cur.execute("CREATE TABLE integ_unique_async (id INTEGER PRIMARY KEY, x TEXT UNIQUE)")
        await cur.execute("INSERT INTO integ_unique_async (x) VALUES ('a')")
        with pytest.raises(IntegrityError):
            await cur.execute("INSERT INTO integ_unique_async (x) VALUES ('a')")
        await conn.rollback()
    finally:
        await conn.close()
