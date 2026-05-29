"""Fetch after a non-row-returning statement (BEGIN/COMMIT/ROLLBACK) returns
None/[] (stdlib parity), not stale rows from a prior SELECT."""

from __future__ import annotations

import pytest

import dqlitedbapi


@pytest.mark.integration
def test_fetchone_after_rollback_returns_none_per_stdlib(
    cluster_address: str,
) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_fetch_after_rollback")
        cur.execute("CREATE TABLE test_fetch_after_rollback (id INTEGER)")
        cur.execute("INSERT INTO test_fetch_after_rollback (id) VALUES (1), (2)")
        conn.commit()

        cur.execute("BEGIN")
        cur.execute("SELECT id FROM test_fetch_after_rollback")
        cur.execute("ROLLBACK")
        assert cur.description is None
        assert cur.fetchone() is None
        assert cur.fetchmany(10) == []
        assert cur.fetchall() == []


@pytest.mark.integration
def test_fetchone_after_commit_returns_none_per_stdlib(
    cluster_address: str,
) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_fetch_after_commit")
        cur.execute("CREATE TABLE test_fetch_after_commit (id INTEGER)")
        cur.execute("INSERT INTO test_fetch_after_commit (id) VALUES (1), (2)")
        conn.commit()

        cur.execute("BEGIN")
        cur.execute("SELECT id FROM test_fetch_after_commit")
        cur.execute("COMMIT")
        assert cur.description is None
        assert cur.fetchone() is None
        assert cur.fetchmany(10) == []
        assert cur.fetchall() == []


@pytest.mark.integration
def test_fetchone_after_begin_returns_none_per_stdlib(
    cluster_address: str,
) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_fetch_after_begin")
        cur.execute("CREATE TABLE test_fetch_after_begin (id INTEGER)")
        cur.execute("INSERT INTO test_fetch_after_begin (id) VALUES (1)")
        conn.commit()

        cur.execute("SELECT id FROM test_fetch_after_begin")
        cur.execute("BEGIN")
        assert cur.description is None
        assert cur.fetchone() is None
        assert cur.fetchmany(10) == []
        assert cur.fetchall() == []
        conn.rollback()


@pytest.mark.integration
async def test_async_fetchone_after_rollback_returns_none(
    cluster_address: str,
) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_async_fetch_rb")
        await cur.execute("CREATE TABLE test_async_fetch_rb (id INTEGER)")
        await cur.execute("INSERT INTO test_async_fetch_rb (id) VALUES (1)")
        await conn.commit()

        await cur.execute("BEGIN")
        await cur.execute("SELECT id FROM test_async_fetch_rb")
        await cur.execute("ROLLBACK")
        assert cur.description is None
        assert await cur.fetchone() is None
        assert await cur.fetchmany(10) == []
        assert await cur.fetchall() == []
    finally:
        await conn.close()
