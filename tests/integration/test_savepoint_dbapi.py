"""SAVEPOINT round-trip pins at the dbapi layer.

ISSUE-701 added SAVEPOINT integration coverage for the SQLAlchemy
adapter. The dbapi layer had no equivalent — a future change to the
SQL classifier or to the in-transaction flag tracking could silently
break SAVEPOINT routing.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio import aconnect


def test_sync_savepoint_roundtrip_partial_rollback(cluster_address: str) -> None:
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_sp_dbapi")
        cur.execute("CREATE TABLE test_sp_dbapi (n INTEGER PRIMARY KEY)")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO test_sp_dbapi (n) VALUES (1)")
        cur.execute("SAVEPOINT sp1")
        cur.execute("INSERT INTO test_sp_dbapi (n) VALUES (2)")
        cur.execute("ROLLBACK TO SAVEPOINT sp1")
        cur.execute("RELEASE SAVEPOINT sp1")
        cur.execute("COMMIT")

        cur.execute("SELECT n FROM test_sp_dbapi ORDER BY n")
        assert cur.fetchall() == [(1,)]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_savepoint_roundtrip_release(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_sp_dbapi_aio")
        await cur.execute("CREATE TABLE test_sp_dbapi_aio (n INTEGER PRIMARY KEY)")
        await cur.execute("BEGIN")
        await cur.execute("INSERT INTO test_sp_dbapi_aio (n) VALUES (10)")
        await cur.execute("SAVEPOINT sp_a")
        await cur.execute("INSERT INTO test_sp_dbapi_aio (n) VALUES (20)")
        await cur.execute("RELEASE SAVEPOINT sp_a")
        await cur.execute("COMMIT")

        await cur.execute("SELECT n FROM test_sp_dbapi_aio ORDER BY n")
        assert await cur.fetchall() == [(10,), (20,)]
    finally:
        await conn.close()


def test_sync_nested_savepoint(cluster_address: str) -> None:
    """sp1 → sp2 → ROLLBACK TO sp1 implicitly drops sp2.

    Pins SQLite's nested-SAVEPOINT semantics through the SA → dbapi →
    client → wire round-trip. A regression in the SQL classifier
    (``_is_row_returning`` / ``_is_dml_with_returning``) or in the
    client-layer SAVEPOINT tracker would surface here as a row-count
    mismatch.
    """
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_sp_dbapi_nested")
        cur.execute("CREATE TABLE test_sp_dbapi_nested (n INTEGER PRIMARY KEY)")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO test_sp_dbapi_nested (n) VALUES (1)")
        cur.execute("SAVEPOINT sp1")
        cur.execute("INSERT INTO test_sp_dbapi_nested (n) VALUES (2)")
        cur.execute("SAVEPOINT sp2")
        cur.execute("INSERT INTO test_sp_dbapi_nested (n) VALUES (3)")
        cur.execute("ROLLBACK TO SAVEPOINT sp1")
        cur.execute("RELEASE SAVEPOINT sp1")
        cur.execute("COMMIT")

        cur.execute("SELECT n FROM test_sp_dbapi_nested ORDER BY n")
        assert cur.fetchall() == [(1,)]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_nested_savepoint(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_sp_dbapi_nested_aio")
        await cur.execute("CREATE TABLE test_sp_dbapi_nested_aio (n INTEGER PRIMARY KEY)")
        await cur.execute("BEGIN")
        await cur.execute("INSERT INTO test_sp_dbapi_nested_aio (n) VALUES (1)")
        await cur.execute("SAVEPOINT sp1")
        await cur.execute("INSERT INTO test_sp_dbapi_nested_aio (n) VALUES (2)")
        await cur.execute("SAVEPOINT sp2")
        await cur.execute("INSERT INTO test_sp_dbapi_nested_aio (n) VALUES (3)")
        await cur.execute("ROLLBACK TO SAVEPOINT sp1")
        await cur.execute("RELEASE SAVEPOINT sp1")
        await cur.execute("COMMIT")

        await cur.execute("SELECT n FROM test_sp_dbapi_nested_aio ORDER BY n")
        assert await cur.fetchall() == [(1,)]
    finally:
        await conn.close()


def test_sync_savepoint_autobegin_persists_on_commit(cluster_address: str) -> None:
    """Bare SAVEPOINT outside an explicit BEGIN auto-begins a transaction.

    Per SQLite, a SAVEPOINT issued without an active transaction starts
    a new one and the savepoint becomes its outer frame. RELEASE of
    that outermost savepoint commits the implicit transaction.

    Pins both the row-persistence end-to-end and the client-layer
    flag tracker: ``conn.in_transaction`` must read True between
    SAVEPOINT and RELEASE, mirroring stdlib ``sqlite3``.
    """
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_sp_dbapi_autobegin")
        cur.execute("CREATE TABLE test_sp_dbapi_autobegin (n INTEGER PRIMARY KEY)")
        assert conn.in_transaction is False

        cur.execute("SAVEPOINT sp1")
        assert conn.in_transaction is True
        cur.execute("INSERT INTO test_sp_dbapi_autobegin (n) VALUES (1)")
        assert conn.in_transaction is True
        cur.execute("RELEASE SAVEPOINT sp1")
        assert conn.in_transaction is False

        cur.execute("SELECT n FROM test_sp_dbapi_autobegin")
        assert cur.fetchall() == [(1,)]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_savepoint_autobegin_persists_on_commit(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_sp_dbapi_autobegin_aio")
        await cur.execute("CREATE TABLE test_sp_dbapi_autobegin_aio (n INTEGER PRIMARY KEY)")
        assert conn.in_transaction is False

        await cur.execute("SAVEPOINT sp1")
        assert conn.in_transaction is True
        await cur.execute("INSERT INTO test_sp_dbapi_autobegin_aio (n) VALUES (1)")
        assert conn.in_transaction is True
        await cur.execute("RELEASE SAVEPOINT sp1")
        assert conn.in_transaction is False

        await cur.execute("SELECT n FROM test_sp_dbapi_autobegin_aio")
        assert await cur.fetchall() == [(1,)]
    finally:
        await conn.close()
