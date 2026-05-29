"""``close()`` satisfies PEP 249 §6.1's implicit-rollback contract. dqlite needs no client-side
ROLLBACK: the server tears down the per-connection transaction when the TCP connection closes."""

from __future__ import annotations

import uuid

import dqlitedbapi
from dqlitedbapi.aio import aconnect


def _fresh_db_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def test_async_close_rolls_back_uncommitted_transaction(
    cluster_address: str,
) -> None:
    """BEGIN+INSERT then close without commit; a re-opened connection must not see the row."""
    db_name = _fresh_db_name("test_close_rb_async")

    setup = await aconnect(cluster_address, database=db_name)
    try:
        cur = setup.cursor()
        await cur.execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")
        cur.close()
        await setup.commit()
    finally:
        await setup.close()

    conn = await aconnect(cluster_address, database=db_name)
    cur = conn.cursor()
    await cur.execute("BEGIN")
    await cur.execute("INSERT INTO t (x) VALUES (?)", (42,))
    cur.close()
    assert conn.in_transaction
    await conn.close()

    check = await aconnect(cluster_address, database=db_name)
    try:
        cur = check.cursor()
        await cur.execute("SELECT COUNT(*) FROM t")
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == 0, (
            f"PEP 249 §6.1 violated: close() must result in an implicit "
            f"rollback (via server-side teardown) of the uncommitted "
            f"INSERT; found {row[0]} row(s)"
        )
    finally:
        await check.close()


def test_sync_close_rolls_back_uncommitted_transaction(
    cluster_address: str,
) -> None:
    """Sync sibling of the async test."""
    db_name = _fresh_db_name("test_close_rb_sync")

    setup = dqlitedbapi.connect(cluster_address, database=db_name)
    try:
        cur = setup.cursor()
        cur.execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")
        cur.close()
        setup.commit()
    finally:
        setup.close()

    conn = dqlitedbapi.connect(cluster_address, database=db_name)
    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.execute("INSERT INTO t (x) VALUES (?)", (42,))
    cur.close()
    assert conn.in_transaction
    conn.close()

    check = dqlitedbapi.connect(cluster_address, database=db_name)
    try:
        cur = check.cursor()
        cur.execute("SELECT COUNT(*) FROM t")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 0, (
            f"PEP 249 §6.1 violated (sync): close() must result in an "
            f"implicit rollback (via server-side teardown) of the "
            f"uncommitted INSERT; found {row[0]} row(s)"
        )
    finally:
        check.close()


async def test_async_close_after_commit_preserves_committed_row(
    cluster_address: str,
) -> None:
    """Negative pin: a committed row must survive close (no spurious post-commit ROLLBACK)."""
    db_name = _fresh_db_name("test_close_committed")

    setup = await aconnect(cluster_address, database=db_name)
    try:
        cur = setup.cursor()
        await cur.execute("CREATE TABLE t (x INTEGER PRIMARY KEY)")
        cur.close()
        await setup.commit()
    finally:
        await setup.close()

    conn = await aconnect(cluster_address, database=db_name)
    cur = conn.cursor()
    await cur.execute("BEGIN")
    await cur.execute("INSERT INTO t (x) VALUES (?)", (99,))
    cur.close()
    await conn.commit()
    assert not conn.in_transaction
    await conn.close()

    check = await aconnect(cluster_address, database=db_name)
    try:
        cur = check.cursor()
        await cur.execute("SELECT COUNT(*) FROM t WHERE x = 99")
        row = await cur.fetchone()
        assert row is not None
        assert row[0] == 1, (
            f"committed row vanished — future regression in close() may "
            f"have introduced a spurious ROLLBACK; found {row[0]} row(s) "
            f"for x=99"
        )
    finally:
        await check.close()
