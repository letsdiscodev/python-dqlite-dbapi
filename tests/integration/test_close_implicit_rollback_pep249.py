"""Pin: ``Connection.close()`` / ``AsyncConnection.close()`` satisfies
PEP 249 §6.1's implicit-rollback contract.

PEP 249 §6.1: "Note that closing a connection without committing the
changes first will cause an implicit rollback to be performed."

Most drivers implement this with an explicit client-side ROLLBACK
round-trip during close. dqlite's server tears down the connection-
bound transaction when the TCP connection closes (the server-side
gateway state is per-connection), so the spec's contract is satisfied
transparently — no client-side ROLLBACK round-trip is needed.

This test pins the externally observable contract: after a BEGIN +
INSERT + close (no commit), a fresh connection MUST NOT see the row.
The test guards against a future refactor that breaks the server-side
teardown semantics.
"""

from __future__ import annotations

import uuid

import dqlitedbapi
from dqlitedbapi.aio import aconnect


def _fresh_db_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


async def test_async_close_rolls_back_uncommitted_transaction(
    cluster_address: str,
) -> None:
    """Open conn, BEGIN+INSERT, close WITHOUT commit. Re-open and
    SELECT — the row must not be present (PEP 249 §6.1 contract via
    server-side teardown)."""
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
    """Sync sibling of the async test. Same contract."""
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
    """Negative pin: a committed transaction's row MUST survive
    close. Guards against a hypothetical future refactor adding a
    client-side ROLLBACK that fires after commit and rolls back the
    already-committed write (idempotency check)."""
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
