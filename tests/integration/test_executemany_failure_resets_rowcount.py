"""When executemany fails mid-batch, cursor.rowcount must report -1
("undetermined") rather than the last successful iteration's value."""

from __future__ import annotations

import pytest

from dqlitedbapi import IntegrityError, connect
from dqlitedbapi.aio import aconnect


def test_sync_executemany_failure_clears_rowcount(cluster_address: str) -> None:
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_em_fail")
        cur.execute("CREATE TABLE test_em_fail (id INTEGER PRIMARY KEY)")
        # Seed a pre-batch INSERT so the snapshot/restore semantics of the
        # BaseException arm are observable; 42 is the pre-batch rowid.
        cur.execute("INSERT INTO test_em_fail (id) VALUES (?)", (42,))
        pre_batch_lastrowid = cur.lastrowid
        assert pre_batch_lastrowid is not None
        with pytest.raises(IntegrityError):
            # Third row collides with the first; the loop fails mid-batch.
            cur.executemany(
                "INSERT INTO test_em_fail (id) VALUES (?)",
                [(1,), (2,), (1,)],
            )
        assert cur.rowcount == -1
        # lastrowid restores to the pre-batch snapshot (42) on a mid-batch failure,
        # matching the success path / docstring / stdlib; _completed_iterations stays
        # at the in-batch count (2) as the separate partial-progress anchor.
        assert cur.lastrowid == pre_batch_lastrowid
        assert cur.completed_iterations == 2
        assert cur.fetchall() == []
        assert cur.description is None
    finally:
        conn.close()


async def test_async_executemany_failure_clears_rowcount(
    cluster_address: str,
) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_em_fail_aio")
        await cur.execute("CREATE TABLE test_em_fail_aio (id INTEGER PRIMARY KEY)")
        # Seed a pre-batch INSERT so the snapshot/restore semantics are observable.
        await cur.execute("INSERT INTO test_em_fail_aio (id) VALUES (?)", (42,))
        pre_batch_lastrowid = cur.lastrowid
        assert pre_batch_lastrowid is not None
        with pytest.raises(IntegrityError):
            await cur.executemany(
                "INSERT INTO test_em_fail_aio (id) VALUES (?)",
                [(1,), (2,), (1,)],
            )
        assert cur.rowcount == -1
        # lastrowid restores to the pre-batch snapshot (42) on a mid-batch failure;
        # _completed_iterations stays at the in-batch count (2).
        assert cur.lastrowid == pre_batch_lastrowid
        assert cur.completed_iterations == 2
        assert cur._rows == []
        assert cur._description is None
    finally:
        await conn.close()
