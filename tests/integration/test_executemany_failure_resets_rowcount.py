"""When executemany fails mid-batch, cursor.rowcount must report -1
("undetermined") rather than leaking the last successful iteration's
value. Misleading rowcount confuses recovery code that tries to
"save what we got so far."
"""

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
        # Seed a pre-batch INSERT so the snapshot/restore semantics
        # of executemany's BaseException arm are observable. The
        # rowid the caller observes here is the value the arm must
        # restore after the mid-batch failure.
        cur.execute("INSERT INTO test_em_fail (id) VALUES (?)", (42,))
        pre_batch_lastrowid = cur.lastrowid
        assert pre_batch_lastrowid is not None
        with pytest.raises(IntegrityError):
            # The third row collides with the first; the loop fails
            # mid-batch.
            cur.executemany(
                "INSERT INTO test_em_fail (id) VALUES (?)",
                [(1,), (2,), (1,)],
            )
        # PEP 249 permits -1 as "undetermined". Pin the conservative
        # behaviour so callers don't observe a misleading
        # last-iteration rowcount.
        assert cur.rowcount == -1
        # ``lastrowid`` is PRESERVED at the last successful in-batch
        # iteration's rowid (2 from row [(2,)]), so the
        # (``_completed_iterations``, ``lastrowid``) pair is internally
        # consistent for idempotent compensation. The pre-batch
        # snapshot (42) is restored only when no in-batch iteration
        # committed; mid-batch failures keep the in-batch anchor.
        assert cur.lastrowid == 2
        assert cur._completed_iterations == 2
        assert cur._rows == []
        assert cur._description is None
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
        # Seed a pre-batch INSERT so the snapshot/restore semantics
        # of executemany's BaseException arm are observable.
        await cur.execute("INSERT INTO test_em_fail_aio (id) VALUES (?)", (42,))
        pre_batch_lastrowid = cur.lastrowid
        assert pre_batch_lastrowid is not None
        with pytest.raises(IntegrityError):
            await cur.executemany(
                "INSERT INTO test_em_fail_aio (id) VALUES (?)",
                [(1,), (2,), (1,)],
            )
        assert cur.rowcount == -1
        # lastrowid preserved at the last successful in-batch row (2).
        assert cur.lastrowid == 2
        assert cur._completed_iterations == 2
        assert cur._rows == []
        assert cur._description is None
    finally:
        await conn.close()
