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
        # ``lastrowid`` is restored to the pre-batch snapshot —
        # stdlib ``sqlite3.Cursor.lastrowid`` is documented as not
        # being cleared by failed/cancelled operations, and the
        # snapshot/restore arm overwrites the intra-batch write
        # (which would otherwise leak whichever row the loop touched
        # last) with the value the caller observed before the batch.
        assert cur.lastrowid == pre_batch_lastrowid
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
        # lastrowid restored to pre-batch snapshot (stdlib parity).
        assert cur.lastrowid == pre_batch_lastrowid
        assert cur._rows == []
        assert cur._description is None
    finally:
        await conn.close()
