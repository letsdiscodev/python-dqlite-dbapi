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
        # ``_lastrowid`` is intentionally preserved across a failed
        # executemany — stdlib ``sqlite3.Cursor.lastrowid`` is
        # documented as not being cleared by failed/cancelled
        # operations. The successful iteration before the failure DID
        # insert id=2; that rowid survives the mid-batch IntegrityError.
        assert cur.lastrowid == 2
        assert cur._rows == []
        assert cur._description is None
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_executemany_failure_clears_rowcount(
    cluster_address: str,
) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_em_fail_aio")
        await cur.execute("CREATE TABLE test_em_fail_aio (id INTEGER PRIMARY KEY)")
        with pytest.raises(IntegrityError):
            await cur.executemany(
                "INSERT INTO test_em_fail_aio (id) VALUES (?)",
                [(1,), (2,), (1,)],
            )
        assert cur.rowcount == -1
        # _lastrowid preserved across failed executemany (stdlib parity).
        assert cur.lastrowid == 2
        assert cur._rows == []
        assert cur._description is None
    finally:
        await conn.close()
