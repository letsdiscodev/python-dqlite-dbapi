"""``executemany`` leaves ``lastrowid`` unchanged (stdlib sqlite3 parity), rather
than clearing it to None, so a prior single INSERT's rowid stays observable."""

from __future__ import annotations

import pytest

import dqlitedbapi


@pytest.mark.integration
def test_sync_executemany_preserves_prior_lastrowid(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS em_lastrowid")
        cur.execute("CREATE TABLE em_lastrowid (id INTEGER PRIMARY KEY, v INTEGER)")
        cur.execute("INSERT INTO em_lastrowid (v) VALUES (?)", (9,))
        prior = cur.lastrowid
        assert prior is not None

        cur.executemany("INSERT INTO em_lastrowid (v) VALUES (?)", [(1,), (2,), (3,)])
        assert cur.lastrowid == prior
        conn.commit()


@pytest.mark.integration
async def test_async_executemany_preserves_prior_lastrowid(cluster_address: str) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS em_lastrowid_async")
        await cur.execute("CREATE TABLE em_lastrowid_async (id INTEGER PRIMARY KEY, v INTEGER)")
        await cur.execute("INSERT INTO em_lastrowid_async (v) VALUES (?)", (9,))
        prior = cur.lastrowid
        assert prior is not None

        await cur.executemany("INSERT INTO em_lastrowid_async (v) VALUES (?)", [(1,), (2,)])
        assert cur.lastrowid == prior
        await conn.commit()
    finally:
        await conn.close()
