"""A row-returning PRAGMA (e.g. ``PRAGMA table_info``) reports ``rowcount == -1``,
matching stdlib sqlite3 (which returns -1 for all PRAGMA), while its rows are
still fetchable. SELECT keeps reporting ``len(rows)``."""

from __future__ import annotations

import pytest

import dqlitedbapi


@pytest.mark.integration
def test_sync_pragma_read_rowcount_is_minus_one(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS pragma_rc")
        cur.execute("CREATE TABLE pragma_rc (id INTEGER PRIMARY KEY, v INTEGER)")
        conn.commit()

        cur.execute("PRAGMA table_info(pragma_rc)")
        rows = cur.fetchall()
        assert len(rows) == 2  # two columns
        assert cur.rowcount == -1

        # SELECT still reports the buffered count (unchanged).
        cur.execute("SELECT * FROM pragma_rc")
        assert cur.rowcount == len(cur.fetchall())


@pytest.mark.integration
async def test_async_pragma_read_rowcount_is_minus_one(cluster_address: str) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS pragma_rc_async")
        await cur.execute("CREATE TABLE pragma_rc_async (id INTEGER PRIMARY KEY, v INTEGER)")
        await conn.commit()

        await cur.execute("PRAGMA table_info(pragma_rc_async)")
        rows = await cur.fetchall()
        assert len(rows) == 2
        assert cur.rowcount == -1
    finally:
        await conn.close()
