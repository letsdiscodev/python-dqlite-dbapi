"""Regression fence: ``fetchone`` after a ``fetchall``-then-re-execute
must return the first row of the new result set.

``Cursor.execute`` resets ``_row_index`` to 0 for every new result set.
The terminal state left by ``fetchall`` (``_row_index == len(_rows)``) is
numerically distinct from the mid-consumption state already pinned by
``test_cursor_iterator_reset.py`` and the DML re-execute path covered
by ``test_cursor_row_index_reset.py``: this module specifically fences
"buffer exhausted by fetchall, then a new query, then the first row of
that new query". A lazy-reset optimisation that only rewinds when the
new result is shorter than the old one would silently re-expose the
old rows past the new boundary and go undetected without this fence.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio import aconnect


@pytest.mark.integration
class TestAsyncFetchoneAfterFetchallReexecute:
    async def test_fetchone_returns_first_row_of_new_query(self, cluster_address: str) -> None:
        async with await aconnect(cluster_address, database="test_ridx_aio") as conn:
            c = conn.cursor()
            await c.execute("DROP TABLE IF EXISTS ridx")
            await c.execute("CREATE TABLE ridx (id INTEGER)")
            await c.executemany(
                "INSERT INTO ridx (id) VALUES (?)",
                [(1,), (2,), (3,)],
            )
            await conn.commit()

            # First query: fetch every row so _row_index lands at the
            # terminal position (== len(_rows)).
            await c.execute("SELECT id FROM ridx ORDER BY id")
            first_all = await c.fetchall()
            assert [row[0] for row in first_all] == [1, 2, 3]

            # Re-execute a completely different query. fetchone must
            # return the first row of the new set, not None (which
            # would mean the cursor is still at the terminal position
            # of the previous result).
            await c.execute("SELECT id FROM ridx WHERE id > 1 ORDER BY id")
            row = await c.fetchone()
            assert row is not None
            assert row[0] == 2

            tail = await c.fetchall()
            assert [r[0] for r in tail] == [3]

            await c.execute("DROP TABLE ridx")

    async def test_asymmetric_sizes_no_stale_buffer_bleed(self, cluster_address: str) -> None:
        """Q1 returns 5 rows, Q2 returns 2. After fetchall → execute →
        fetchall, the second list is exactly Q2's two rows — not three
        stale rows from Q1 concatenated or overwritten in place.
        """
        async with await aconnect(cluster_address, database="test_ridx_asym_aio") as conn:
            c = conn.cursor()
            await c.execute("DROP TABLE IF EXISTS ridx")
            await c.execute("CREATE TABLE ridx (id INTEGER)")
            await c.executemany(
                "INSERT INTO ridx (id) VALUES (?)",
                [(10,), (20,), (30,), (40,), (50,)],
            )
            await conn.commit()

            await c.execute("SELECT id FROM ridx ORDER BY id")
            first = await c.fetchall()
            assert [r[0] for r in first] == [10, 20, 30, 40, 50]

            await c.execute("SELECT id FROM ridx WHERE id < 30 ORDER BY id")
            second = await c.fetchall()
            assert [r[0] for r in second] == [10, 20]

            await c.execute("DROP TABLE ridx")


@pytest.mark.integration
class TestSyncFetchoneAfterFetchallReexecute:
    def test_fetchone_returns_first_row_of_new_query(self, cluster_address: str) -> None:
        conn = connect(cluster_address, database="test_ridx_sync")
        try:
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS ridx_sync")
            c.execute("CREATE TABLE ridx_sync (id INTEGER)")
            c.executemany(
                "INSERT INTO ridx_sync (id) VALUES (?)",
                [(1,), (2,), (3,)],
            )
            conn.commit()

            c.execute("SELECT id FROM ridx_sync ORDER BY id")
            first_all = c.fetchall()
            assert [row[0] for row in first_all] == [1, 2, 3]

            c.execute("SELECT id FROM ridx_sync WHERE id > 1 ORDER BY id")
            row = c.fetchone()
            assert row is not None
            assert row[0] == 2

            tail = c.fetchall()
            assert [r[0] for r in tail] == [3]

            c.execute("DROP TABLE ridx_sync")
        finally:
            conn.close()
