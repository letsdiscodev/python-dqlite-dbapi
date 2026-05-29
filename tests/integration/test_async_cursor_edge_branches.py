"""Pin async cursor edge branches: rownumber with description set, fetchmany default size,
fetchmany break on early exhaustion."""

from __future__ import annotations

import pytest

from dqlitedbapi.aio import aconnect


@pytest.mark.integration
class TestAsyncCursorEdgeBranches:
    async def test_rownumber_after_described_cursor_pre_and_post_fetch(
        self, cluster_address: str
    ) -> None:
        """``rownumber`` returns ``self._row_index`` once description is set."""
        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS rownum_t")
            await cur.execute("CREATE TABLE rownum_t (n INTEGER)")
            await cur.execute("INSERT INTO rownum_t (n) VALUES (1), (2), (3)")
            await cur.execute("SELECT n FROM rownum_t ORDER BY n")
            assert cur.rownumber == 0
            await cur.fetchone()
            assert cur.rownumber == 1
            await cur.fetchone()
            assert cur.rownumber == 2
        finally:
            await conn.close()

    async def test_fetchmany_default_size_uses_arraysize(self, cluster_address: str) -> None:
        """``fetchmany`` with no argument falls through to ``size = self._arraysize``."""
        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            cur.arraysize = 2
            await cur.execute("DROP TABLE IF EXISTS fm_default_t")
            await cur.execute("CREATE TABLE fm_default_t (n INTEGER)")
            await cur.execute("INSERT INTO fm_default_t (n) VALUES (1), (2), (3), (4), (5)")
            await cur.execute("SELECT n FROM fm_default_t ORDER BY n")
            rows = await cur.fetchmany()
            assert len(rows) == 2
            assert rows == [(1,), (2,)]
        finally:
            await conn.close()

    async def test_fetchmany_breaks_on_exhausted_cursor(self, cluster_address: str) -> None:
        """``fetchmany(size)`` breaks early when the cursor exhausts before reaching size."""
        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS fm_short_t")
            await cur.execute("CREATE TABLE fm_short_t (n INTEGER)")
            await cur.execute("INSERT INTO fm_short_t (n) VALUES (1), (2)")
            await cur.execute("SELECT n FROM fm_short_t ORDER BY n")
            rows = await cur.fetchmany(100)
            assert len(rows) == 2
            assert rows == [(1,), (2,)]
        finally:
            await conn.close()
