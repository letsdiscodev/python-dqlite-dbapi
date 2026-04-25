"""Pin async cursor edge branches reported as uncovered by
``pytest --cov``.

Lines covered (pre-pragma):

- aio/cursor.py:127 — ``rownumber`` returns ``self._row_index`` when
  ``description`` is set (i.e. an active SELECT result set).
- aio/cursor.py:415 — ``fetchmany``'s default-size branch:
  ``size = self._arraysize`` when no explicit size is passed.
- aio/cursor.py:425 — ``fetchmany``'s inner ``break`` when
  ``fetchone()`` returns ``None`` (cursor exhausted before reaching
  the requested size).

These are PEP 249 ``Cursor`` extension-method contracts that the
existing async-cursor tests cover only transitively. Pin each
explicitly.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.aio import aconnect


@pytest.mark.integration
class TestAsyncCursorEdgeBranches:
    async def test_rownumber_after_described_cursor_pre_and_post_fetch(
        self, cluster_address: str
    ) -> None:
        """``rownumber`` returns ``self._row_index`` once description
        is set (i.e. after a SELECT has populated the result set).
        Drives aio/cursor.py:127."""
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
        """``fetchmany`` with no argument falls through to
        ``size = self._arraysize``. Drives aio/cursor.py:415."""
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
        """``fetchmany(size)`` breaks the inner loop early when
        ``fetchone()`` returns ``None`` (cursor exhausted with fewer
        rows than requested). Drives aio/cursor.py:425."""
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
