"""Pin the cursor / connection state when async fetch is cancelled.

The async cursor advances ``_row_index`` synchronously after each row
yielded out of ``_rows``; the only awaitable boundary inside fetchone
is the protocol-level continuation read. A cancel mid-fetch can
either:

* fire before any continuation read completes — the cursor has the
  rows it already buffered, ``_row_index`` reflects what was yielded,
  and the protocol is invalidated by ``_run_protocol``'s
  ``except (asyncio.CancelledError, ...)`` handler;
* fire while a continuation read is in flight — the protocol's slot
  is invalidated; the cursor is no longer usable for further fetches.

Either way, the contract is: the **connection** is invalidated; a
sibling cursor on the **same connection** sees ``InterfaceError`` /
``OperationalError`` on the next call (not a hang, not stale rows).

These tests pin that contract end-to-end so a regression that left
the protocol slot in an inconsistent state surfaces immediately.
"""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import InterfaceError, OperationalError

# Recursive CTE that the server iterates row-by-row over a large
# bound. The cursor's ``execute`` must yield row pages back through
# the wire's continuation stream; a tight outer timeout reliably
# lands inside the await on a continuation read.
_BIG_CTE = """
WITH RECURSIVE seq(n) AS (
    SELECT 1
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 5000000
)
SELECT n FROM seq
"""


@pytest.mark.integration
class TestAsyncFetchmanyCancel:
    async def test_cancel_during_execute_of_big_select_invalidates_connection(
        self, cluster_address: str
    ) -> None:
        """Cancel during the awaiting ``execute`` of a multi-frame
        SELECT. The protocol's slot must be invalidated; a follow-up
        operation on the same connection must surface as
        ``InterfaceError`` / ``OperationalError`` rather than hanging
        or returning stale rows."""
        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
                async with asyncio.timeout(0.05):
                    await cur.execute(_BIG_CTE)

            with pytest.raises((InterfaceError, OperationalError)):
                await cur.execute("SELECT 1")
        finally:
            await conn.close()

    async def test_cancel_during_async_for_invalidates_connection(
        self, cluster_address: str
    ) -> None:
        """Cancel inside an ``async for`` over the cursor. The
        iteration is a sequence of ``fetchone`` calls; the cancel
        lands on one of them. The connection must be invalidated."""
        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            collected: list[int] = []
            with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
                async with asyncio.timeout(0.05):
                    await cur.execute(_BIG_CTE)
                    async for row in cur:
                        collected.append(row[0])

            with pytest.raises((InterfaceError, OperationalError)):
                await cur.execute("SELECT 1")
        finally:
            await conn.close()
