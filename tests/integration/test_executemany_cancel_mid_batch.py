"""Pin the cursor-state and connection invalidation invariants when an
``executemany`` is cancelled mid-batch.

``AsyncCursor.executemany`` resets ``_rowcount``, ``_rows``,
``_description``, ``_lastrowid``, ``_row_index`` on a ``BaseException``
(which catches ``CancelledError``). The underlying connection is
invalidated by ``DqliteConnection._run_protocol``'s
``except (asyncio.CancelledError, KeyboardInterrupt, SystemExit)``
handler. None of this was previously pinned by an integration test.

A regression that, say, moves the ``_rows = []`` reset out of the
``except BaseException`` block, or that fails to invalidate the
underlying connection on cancellation, would silently break the
contract.

The two test cases:

1. *no enclosing BEGIN*: assert cursor state reset and connection
   invalidation.
2. *wrapped in BEGIN*: on a fresh connection, assert the table is
   empty — the pool's ``_reset_connection`` ROLLBACK on invalidate is
   the production safety net.
"""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import InterfaceError, OperationalError


@pytest.mark.integration
class TestExecutemanyCancelMidBatch:
    async def test_cancel_resets_cursor_state_and_invalidates_connection(
        self, cluster_address: str
    ) -> None:
        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            await cur.execute("DROP TABLE IF EXISTS test_em_cancel_no_begin")
            await cur.execute("CREATE TABLE test_em_cancel_no_begin (n INTEGER)")
            # Pre-load a stale description so we can verify it is reset
            # by the executemany cancel path.
            await cur.execute("SELECT 1")
            assert cur.description is not None

            with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
                async with asyncio.timeout(0.05):
                    await cur.executemany(
                        "INSERT INTO test_em_cancel_no_begin VALUES (?)",
                        [(i,) for i in range(1000)],
                    )

            # Cursor state has been reset to PEP-249 "undetermined".
            assert cur._rowcount == -1
            assert cur._rows == []
            assert cur._description is None
            assert cur._lastrowid is None
            assert cur._row_index == 0

            # Underlying DqliteConnection is invalidated. The next
            # operation surfaces as InterfaceError (cursor / connection
            # closed semantics) or OperationalError, depending on
            # whether the dbapi adapter's ``_call_client`` wraps the
            # invalidation as the closed shape — accept either.
            with pytest.raises((InterfaceError, OperationalError)):
                await cur.execute("SELECT 1")
        finally:
            await conn.close()

    async def test_cancel_inside_begin_rolls_back_partial_writes(
        self, cluster_address: str
    ) -> None:
        # Setup table on a separate connection so the INSERT batch
        # cancellation does not race with the CREATE TABLE.
        setup = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = setup.cursor()
            await cur.execute("DROP TABLE IF EXISTS test_em_cancel_with_begin")
            await cur.execute("CREATE TABLE test_em_cancel_with_begin (n INTEGER)")
        finally:
            await setup.close()

        conn = await aconnect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            await cur.execute("BEGIN")
            with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
                async with asyncio.timeout(0.05):
                    await cur.executemany(
                        "INSERT INTO test_em_cancel_with_begin VALUES (?)",
                        [(i,) for i in range(1000)],
                    )
        finally:
            # close() on an invalidated connection is the canonical
            # cleanup path; the underlying transport is already torn.
            await conn.close()

        # Fresh connection, verify rollback happened.
        verifier = await aconnect(cluster_address, timeout=2.0)
        try:
            vcur = verifier.cursor()
            await vcur.execute("SELECT count(*) FROM test_em_cancel_with_begin")
            row = await vcur.fetchone()
            assert row is not None
            # The mid-batch cancel invalidates the connection; the
            # uncommitted BEGIN is rolled back when the underlying
            # SQLite session ends. No rows should be persisted.
            assert row[0] == 0
        finally:
            await verifier.close()
