"""Cursor-state reset and connection invalidation when an ``executemany``
is cancelled mid-batch, both with and without an enclosing BEGIN."""

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
            # Pre-load a stale description so we can verify the cancel path resets it.
            await cur.execute("SELECT 1")
            assert cur.description is not None

            with pytest.raises((asyncio.CancelledError, asyncio.TimeoutError)):
                async with asyncio.timeout(0.05):
                    await cur.executemany(
                        "INSERT INTO test_em_cancel_no_begin VALUES (?)",
                        [(i,) for i in range(1000)],
                    )

            # Cursor state reset to PEP-249 "undetermined"; _lastrowid is
            # intentionally NOT reset on cancellation (stdlib parity, cleared only by close()).
            assert cur._rowcount == -1
            assert cur._rows == []
            assert cur._description is None
            assert cur._row_index == 0

            # Connection is invalidated; the next op surfaces as InterfaceError
            # or OperationalError depending on how _call_client wraps it.
            with pytest.raises((InterfaceError, OperationalError)):
                await cur.execute("SELECT 1")
        finally:
            await conn.close()

    async def test_cancel_inside_begin_rolls_back_partial_writes(
        self, cluster_address: str
    ) -> None:
        # Setup on a separate connection so the cancellation can't race the CREATE TABLE.
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
            await conn.close()

        verifier = await aconnect(cluster_address, timeout=2.0)
        try:
            vcur = verifier.cursor()
            await vcur.execute("SELECT count(*) FROM test_em_cancel_with_begin")
            row = await vcur.fetchone()
            assert row is not None
            # The uncommitted BEGIN is rolled back when the invalidated session ends.
            assert row[0] == 0
        finally:
            await verifier.close()
