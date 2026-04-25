"""Two ``executemany`` calls racing on the same async connection are
serialized by the connection's ``_op_lock``.

The existing protocol-serialization unit test
(``test_protocol_serialization.py``) pins that two concurrent
``execute()`` calls on cursors sharing one connection are serialized.
What is NOT covered:

- Same shape for ``executemany`` (which has its own op_lock
  acquisition path inside ``_executemany_async``).
- End-to-end against a live cluster (the existing test uses mocks).

Pin both: under contention, all rows from both racers must persist
exactly once and no row may be lost to wire desync from interleaved
op-lock-less batching. A future refactor that drops the lock for
"fast path" executemany would fail this test loudly.
"""

from __future__ import annotations

import asyncio

import pytest

from dqlitedbapi.aio import aconnect


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_executemany_on_same_async_connection_serialized(
    cluster_address: str,
) -> None:
    conn = await aconnect(cluster_address)
    try:
        setup_cur = conn.cursor()
        await setup_cur.execute("DROP TABLE IF EXISTS test_concurrent_emany")
        await setup_cur.execute("CREATE TABLE test_concurrent_emany (id INTEGER PRIMARY KEY)")
        await conn.commit()

        cur_a = conn.cursor()
        cur_b = conn.cursor()

        # Two batches of 50 rows each, racing.
        rows_a = [(i,) for i in range(50)]
        rows_b = [(i,) for i in range(50, 100)]

        await asyncio.gather(
            cur_a.executemany("INSERT INTO test_concurrent_emany (id) VALUES (?)", rows_a),
            cur_b.executemany("INSERT INTO test_concurrent_emany (id) VALUES (?)", rows_b),
        )
        await conn.commit()

        check_cur = conn.cursor()
        await check_cur.execute("SELECT count(*) FROM test_concurrent_emany")
        (count,) = await check_cur.fetchone()  # type: ignore[misc]
        # All 100 rows persisted — neither batch was clobbered nor
        # interleaved at the wire layer.
        assert count == 100

        await check_cur.execute("SELECT id FROM test_concurrent_emany ORDER BY id")
        ids = [r[0] for r in await check_cur.fetchall()]
        assert ids == list(range(100))
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_two_execute_loops_on_same_async_connection_serialized(
    cluster_address: str,
) -> None:
    """Same shape as above but with per-row ``execute`` calls inside
    each task. Pins that the op-lock protects single-row execute
    paths just as it protects executemany batches."""
    conn = await aconnect(cluster_address)
    try:
        setup_cur = conn.cursor()
        await setup_cur.execute("DROP TABLE IF EXISTS test_concurrent_exec")
        await setup_cur.execute("CREATE TABLE test_concurrent_exec (id INTEGER PRIMARY KEY)")
        await conn.commit()

        cur_a = conn.cursor()
        cur_b = conn.cursor()

        async def worker(cur, lo: int, hi: int) -> None:
            for i in range(lo, hi):
                await cur.execute("INSERT INTO test_concurrent_exec (id) VALUES (?)", (i,))

        await asyncio.gather(worker(cur_a, 0, 30), worker(cur_b, 30, 60))
        await conn.commit()

        check_cur = conn.cursor()
        await check_cur.execute("SELECT id FROM test_concurrent_exec ORDER BY id")
        ids = [r[0] for r in await check_cur.fetchall()]
        assert ids == list(range(60))
    finally:
        await conn.close()
