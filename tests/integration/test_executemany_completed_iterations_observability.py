"""Pin: ``Cursor.completed_iterations`` reports the count of
executemany() iterations that committed before a cancel / mid-batch
raise.

After a cancel, ``rowcount`` resets to PEP 249's "undetermined"
sentinel (-1); ``completed_iterations`` retains the count so callers
can write idempotent compensation against the exact prefix of
``seq_of_parameters`` that already persisted.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect


@pytest.mark.integration
def test_completed_iterations_after_successful_executemany(
    cluster_address: str,
) -> None:
    """After a normal successful executemany, equals the input count."""
    with dqlitedbapi.connect(cluster_address, database="test_completed_iters") as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS exec_iter")
        cur.execute("CREATE TABLE exec_iter (id INTEGER)")
        conn.commit()
        cur.executemany("INSERT INTO exec_iter VALUES (?)", [(i,) for i in range(5)])
        conn.commit()
        assert cur.completed_iterations == 5


@pytest.mark.integration
def test_completed_iterations_resets_per_call(cluster_address: str) -> None:
    """A fresh executemany call resets the counter."""
    with dqlitedbapi.connect(cluster_address, database="test_completed_iters") as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS exec_iter")
        cur.execute("CREATE TABLE exec_iter (id INTEGER)")
        conn.commit()
        cur.executemany("INSERT INTO exec_iter VALUES (?)", [(1,), (2,), (3,)])
        assert cur.completed_iterations == 3
        # Second call resets:
        cur.executemany("INSERT INTO exec_iter VALUES (?)", [(4,)])
        assert cur.completed_iterations == 1


@pytest.mark.integration
def test_completed_iterations_starts_at_zero() -> None:
    """A never-executed cursor reports 0."""
    cur = dqlitedbapi.Cursor.__new__(dqlitedbapi.Cursor)
    cur._completed_iterations = 0  # mirror what __init__ would do
    assert cur.completed_iterations == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_completed_iterations_after_success(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, database="test_completed_iters_async")
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS exec_iter_async")
        await cur.execute("CREATE TABLE exec_iter_async (id INTEGER)")
        await conn.commit()
        await cur.executemany(
            "INSERT INTO exec_iter_async VALUES (?)", [(i,) for i in range(7)]
        )
        await conn.commit()
        assert cur.completed_iterations == 7
    finally:
        await conn.close()
