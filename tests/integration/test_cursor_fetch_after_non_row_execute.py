"""Fetch on a cursor whose last ``execute`` was a non-row-returning
statement raises ``ProgrammingError("no results to fetch")``.

The cursor's ``description`` is updated by every ``execute`` call;
non-row statements (BEGIN, COMMIT, ROLLBACK) clear it. A subsequent
``fetchone`` then reaches ``_check_result_set`` and raises
``ProgrammingError`` with the "no results to fetch" wording.

This contract is correct today and unpinned. A future refactor that
preserves the prior call's description across non-row statements
would silently change the error shape (or worse, return stale rows
from the prior SELECT). Pin one assertion per non-row verb so the
regression is loud.

The COMMIT / ROLLBACK / BEGIN cases are the load-bearing ones —
SAVEPOINT and friends are covered by the in_transaction-flag tests
elsewhere.
"""

from __future__ import annotations

import pytest

import dqlitedbapi


@pytest.mark.integration
def test_fetchone_after_rollback_raises_no_results_to_fetch(
    cluster_address: str,
) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_fetch_after_rollback")
        cur.execute("CREATE TABLE test_fetch_after_rollback (id INTEGER)")
        cur.execute("INSERT INTO test_fetch_after_rollback (id) VALUES (1), (2)")
        conn.commit()

        cur.execute("BEGIN")
        cur.execute("SELECT id FROM test_fetch_after_rollback")
        # ROLLBACK on the same cursor: description cleared; next
        # fetchone must raise ProgrammingError, not return stale rows.
        cur.execute("ROLLBACK")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="no results"):
            cur.fetchone()


@pytest.mark.integration
def test_fetchone_after_commit_raises_no_results_to_fetch(
    cluster_address: str,
) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_fetch_after_commit")
        cur.execute("CREATE TABLE test_fetch_after_commit (id INTEGER)")
        cur.execute("INSERT INTO test_fetch_after_commit (id) VALUES (1), (2)")
        conn.commit()

        cur.execute("BEGIN")
        cur.execute("SELECT id FROM test_fetch_after_commit")
        cur.execute("COMMIT")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="no results"):
            cur.fetchone()


@pytest.mark.integration
def test_fetchone_after_begin_raises_no_results_to_fetch(
    cluster_address: str,
) -> None:
    """A BEGIN on the same cursor after a SELECT also clears the
    description — the inverse direction of the COMMIT/ROLLBACK
    cases above."""
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS test_fetch_after_begin")
        cur.execute("CREATE TABLE test_fetch_after_begin (id INTEGER)")
        cur.execute("INSERT INTO test_fetch_after_begin (id) VALUES (1)")
        conn.commit()

        cur.execute("SELECT id FROM test_fetch_after_begin")
        cur.execute("BEGIN")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="no results"):
            cur.fetchone()
        conn.rollback()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_fetchone_after_rollback_raises_no_results(
    cluster_address: str,
) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS test_async_fetch_rb")
        await cur.execute("CREATE TABLE test_async_fetch_rb (id INTEGER)")
        await cur.execute("INSERT INTO test_async_fetch_rb (id) VALUES (1)")
        await conn.commit()

        await cur.execute("BEGIN")
        await cur.execute("SELECT id FROM test_async_fetch_rb")
        await cur.execute("ROLLBACK")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="no results"):
            await cur.fetchone()
    finally:
        await conn.close()
