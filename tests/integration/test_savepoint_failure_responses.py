"""SAVEPOINT-related FailureResponse handling at the dbapi layer.

The dbapi's ``_NO_TX_SUBSTRINGS`` (``"no transaction is active"``,
``"cannot rollback"``) gates the silent ``commit()`` / ``rollback()``
swallow. SQLite's "no such savepoint" error wording does NOT contain
either substring, so RELEASE / ROLLBACK TO of an unknown savepoint
must propagate as ``OperationalError`` rather than being silently
swallowed.

These tests pin the contract end-to-end. They complement the unit
tests in ``test_is_no_transaction_error.py`` (which exercise the
classifier in isolation) by confirming the wire/client/dbapi stack
preserves the propagation through every layer.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import connect
from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import OperationalError


def test_release_unknown_savepoint_raises_operational_error(cluster_address: str) -> None:
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        with pytest.raises(OperationalError, match="no such savepoint"):
            cur.execute("RELEASE SAVEPOINT does_not_exist")
        # Connection is still usable after the rejection.
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
    finally:
        conn.close()


def test_rollback_to_unknown_savepoint_raises_operational_error(cluster_address: str) -> None:
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        with pytest.raises(OperationalError, match="no such savepoint"):
            cur.execute("ROLLBACK TO SAVEPOINT does_not_exist")
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)
    finally:
        conn.close()


def test_release_unknown_inside_active_tx_raises_and_keeps_tx(cluster_address: str) -> None:
    """RELEASE of an unknown savepoint inside an explicit BEGIN
    surfaces the failure but does NOT roll back the outer
    transaction. Subsequent statements in the same transaction
    succeed; commit() persists their effect."""
    conn = connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS sp_failure_tx")
        cur.execute("CREATE TABLE sp_failure_tx (n INTEGER PRIMARY KEY)")
        cur.execute("BEGIN")
        cur.execute("INSERT INTO sp_failure_tx (n) VALUES (1)")
        with pytest.raises(OperationalError, match="no such savepoint"):
            cur.execute("RELEASE SAVEPOINT does_not_exist")
        # Outer tx still alive — insert another row, commit, observe both.
        cur.execute("INSERT INTO sp_failure_tx (n) VALUES (2)")
        cur.execute("COMMIT")
        cur.execute("SELECT n FROM sp_failure_tx ORDER BY n")
        assert cur.fetchall() == [(1,), (2,)]
    finally:
        cur.execute("DROP TABLE IF EXISTS sp_failure_tx")
        conn.close()


@pytest.mark.asyncio
async def test_async_release_unknown_savepoint_raises(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        with pytest.raises(OperationalError, match="no such savepoint"):
            await cur.execute("RELEASE SAVEPOINT does_not_exist")
        await cur.execute("SELECT 1")
        assert await cur.fetchone() == (1,)
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_async_rollback_to_unknown_savepoint_raises(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        with pytest.raises(OperationalError, match="no such savepoint"):
            await cur.execute("ROLLBACK TO SAVEPOINT does_not_exist")
        await cur.execute("SELECT 1")
        assert await cur.fetchone() == (1,)
    finally:
        await conn.close()
