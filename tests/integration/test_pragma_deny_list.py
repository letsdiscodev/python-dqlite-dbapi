"""Every PRAGMA in the dqlite C VFS deny list surfaces as DatabaseError("not authorized").

Both setter-only and read+write forms are exercised so a C change loosening one direction
surfaces.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi import DatabaseError
from dqlitedbapi.aio import aconnect

_DENIED_PRAGMAS_SYNC: list[str] = [
    # Setter-only denials
    "PRAGMA journal_mode = WAL",
    "PRAGMA page_size = 4096",
    "PRAGMA wal_autocheckpoint = 1000",
    "PRAGMA synchronous = OFF",
    # Both-form denials (read + write)
    "PRAGMA wal_checkpoint",
    "PRAGMA wal_checkpoint(FULL)",
    # busy_timeout is absent: the dbapi layer now intercepts it client-side (see
    # _pragma_intercept.py), so it no longer reaches the server's deny list.
    "PRAGMA read_uncommitted = 1",
    "PRAGMA read_uncommitted",
    "PRAGMA locking_mode = EXCLUSIVE",
    "PRAGMA locking_mode",
]


@pytest.mark.integration
@pytest.mark.parametrize("pragma", _DENIED_PRAGMAS_SYNC)
def test_denied_pragma_raises_database_error_sync(cluster_address: str, pragma: str) -> None:
    with dqlitedbapi.connect(cluster_address, timeout=2.0) as conn:
        cur = conn.cursor()
        with pytest.raises(DatabaseError, match="not authorized"):
            cur.execute(pragma)


@pytest.mark.integration
@pytest.mark.parametrize("pragma", _DENIED_PRAGMAS_SYNC)
async def test_denied_pragma_raises_database_error_async(cluster_address: str, pragma: str) -> None:
    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        with pytest.raises(DatabaseError, match="not authorized"):
            await cur.execute(pragma)
    finally:
        await conn.close()


@pytest.mark.integration
@pytest.mark.parametrize("pragma", ["PRAGMA busy_timeout = 1000", "PRAGMA busy_timeout"])
def test_pragma_busy_timeout_intercepted_sync(cluster_address: str, pragma: str) -> None:
    """PRAGMA busy_timeout is intercepted at the cursor layer; the server never sees it."""
    with dqlitedbapi.connect(cluster_address, timeout=2.0) as conn:
        cur = conn.cursor()
        cur.execute(pragma)
        row = cur.fetchone()
        assert row is not None
        if "=" in pragma:
            assert row[0] == 1000
            assert conn._busy_timeout == 1.0
        else:
            assert row[0] == 5000


@pytest.mark.integration
@pytest.mark.parametrize("pragma", ["PRAGMA busy_timeout = 1000", "PRAGMA busy_timeout"])
async def test_pragma_busy_timeout_intercepted_async(cluster_address: str, pragma: str) -> None:
    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute(pragma)
        row = await cur.fetchone()
        assert row is not None
        if "=" in pragma:
            assert row[0] == 1000
            assert conn._busy_timeout == 1.0
        else:
            assert row[0] == 5000
    finally:
        await conn.close()
