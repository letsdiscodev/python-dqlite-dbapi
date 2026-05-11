"""Pin: every PRAGMA in the dqlite C VFS deny list surfaces as
``DatabaseError`` with the ``"not authorized"`` substring.

Drift fence: a change to the C deny list
(``dqlite-upstream/src/vfs.c:vfsAuthorizer``) or to the
``SQLITE_AUTH`` → ``DatabaseError`` routing at ``cursor.py`` would
surface here, not weeks later in a confused operator's traceback.

The deny list is enumerated from ``vfs.c:vfsAuthorizer`` and split
between setter-only (rejected only with a fourth argument present
in the authorizer callback) and both-form (rejected as either
read or write). Both forms are exercised so a future C change that
loosens one direction surfaces.
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
    "PRAGMA busy_timeout = 1000",
    "PRAGMA busy_timeout",
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
