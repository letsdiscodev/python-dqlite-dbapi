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
    # ``PRAGMA busy_timeout`` was historically in this deny list
    # (C VFS rejects it server-side via SQLITE_AUTH), but the
    # dbapi-layer now intercepts the PRAGMA at the cursor's
    # execute path BEFORE the wire send — the interception updates
    # the connection's busy_timeout (stdlib parity) without ever
    # reaching the server. See
    # ``test_pragma_busy_timeout_intercept.py`` for the
    # interception pin and ``_pragma_intercept.py`` for the
    # implementation. The busy_timeout entries are removed from
    # this deny-list-pin because the user-observable behaviour is
    # no longer ``DatabaseError("not authorized")`` — it's a
    # successful PRAGMA that returns the current value as a row.
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


# Pre-existing pin was "PRAGMA busy_timeout raises DatabaseError(not
# authorized)" — that pin pre-dated the dbapi-layer interception.
# The busy_timeout entries are now intercepted client-side: the
# setter updates the connection's busy_timeout and emits the value
# as a row; the getter emits the current value. These tests pin the
# new user-observable behaviour against the live cluster (the
# unit-test ``test_pragma_busy_timeout_intercept.py`` pins it
# against a fake cursor).


@pytest.mark.integration
@pytest.mark.parametrize("pragma", ["PRAGMA busy_timeout = 1000", "PRAGMA busy_timeout"])
def test_pragma_busy_timeout_intercepted_sync(cluster_address: str, pragma: str) -> None:
    """``PRAGMA busy_timeout`` is intercepted at the cursor layer
    BEFORE the wire send — the server never sees it. The setter
    updates the connection's busy_timeout; both forms emit the
    (possibly updated) value as a row."""
    with dqlitedbapi.connect(cluster_address, timeout=2.0) as conn:
        cur = conn.cursor()
        cur.execute(pragma)
        row = cur.fetchone()
        assert row is not None
        # Setter sets to 1000ms (1.0s); getter returns whatever the
        # connection's default is (5000ms from the kwarg default).
        if "=" in pragma:
            assert row[0] == 1000
            # Verify the connection state was actually updated.
            assert conn._busy_timeout == 1.0
        else:
            # Getter — returns the current value (default 5000ms).
            assert row[0] == 5000


@pytest.mark.integration
@pytest.mark.parametrize("pragma", ["PRAGMA busy_timeout = 1000", "PRAGMA busy_timeout"])
async def test_pragma_busy_timeout_intercepted_async(cluster_address: str, pragma: str) -> None:
    """Async sibling — same interception behaviour."""
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
