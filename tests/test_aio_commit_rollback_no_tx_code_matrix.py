"""Sync/async parity for the no-transaction swallow at the dbapi
``commit()`` / ``rollback()`` boundary.

The sync helper test (``test_is_no_transaction_error.py``) covers
extended SQLITE_ERROR codes (769, 513) plus the canonical primary
1. The async wrapper tests covered code=1 only. Pin sync/async
parity over the full code matrix so a future refactor that moves
the swallow into one branch only does not silently drop the other.
"""

from __future__ import annotations

from typing import Any

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import DatabaseError


def _build_fake_inner(code: int, message: str) -> Any:
    async def fake_execute(sql: str) -> object:
        raise _client_exc.OperationalError(code, message)

    fake = type("_FakeInner", (), {})()
    fake.execute = fake_execute
    fake.in_transaction = True
    fake._has_untracked_savepoint = False
    return fake


@pytest.mark.parametrize(
    ("code", "should_swallow"),
    [
        (1, True),  # SQLITE_ERROR primary
        (769, True),  # extended SQLITE_ERROR variant
        (513, True),  # extended SQLITE_ERROR variant
        (10, False),  # SQLITE_IOERR — different primary, propagates
        (19, False),  # SQLITE_CONSTRAINT — propagates
        (21, False),  # SQLITE_MISUSE — propagates
    ],
)
@pytest.mark.asyncio
async def test_aio_commit_swallow_matrix(code: int, should_swallow: bool) -> None:
    conn = AsyncConnection("localhost:9001")
    conn._async_conn = _build_fake_inner(code, "cannot commit - no transaction is active")

    if should_swallow:
        await conn.commit()
    else:
        with pytest.raises(DatabaseError):
            await conn.commit()


@pytest.mark.parametrize(
    ("code", "should_swallow"),
    [
        (1, True),
        (769, True),
        (513, True),
        (10, False),
        (19, False),
        (21, False),
    ],
)
@pytest.mark.asyncio
async def test_aio_rollback_swallow_matrix(code: int, should_swallow: bool) -> None:
    conn = AsyncConnection("localhost:9001")
    conn._async_conn = _build_fake_inner(code, "cannot rollback - no transaction is active")

    if should_swallow:
        await conn.rollback()
    else:
        with pytest.raises(DatabaseError):
            await conn.rollback()
