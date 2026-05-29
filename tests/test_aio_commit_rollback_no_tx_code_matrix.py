"""Async no-transaction swallow over the full SQLITE_ERROR code matrix (1, 769, 513), matching
the sync helper, so a refactor can't silently drop the swallow on one branch."""

from __future__ import annotations

from typing import Any

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import Error


def _build_fake_inner(code: int, message: str) -> Any:
    async def fake_execute(sql: str) -> object:
        raise _client_exc.OperationalError(message, code)

    fake = type("_FakeInner", (), {})()
    fake.execute = fake_execute
    fake.in_transaction = True
    fake._has_untracked_savepoint = False
    return fake


@pytest.mark.parametrize(
    ("code", "should_swallow"),
    [
        # code=0 is upstream's failure(req, 0, "empty statement"); must surface, not be swallowed.
        (0, False),
        (1, True),  # SQLITE_ERROR primary
        (769, True),  # extended SQLITE_ERROR variant
        (513, True),  # extended SQLITE_ERROR variant
        (10, False),  # SQLITE_IOERR — different primary, propagates
        (19, False),  # SQLITE_CONSTRAINT — propagates
        (21, False),  # SQLITE_MISUSE — propagates
    ],
)
async def test_aio_commit_swallow_matrix(code: int, should_swallow: bool) -> None:
    message = "empty statement" if code == 0 else "cannot commit - no transaction is active"
    conn = AsyncConnection("localhost:9001")
    conn._async_conn = _build_fake_inner(code, message)

    if should_swallow:
        await conn.commit()
    else:
        # Broad catch: this matrix spans subclasses; we pin only that the no-tx swallow
        # does NOT engage (per-code subclass mapping is pinned by classifier unit tests).
        with pytest.raises(Error):
            await conn.commit()


@pytest.mark.parametrize(
    ("code", "should_swallow"),
    [
        (0, False),  # See commit-side rationale.
        (1, True),
        (769, True),
        (513, True),
        (10, False),
        (19, False),
        (21, False),
    ],
)
async def test_aio_rollback_swallow_matrix(code: int, should_swallow: bool) -> None:
    message = "empty statement" if code == 0 else "cannot rollback - no transaction is active"
    conn = AsyncConnection("localhost:9001")
    conn._async_conn = _build_fake_inner(code, message)

    if should_swallow:
        await conn.rollback()
    else:
        # See commit-matrix: broad catch since the matrix spans subclasses.
        with pytest.raises(Error):
            await conn.rollback()
