"""Cross-layer pin: a deferred-FK code-19 COMMIT failure routes to
IntegrityError and ``conn.in_transaction`` reflects the tracker clear."""

from __future__ import annotations

from typing import Any

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import IntegrityError


async def test_aio_commit_deferred_fk_violation_raises_integrity_clears_inflight() -> None:
    conn = AsyncConnection("localhost:9001")
    fake_inner: Any = type("_FakeInner", (), {})()
    fake_inner.in_transaction = True
    fake_inner._has_untracked_savepoint = False

    async def fake_execute(sql: str) -> object:
        # Mimic the real client path: tracker cleared THEN exception raised.
        fake_inner.in_transaction = False  # tracker cleared before the raise
        raise _client_exc.OperationalError("FOREIGN KEY constraint failed", 19)

    fake_inner.execute = fake_execute
    conn._async_conn = fake_inner

    with pytest.raises(IntegrityError, match="FOREIGN KEY"):
        await conn.commit()
    assert conn.in_transaction is False
