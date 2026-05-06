"""Pin the cross-layer contract for deferred-FK COMMIT failures.

The client layer's _run_protocol auto-rollback branch (and the
deferred-FK clear in execute()) clears the tracker on a code-19
COMMIT/RELEASE failure under PRAGMA defer_foreign_keys=ON. This
test pins that the dbapi-layer commit() routes the failure to
IntegrityError and that ``conn.in_transaction`` reflects the
tracker clear immediately.
"""

from __future__ import annotations

from typing import Any

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import IntegrityError


@pytest.mark.asyncio
async def test_aio_commit_deferred_fk_violation_raises_integrity_clears_inflight() -> None:
    conn = AsyncConnection("localhost:9001")
    fake_inner: Any = type("_FakeInner", (), {})()
    fake_inner.in_transaction = True
    fake_inner._has_untracked_savepoint = False

    async def fake_execute(sql: str) -> object:
        # Mimic the real client path: tracker cleared THEN exception raised.
        fake_inner.in_transaction = False
        raise _client_exc.OperationalError("FOREIGN KEY constraint failed", 19)

    fake_inner.execute = fake_execute
    conn._async_conn = fake_inner

    with pytest.raises(IntegrityError, match="FOREIGN KEY"):
        await conn.commit()
    # Cross-layer pin: dbapi.in_transaction reflects client tracker.
    assert conn.in_transaction is False
