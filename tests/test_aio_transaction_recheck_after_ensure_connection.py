"""Pin: ``AsyncConnection.transaction()``'s ctxmgr re-checks
``_closed`` / ``_async_conn`` AFTER the ``_ensure_connection`` await
returns and BEFORE reserving the ``_transaction_owner`` slot.

Without this re-check, a foreign-thread ``force_close_transport``
landing between the fast-path return and the slot reservation
pinned the slot to a now-dying task, surfacing as a misleading
"Nested conn.transaction() not supported" on sibling calls instead
of the truthful "Connection is closed".

Mirrors the in-lock recheck discipline at ``commit()`` / ``rollback()``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


@pytest.mark.asyncio
async def test_transaction_raises_when_close_races_ensure_connection() -> None:
    """Simulate a concurrent close that ran while
    ``_ensure_connection`` was suspended: returns the inner handle
    but ``_closed`` / ``_async_conn`` reflect the closed state. The
    transaction ctxmgr must raise InterfaceError with the truthful
    "closed during setup" diagnostic, and must NOT pin the
    ``_transaction_owner`` slot.
    """
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._async_conn = MagicMock()
    conn._async_conn.transaction = MagicMock()
    conn._transaction_owner = None
    conn._loop_ref = None
    conn.messages = []
    conn._address = "host:1234"

    # Bypass loop-binding check.
    conn._check_loop_binding = MagicMock()

    inner = MagicMock()

    async def fake_ensure_connection() -> object:
        # Simulate a foreign-thread close racing in DURING the await.
        conn._closed = True
        conn._async_conn = None
        return inner

    conn._ensure_connection = fake_ensure_connection  # type: ignore[assignment]

    with pytest.raises(InterfaceError, match="closed during transaction setup"):
        async with conn.transaction():
            pytest.fail("body must never run")

    # Slot must not be pinned: the recheck fired before reservation.
    assert conn._transaction_owner is None


@pytest.mark.asyncio
async def test_transaction_happy_path_unchanged() -> None:
    """Regression: no concurrent close → ctxmgr enters the body."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._transaction_owner = None
    conn._loop_ref = None
    conn.messages = []
    conn._address = "host:1234"

    inner_tx = MagicMock()
    inner_tx.__aenter__ = AsyncMock(return_value=None)
    inner_tx.__aexit__ = AsyncMock(return_value=None)

    inner = MagicMock()
    inner.transaction = MagicMock(return_value=inner_tx)
    conn._async_conn = inner

    async def fake_ensure_connection() -> object:
        return inner

    conn._ensure_connection = fake_ensure_connection  # type: ignore[assignment]
    conn._check_loop_binding = MagicMock()

    body_ran = False
    async with conn.transaction():
        body_ran = True
        # Owner slot reserved during body.
        assert conn._transaction_owner is not None
    assert body_ran
    # Slot cleared on exit.
    assert conn._transaction_owner is None
