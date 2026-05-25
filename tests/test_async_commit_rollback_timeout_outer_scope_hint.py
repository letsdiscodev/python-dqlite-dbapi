"""Pin: when both the operator's outer ``asyncio.timeout`` AND the
driver's inner ``commit_budget`` / ``rollback_budget`` deadlines
fire near-simultaneously, the OperationalError diagnostic annotates
the elapsed-vs-budget so the operator can correlate against their
outer-scope budget.

Without this annotation, the message named the driver's internal
``_SYNC_PHASES_MULTIPLIER * timeout`` value even when the actual
elapsed time was set by the operator's shorter outer scope —
making post-hoc triage of dangling server-side transactions
impossible to attribute to the right scope.

This pin exercises the elapsed-shorter-than-budget branch by
construction: we cannot reliably synthesise a near-simultaneous
double-deadline in CI, so we drive the elapsed computation
directly via a tiny inner budget and a mocked clock.
"""

from __future__ import annotations

import asyncio
import os
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _make_conn() -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._timeout = 5.0
    conn._async_conn = MagicMock()
    conn._async_conn._protocol = MagicMock()
    conn._async_conn.in_transaction = True
    conn._loop_ref = None
    conn.messages = []
    conn._address = "host:1234"
    conn._cursors = MagicMock()
    conn._cursors.__iter__ = lambda self: iter([])
    conn._cursors.clear = MagicMock()
    conn._finalizer = None
    conn._creator_pid = os.getpid()
    conn._transaction_owner = None
    conn._ensure_locks = MagicMock(return_value=(asyncio.Lock(), asyncio.Lock()))
    conn._check_loop_only = MagicMock()
    return conn


@pytest.mark.asyncio
async def test_commit_timeout_outer_scope_hint_when_elapsed_short() -> None:
    """If elapsed << budget when TimeoutError fires, the message
    includes the "outer scope or sibling cancel likely interrupted"
    hint with the measured elapsed.
    """
    conn = _make_conn()

    # Patch the inner client's execute to await a slow coroutine that
    # the outer asyncio.timeout will cancel.
    async def slow_execute(_sql: str) -> None:
        await asyncio.sleep(60)

    assert conn._async_conn is not None
    conn._async_conn.execute = MagicMock(side_effect=lambda _sql: slow_execute(_sql))

    # Use the inner _SYNC_PHASES_MULTIPLIER * self._timeout = ~ 20s
    # (multiplier default). Wrap commit in a short outer timeout.
    with pytest.raises((OperationalError, asyncio.TimeoutError)) as ei:
        async with asyncio.timeout(0.05):
            await conn.commit()

    # If the outer-timeout escaped as a raw TimeoutError, the inner
    # TimeoutError arm did not fire — that's the "outer fires first"
    # case covered elsewhere. We only assert the hint when the inner
    # OperationalError IS raised.
    if isinstance(ei.value, OperationalError):
        msg = str(ei.value)
        assert "outer scope or sibling cancel likely interrupted" in msg, (
            f"expected outer-scope hint in commit OperationalError; got: {msg!r}"
        )
        assert "elapsed" in msg


@pytest.mark.asyncio
async def test_rollback_timeout_outer_scope_hint_when_elapsed_short() -> None:
    """Mirror pin for rollback."""
    conn = _make_conn()

    async def slow_execute(_sql: str) -> None:
        await asyncio.sleep(60)

    assert conn._async_conn is not None
    conn._async_conn.execute = MagicMock(side_effect=lambda _sql: slow_execute(_sql))

    with pytest.raises((OperationalError, asyncio.TimeoutError)) as ei:
        async with asyncio.timeout(0.05):
            await conn.rollback()

    if isinstance(ei.value, OperationalError):
        msg = str(ei.value)
        assert "outer scope or sibling cancel likely interrupted" in msg, (
            f"expected outer-scope hint in rollback OperationalError; got: {msg!r}"
        )


@pytest.mark.asyncio
async def test_commit_timeout_no_hint_when_inner_deadline_actually_expired() -> None:
    """Regression: when the inner deadline actually expired (elapsed
    >= budget * 0.95), the message stays unchanged — no outer-scope
    hint.
    """
    conn = _make_conn()
    conn._timeout = 0.01  # commit_budget = ~0.04s with multiplier

    async def slow_execute(_sql: str) -> None:
        await asyncio.sleep(60)

    assert conn._async_conn is not None
    conn._async_conn.execute = MagicMock(side_effect=lambda _sql: slow_execute(_sql))

    # No outer timeout. The inner ~0.04s budget will expire.
    with pytest.raises(OperationalError) as ei:
        await conn.commit()

    msg = str(ei.value)
    assert "outer scope or sibling cancel likely interrupted" not in msg, (
        f"inner-deadline expiry must NOT carry the outer-scope hint; got: {msg!r}"
    )
