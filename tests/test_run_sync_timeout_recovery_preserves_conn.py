"""Pin: ``_run_sync``'s race-recovery branch (sync timeout fires at
the same instant the loop-thread coroutine completes with an
exception) re-raises the recovered exception WITHOUT invalidating
the underlying async connection.

The connection is healthy at that moment — ``_run_protocol``'s
``finally`` already cleared ``_in_use`` — so nulling
``self._async_conn`` and scheduling ``_invalidate`` is a gratuitous
side effect that forces the next sync call to reconnect. Under
operating regimes where sync timeouts fire near the inner completion
deadline (slow server + tight sync timeout), the reconnect rate is
amplified by the race rate.

Sibling fix: the KI/SystemExit arm in ``_run_sync`` already had this
discipline; this test pins the timeout arm.
"""

from __future__ import annotations

import asyncio
import concurrent.futures as cf
from typing import Any
from unittest.mock import MagicMock

import pytest

from dqlitedbapi import Connection
from dqlitedbapi import connection as conn_module
from dqlitedbapi.exceptions import IntegrityError


class _RecoveredFuture:
    """``result(timeout=...)`` raises TimeoutError on first call (the
    sync caller's Future.result), then surfaces the recovered
    server-side IntegrityError on the bounded re-fetch."""

    def __init__(self) -> None:
        self._calls = 0
        self._cancelled = False

    def cancel(self) -> bool:
        self._cancelled = True
        return False  # already done

    def done(self) -> bool:
        return True

    def cancelled(self) -> bool:
        return self._cancelled

    def result(self, timeout: float | None = None) -> Any:
        self._calls += 1
        if self._calls == 1:
            raise cf.TimeoutError()
        raise IntegrityError(
            "UNIQUE constraint failed",
            code=2067,
            raw_message="UNIQUE constraint failed",
        )

    def exception(self, timeout: float | None = None) -> BaseException | None:
        return IntegrityError("UNIQUE constraint failed", code=2067)


def test_race_recovery_does_not_null_async_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the recovered exception path fires, ``self._async_conn``
    must remain pointing at the same object (no nulling) so the next
    sync call reuses the healthy connection rather than reconnecting."""
    conn = Connection("localhost:9001", timeout=0.05)

    stub_future = _RecoveredFuture()
    invalidate_calls: list[Any] = []

    def _fake_run_coroutine_threadsafe(coro: Any, loop: Any) -> _RecoveredFuture:
        coro.close()
        return stub_future

    monkeypatch.setattr(
        conn_module.asyncio,  # type: ignore[attr-defined]
        "run_coroutine_threadsafe",
        _fake_run_coroutine_threadsafe,
    )

    # Plant a sentinel on _async_conn so we can observe whether the
    # recovery branch nulled it. A MagicMock is sufficient — the
    # production code only reads attributes on it for the invalidate
    # scheduling.
    sentinel_async_conn = MagicMock(name="async_conn_sentinel")
    sentinel_async_conn._invalidate = lambda *args, **kw: invalidate_calls.append(args)
    conn._async_conn = sentinel_async_conn

    async def _never_runs() -> None:
        await asyncio.sleep(999)

    # The recovered IntegrityError propagates — same as the existing
    # fidelity pin.
    with pytest.raises(IntegrityError, match="UNIQUE constraint failed"):
        conn._run_sync(_never_runs())

    # Connection MUST remain pointing at the same object — the
    # recovered-error branch does NOT null _async_conn.
    assert conn._async_conn is sentinel_async_conn, (
        "race-recovery branch must NOT null self._async_conn — the connection "
        "is healthy (the inner coroutine completed)"
    )
    # _invalidate must NOT have been scheduled.
    assert invalidate_calls == [], (
        f"race-recovery branch must NOT schedule _invalidate; got calls={invalidate_calls}"
    )
