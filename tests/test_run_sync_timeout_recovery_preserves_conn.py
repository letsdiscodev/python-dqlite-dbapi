"""Pin: ``_run_sync``'s race-recovery branch re-raises the recovered exception WITHOUT
invalidating the connection — it is healthy (``_run_protocol``'s finally cleared ``_in_use``),
so nulling ``_async_conn`` would gratuitously force a reconnect."""

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
    """result() raises TimeoutError first, then the recovered IntegrityError."""

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
    """The recovered-exception path must keep ``self._async_conn`` so the next call reuses it."""
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

    sentinel_async_conn = MagicMock(name="async_conn_sentinel")
    sentinel_async_conn._invalidate = lambda *args, **kw: invalidate_calls.append(args)
    conn._async_conn = sentinel_async_conn

    async def _never_runs() -> None:
        await asyncio.sleep(999)

    with pytest.raises(IntegrityError, match="UNIQUE constraint failed"):
        conn._run_sync(_never_runs())

    assert conn._async_conn is sentinel_async_conn, (
        "race-recovery branch must NOT null self._async_conn — the connection "
        "is healthy (the inner coroutine completed)"
    )
    assert invalidate_calls == [], (
        f"race-recovery branch must NOT schedule _invalidate; got calls={invalidate_calls}"
    )
