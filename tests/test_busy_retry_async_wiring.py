"""Pin: ``retry_async_on_busy`` implements the SQLite-curve BUSY
retry on the async surface.

These tests drive the module-level helper directly with a controlled
coroutine factory; the integration with ``AsyncCursor.execute`` /
``AsyncCursor.executemany`` / ``AsyncConnection.commit`` is pinned at
the higher-level test files.

Behaviours pinned:

- BUSY retry succeeds after sleep, advancing the SQLite curve.
- ``busy_timeout=0`` disables retry.
- Non-BUSY ``OperationalError`` (different code) propagates
  immediately.
- ``CancelledError`` during ``asyncio.sleep`` propagates (not caught
  by the BUSY arm).
- Budget exhaustion raises the most recent BUSY exception.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from dqlitedbapi._busy_retry import retry_async_on_busy
from dqlitedbapi.exceptions import OperationalError
from dqlitewire import SQLITE_BUSY


@pytest.mark.asyncio
async def test_retry_async_succeeds_after_busy_then_ok() -> None:
    """First await raises BUSY, second await returns."""
    call_count = [0]

    async def factory() -> str:
        call_count[0] += 1
        if call_count[0] == 1:
            raise OperationalError("database is locked", code=SQLITE_BUSY)
        return "ok"

    with patch("asyncio.sleep") as sleep_mock:
        sleep_mock.return_value = None

        # asyncio.sleep is async; configure the mock to return an
        # awaitable.
        async def fake_sleep(*args: Any, **kwargs: Any) -> None:
            return None

        with patch("dqlitedbapi._busy_retry.asyncio.sleep", side_effect=fake_sleep):
            result = await retry_async_on_busy(5.0, factory)
    assert result == "ok"
    assert call_count[0] == 2


@pytest.mark.asyncio
async def test_retry_async_sleeps_sqlite_curve() -> None:
    """Sleep durations follow the SQLite curve (1, 2, 5, 10, 15 ms ...)."""
    call_count = [0]

    async def factory() -> str:
        call_count[0] += 1
        if call_count[0] <= 5:
            raise OperationalError("locked", code=SQLITE_BUSY)
        return "ok"

    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    with patch("dqlitedbapi._busy_retry.asyncio.sleep", side_effect=fake_sleep):
        result = await retry_async_on_busy(5.0, factory)
    assert result == "ok"
    actual_ms = [int(s * 1000) for s in sleeps]
    assert actual_ms == [1, 2, 5, 10, 15]


@pytest.mark.asyncio
async def test_retry_async_raises_when_budget_exhausted() -> None:
    """Budget exhausted → raise the original OperationalError."""

    async def factory() -> str:
        raise OperationalError("locked", code=SQLITE_BUSY)

    async def fake_sleep(d: float) -> None:
        return None

    with (
        patch("dqlitedbapi._busy_retry.asyncio.sleep", side_effect=fake_sleep),
        pytest.raises(OperationalError) as exc_info,
    ):
        await retry_async_on_busy(0.005, factory)  # 5ms budget
    assert exc_info.value.code == SQLITE_BUSY


@pytest.mark.asyncio
async def test_retry_async_non_busy_propagates_immediately() -> None:
    """Different OperationalError code → no retry, no sleep."""

    async def factory() -> str:
        raise OperationalError("locked", code=6)  # SQLITE_LOCKED

    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    with (
        patch("dqlitedbapi._busy_retry.asyncio.sleep", side_effect=fake_sleep),
        pytest.raises(OperationalError) as exc_info,
    ):
        await retry_async_on_busy(5.0, factory)
    assert exc_info.value.code == 6
    assert sleeps == []


@pytest.mark.asyncio
async def test_retry_async_zero_budget_no_retry() -> None:
    """busy_timeout=0 → no retry, first BUSY raises."""
    call_count = [0]

    async def factory() -> str:
        call_count[0] += 1
        raise OperationalError("locked", code=SQLITE_BUSY)

    sleeps: list[float] = []

    async def fake_sleep(d: float) -> None:
        sleeps.append(d)

    with (
        patch("dqlitedbapi._busy_retry.asyncio.sleep", side_effect=fake_sleep),
        pytest.raises(OperationalError),
    ):
        await retry_async_on_busy(0.0, factory)
    assert sleeps == []
    assert call_count[0] == 1


@pytest.mark.asyncio
async def test_retry_async_cancellederror_propagates() -> None:
    """CancelledError during await asyncio.sleep must NOT be caught
    by the ``except OperationalError`` arm — CancelledError is a
    BaseException subclass."""

    async def factory() -> str:
        raise OperationalError("locked", code=SQLITE_BUSY)

    async def fake_sleep(d: float) -> None:
        raise asyncio.CancelledError

    with (
        patch("dqlitedbapi._busy_retry.asyncio.sleep", side_effect=fake_sleep),
        pytest.raises(asyncio.CancelledError),
    ):
        await retry_async_on_busy(5.0, factory)
