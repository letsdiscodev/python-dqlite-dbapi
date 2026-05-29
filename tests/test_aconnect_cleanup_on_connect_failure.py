"""aconnect()'s partial-construct cleanup: if connect() raises, the partial conn is closed.

The close is wrapped in suppress(Exception) so a close error doesn't mask the original.
"""

from __future__ import annotations

import asyncio

import pytest

import dqlitedbapi.exceptions
from dqlitedbapi.aio import aconnect
from dqlitedbapi.aio.connection import AsyncConnection


async def test_aconnect_calls_close_on_connect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connect() Exception makes aconnect() close the partial conn and re-raise."""
    closed: list[bool] = []

    async def _failing_connect(self: AsyncConnection) -> None:
        raise dqlitedbapi.exceptions.OperationalError("boom", code=1)

    async def _spy_close(self: AsyncConnection) -> None:
        closed.append(True)

    monkeypatch.setattr(AsyncConnection, "connect", _failing_connect)
    monkeypatch.setattr(AsyncConnection, "close", _spy_close)

    with pytest.raises(dqlitedbapi.exceptions.OperationalError, match="boom"):
        await aconnect("localhost:9001")

    assert closed == [True], "aconnect must close the partial conn on connect failure"


async def test_aconnect_swallows_close_error_to_preserve_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If close() also fails during cleanup, the original connect() exception still propagates."""

    async def _failing_connect(self: AsyncConnection) -> None:
        raise dqlitedbapi.exceptions.OperationalError("primary", code=1)

    async def _failing_close(self: AsyncConnection) -> None:
        raise RuntimeError("secondary close failure")

    monkeypatch.setattr(AsyncConnection, "connect", _failing_connect)
    monkeypatch.setattr(AsyncConnection, "close", _failing_close)

    with pytest.raises(dqlitedbapi.exceptions.OperationalError, match="primary"):
        await aconnect("localhost:9001")


async def test_aconnect_propagates_cancellederror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CancelledError during connect() propagates and still runs cleanup (caught as
    BaseException, not just Exception)."""
    closed: list[bool] = []

    async def _cancelled_connect(self: AsyncConnection) -> None:
        raise asyncio.CancelledError()

    async def _spy_close(self: AsyncConnection) -> None:
        closed.append(True)

    monkeypatch.setattr(AsyncConnection, "connect", _cancelled_connect)
    monkeypatch.setattr(AsyncConnection, "close", _spy_close)

    with pytest.raises(asyncio.CancelledError):
        await aconnect("localhost:9001")

    assert closed == [True]
