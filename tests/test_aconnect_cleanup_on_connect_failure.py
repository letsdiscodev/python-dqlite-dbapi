"""Pin ``aconnect()``'s partial-construct cleanup contract.

If ``AsyncConnection.connect()`` raises after the constructor
returned, ``aconnect()``'s except-clause closes the partially-
constructed connection (with ``contextlib.suppress(Exception)`` so
the close error doesn't mask the original) and re-raises. Without
coverage, a refactor that loses the suppress wrapper or the close
call would silently leak loop-bound locks, transports, and reader
tasks on a connect failure.

Drives ``aio/__init__.py:198-209`` reported as uncovered by
``pytest --cov``.
"""

from __future__ import annotations

import asyncio

import pytest

import dqlitedbapi.exceptions
from dqlitedbapi.aio import aconnect
from dqlitedbapi.aio.connection import AsyncConnection


@pytest.mark.asyncio
async def test_aconnect_calls_close_on_connect_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If AsyncConnection.connect() raises an Exception, aconnect()
    must call close() on the partially-constructed instance and
    re-raise the original exception."""
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


@pytest.mark.asyncio
async def test_aconnect_swallows_close_error_to_preserve_original(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If close() ALSO fails during cleanup, the original connect()
    exception must still propagate (close failure is secondary).
    Pin the contextlib.suppress(Exception) wrapper around the
    cleanup close."""

    async def _failing_connect(self: AsyncConnection) -> None:
        raise dqlitedbapi.exceptions.OperationalError("primary", code=1)

    async def _failing_close(self: AsyncConnection) -> None:
        raise RuntimeError("secondary close failure")

    monkeypatch.setattr(AsyncConnection, "connect", _failing_connect)
    monkeypatch.setattr(AsyncConnection, "close", _failing_close)

    with pytest.raises(dqlitedbapi.exceptions.OperationalError, match="primary"):
        await aconnect("localhost:9001")


@pytest.mark.asyncio
async def test_aconnect_propagates_cancellederror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CancelledError during connect() must propagate (BaseException
    catch covers it, not just Exception) AND the close cleanup must
    still run. Pin the BaseException-vs-Exception choice — narrowing
    to Exception would skip cleanup on async cancellation."""
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
