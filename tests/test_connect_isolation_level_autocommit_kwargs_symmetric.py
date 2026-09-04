"""Constructor and setter accept the same no-op sentinels for
``isolation_level``/``autocommit``; non-sentinel values are rejected on both."""

from __future__ import annotations

from unittest.mock import patch

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import NotSupportedError, ProgrammingError


def test_sync_connect_accepts_isolation_level_none() -> None:
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


def test_sync_connect_accepts_autocommit_true() -> None:
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=True)
    _ctor.assert_called_once()


def test_sync_connect_accepts_autocommit_minus_one() -> None:
    """Stdlib's ``LEGACY_TRANSACTION_CONTROL`` sentinel (-1) is an accepted no-op."""
    with patch("dqlitedbapi.Connection") as _ctor:
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=-1)
    _ctor.assert_called_once()


def test_sync_connect_rejects_isolation_level_unknown_string() -> None:
    """Unknown strings stay rejected even though the setter accepts the stdlib set."""
    with pytest.raises(ProgrammingError, match="isolation_level"):
        dqlitedbapi.connect("127.0.0.1:9001", isolation_level="SERIALIZABLE")


def test_sync_connect_rejects_autocommit_false() -> None:
    with pytest.raises(NotSupportedError, match="autocommit"):
        dqlitedbapi.connect("127.0.0.1:9001", autocommit=False)


def test_async_connect_accepts_isolation_level_none() -> None:
    from dqlitedbapi.aio import connect as aio_connect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        aio_connect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


def test_async_connect_rejects_isolation_level_unknown_string() -> None:
    from dqlitedbapi.aio import connect as aio_connect

    with pytest.raises(ProgrammingError, match="isolation_level"):
        aio_connect("127.0.0.1:9001", isolation_level="SERIALIZABLE")


@pytest.mark.asyncio
async def test_aconnect_accepts_isolation_level_none() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", isolation_level=None)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_accepts_autocommit_true() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", autocommit=True)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_accepts_autocommit_minus_one() -> None:
    from unittest.mock import AsyncMock

    from dqlitedbapi.aio import aconnect

    with patch("dqlitedbapi.aio.AsyncConnection") as _ctor:
        _ctor.return_value.connect = AsyncMock()
        await aconnect("127.0.0.1:9001", autocommit=-1)
    _ctor.assert_called_once()


@pytest.mark.asyncio
async def test_aconnect_rejects_isolation_level_unknown_string() -> None:
    from dqlitedbapi.aio import aconnect

    with pytest.raises(ProgrammingError, match="isolation_level"):
        await aconnect("127.0.0.1:9001", isolation_level="SERIALIZABLE")


@pytest.mark.asyncio
async def test_aconnect_rejects_autocommit_false() -> None:
    from dqlitedbapi.aio import aconnect

    with pytest.raises(NotSupportedError, match="autocommit"):
        await aconnect("127.0.0.1:9001", autocommit=False)
