"""``_build_and_connect`` forwards the client error code so sqlalchemy's
``is_disconnect`` can classify leader-change errors by code, not substring."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.connection import _build_and_connect
from dqlitedbapi.exceptions import OperationalError

_SQLITE_IOERR_NOT_LEADER = 10 | (1 << 8) | (31 << 8)  # DQLITE extended code
_SQLITE_IOERR_NOT_LEADER_FALLBACK = 1032  # known constant from dqlite


async def test_connect_forwards_operational_error_code() -> None:
    client_err = _client_exc.OperationalError("not leader", _SQLITE_IOERR_NOT_LEADER_FALLBACK)

    async def fake_connect() -> None:
        raise client_err

    with (
        patch(
            "dqlitedbapi.connection.DqliteConnection.connect",
            new=AsyncMock(side_effect=fake_connect),
        ),
        pytest.raises(OperationalError) as exc_info,
    ):
        await _build_and_connect(
            "localhost:9001",
            database="test",
            timeout=1.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=0.5,
        )

    assert exc_info.value.code == _SQLITE_IOERR_NOT_LEADER_FALLBACK
    assert str(exc_info.value).startswith("Failed to connect: ")


async def test_connect_non_code_exception_yields_code_none() -> None:
    """Non-client exceptions yield ``OperationalError`` with ``.code is None``."""

    async def fake_connect_os_error() -> None:
        raise OSError("unreachable")

    with (
        patch(
            "dqlitedbapi.connection.DqliteConnection.connect",
            new=AsyncMock(side_effect=fake_connect_os_error),
        ),
        pytest.raises(OperationalError) as exc_info,
    ):
        await _build_and_connect(
            "localhost:9001",
            database="test",
            timeout=1.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=0.5,
        )

    assert exc_info.value.code is None
    assert str(exc_info.value).startswith("Failed to connect: ")
