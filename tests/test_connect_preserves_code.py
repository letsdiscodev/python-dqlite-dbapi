"""``_build_and_connect`` must forward the client error code.

sqlalchemy-dqlite's ``is_disconnect`` classifier reads ``exc.code``
to identify leader-change failures (``SQLITE_IOERR_NOT_LEADER``,
``SQLITE_IOERR_LEADERSHIP_LOST``). The query path preserves the
code via ``_classify_operational`` in ``cursor.py``, but the
connect path used to catch every exception as a bare ``Exception``
and rebuild the DBAPI ``OperationalError`` without ``code=``,
so leader-change errors hit the brittle substring matcher instead
of the code-based branch.

Peer of ISSUE-296.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.connection import _build_and_connect
from dqlitedbapi.exceptions import OperationalError

_SQLITE_IOERR_NOT_LEADER = 10 | (1 << 8) | (31 << 8)  # DQLITE extended code
_SQLITE_IOERR_NOT_LEADER_FALLBACK = 1032  # known constant from dqlite


@pytest.mark.asyncio
async def test_connect_forwards_operational_error_code() -> None:
    """A client-layer ``OperationalError(code, message)`` raised during
    ``conn.connect()`` is re-raised as a dbapi ``OperationalError``
    with the same ``.code`` — sqlalchemy's ``is_disconnect`` can then
    classify it via the code-based branch.
    """
    client_err = _client_exc.OperationalError(_SQLITE_IOERR_NOT_LEADER_FALLBACK, "not leader")

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
        )

    assert exc_info.value.code == _SQLITE_IOERR_NOT_LEADER_FALLBACK
    # Message prefix "Failed to connect: " is preserved (tests match on it).
    assert str(exc_info.value).startswith("Failed to connect: ")


@pytest.mark.asyncio
async def test_connect_non_code_exception_yields_code_none() -> None:
    """Non-client exceptions (e.g. ``OSError``) should still produce a
    dbapi ``OperationalError`` — but with ``.code is None`` because
    there is no server error code to forward.
    """

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
        )

    assert exc_info.value.code is None
    assert str(exc_info.value).startswith("Failed to connect: ")
