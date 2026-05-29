"""Pin: ``_build_and_connect`` wraps BaseExceptionGroup (both leader-resolution and
post-construct connect arms) as OperationalError, not DatabaseError. The class choice
drives SA's is_disconnect classifier; the wider DatabaseError would leave a poisoned
connection in the pool."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqliteclient import exceptions as _client_exc
from dqlitedbapi.connection import _build_and_connect
from dqlitedbapi.exceptions import OperationalError


@pytest.mark.asyncio
async def test_build_and_connect_resolve_leader_group_wraps_as_operational_error() -> None:
    """Group from _resolve_leader surfaces as OperationalError with the
    "Failed to find leader" prefix."""

    async def _group(*_a: object, **_kw: object) -> str:
        raise BaseExceptionGroup(
            "synthetic-aggregate",
            [
                _client_exc.DqliteConnectionError("seed-A"),
                _client_exc.DqliteConnectionError("seed-B"),
            ],
        )

    with (
        patch("dqlitedbapi.connection._resolve_leader", new=_group),
        pytest.raises(OperationalError) as info,
    ):
        await _build_and_connect(
            "127.0.0.1:9001",
            database="default",
            timeout=2.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=1.0,
        )

    err = info.value
    msg = str(err)
    assert "Failed to find leader" in msg, (
        "leader-resolution arm must keep the canonical prefix so SA's "
        "substring-based classifier and test assertions stay stable"
    )
    assert "2 child" in msg, (
        "aggregate diagnostic must surface the child-count so operators "
        "see the breadth of the underlying failure"
    )
    assert isinstance(err.__cause__, BaseExceptionGroup), (
        "the original group must survive on __cause__ so SA's "
        "_walk_cause_chain can classify the leaves"
    )


@pytest.mark.asyncio
async def test_build_and_connect_post_construct_group_wraps_as_operational_error() -> None:
    """Group from post-construct conn.connect() surfaces as OperationalError with
    the "Failed to connect:" prefix."""

    async def _ok_resolve(*_a: object, **_kw: object) -> str:
        return "127.0.0.1:9001"

    mock_conn = MagicMock()
    mock_conn.connect = AsyncMock(
        side_effect=BaseExceptionGroup(
            "synthetic-aggregate",
            [
                _client_exc.DqliteConnectionError("connect-A"),
                _client_exc.DqliteConnectionError("connect-B"),
            ],
        )
    )

    with (
        patch("dqlitedbapi.connection._resolve_leader", new=_ok_resolve),
        patch("dqlitedbapi.connection.DqliteConnection", return_value=mock_conn),
        pytest.raises(OperationalError) as info,
    ):
        await _build_and_connect(
            "127.0.0.1:9001",
            database="default",
            timeout=2.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=1.0,
        )

    err = info.value
    msg = str(err)
    assert "Failed to connect" in msg, (
        "post-construct arm must keep the canonical prefix distinct "
        "from the leader-resolution prefix"
    )
    assert "Failed to find leader" not in msg, (
        "the two arms must not collapse into the same diagnostic prefix"
    )
    assert "2 child" in msg
    assert isinstance(err.__cause__, BaseExceptionGroup)
