"""Pin: ``_build_and_connect`` wraps :class:`BaseExceptionGroup`
from BOTH the leader-resolution arm AND the post-construct connect
arm as :class:`OperationalError` — NOT :class:`DatabaseError` (which
``_call_client`` uses for its own analogous arm).

The wrap-class choice discriminates how SA's engine classifies the
failure at the dbapi boundary:

* ``OperationalError`` triggers SA's ``_handle_dbapi_exception`` →
  ``is_disconnect`` classifier → may invalidate the connection.
* ``DatabaseError`` (the wider class) bypasses ``is_disconnect``
  and may not invalidate — a poisoned connection re-enters the
  pool.

A wrong-class wrap on either ``_build_and_connect`` arm would
silently turn a connect-time aggregate into a poisoned-pool
surface. The sibling ``_call_client`` test pins the class for that
site; these two pins close the gap for ``_build_and_connect``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqliteclient import exceptions as _client_exc
from dqlitedbapi.connection import _build_and_connect
from dqlitedbapi.exceptions import OperationalError


@pytest.mark.asyncio
async def test_build_and_connect_resolve_leader_group_wraps_as_operational_error() -> None:
    """A ``BaseExceptionGroup`` raised from ``_resolve_leader`` must
    surface as :class:`OperationalError` with the canonical
    ``"Failed to find leader"`` prefix, NOT
    :class:`DatabaseError`."""

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
    """A ``BaseExceptionGroup`` raised from inside the post-construct
    ``conn.connect()`` must surface as :class:`OperationalError`
    with the ``"Failed to connect:"`` prefix."""

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
