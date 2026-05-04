"""``dqlitedbapi.connect`` does leader-redirect-on-connect.

Pins the production-grade connect path: bootstrap from the
user-supplied address, find the current leader via the cluster
client, then open the database against the leader's address. The
behaviour mirrors go-dqlite's ``database/sql`` driver layering
(``client.NewLeaderConnector(store)``) and is what lets the
SA pool's reconnect-after-pre-ping path recover from a leader
flip without surfacing ``SQLITE_IOERR_NOT_LEADER`` to the
caller.

Tests cover the unit boundary at ``_build_and_connect`` —
mocking the ``ClusterClient`` and ``DqliteConnection`` so the
flows pin without a live cluster:

- Happy path: seed IS the leader → ``find_leader`` returns the
  seed address verbatim → ``DqliteConnection`` is constructed
  with the seed.
- Redirect: seed is a follower → ``find_leader`` returns a
  different address → ``DqliteConnection`` is constructed with
  the leader's address.
- Failure: seed unreachable → ``ClusterError`` → translated to
  ``OperationalError`` with the canonical
  ``"Failed to find leader from"`` prefix.
- Failure: seed reachable but no leader yet (transient) →
  ``ClusterError`` → ``OperationalError``.
- Failure: cluster-policy rejection during leader discovery →
  ``InterfaceError``.
- Mid-connect leader change: ``find_leader`` returns X, X then
  refuses with NOT_LEADER → existing post-find-leader arm
  translates to ``OperationalError``; pool retry kicks in.

Live-cluster integration coverage is in
``tests/integration/test_leader_redirect_live.py``.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.connection import Connection, _build_and_connect, _resolve_leader
from dqlitedbapi.exceptions import InterfaceError, OperationalError

_FakeFindLeader = Callable[[str], Awaitable[str]]


# --- _resolve_leader (tests the helper directly) ---


@pytest.mark.asyncio
async def test_resolve_leader_returns_seed_when_seed_is_leader() -> None:
    """Happy path: ``find_leader`` returns the seed address verbatim
    (the seed-as-leader case)."""
    fake_find = AsyncMock(return_value="localhost:9001")
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        instance = MagicMock()
        instance.find_leader = fake_find
        MockCluster.return_value = instance

        result = await _resolve_leader("localhost:9001", timeout=5.0)

    assert result == "localhost:9001"
    fake_find.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_leader_returns_redirect_address() -> None:
    """Redirect: seed is a follower; ``find_leader`` returns a
    different address — that's what ``_build_and_connect`` will
    actually open the database against."""
    fake_find = AsyncMock(return_value="node2:9002")
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        instance = MagicMock()
        instance.find_leader = fake_find
        MockCluster.return_value = instance

        result = await _resolve_leader("node1:9001", timeout=5.0)

    assert result == "node2:9002"


@pytest.mark.asyncio
async def test_resolve_leader_propagates_cluster_error() -> None:
    """Seed unreachable / no-leader-yet: ``ClusterError`` propagates
    so ``_build_and_connect``'s arm can translate to
    ``OperationalError``."""
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        instance = MagicMock()
        instance.find_leader = AsyncMock(side_effect=_client_exc.ClusterError("no leader known"))
        MockCluster.return_value = instance

        with pytest.raises(_client_exc.ClusterError):
            await _resolve_leader("seed:9001", timeout=5.0)


@pytest.mark.asyncio
async def test_resolve_leader_propagates_cluster_policy_error() -> None:
    """Cluster-policy rejection (operator allowlist denies a
    redirect target) propagates so ``_build_and_connect``'s arm
    can translate to ``InterfaceError``."""
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        instance = MagicMock()
        instance.find_leader = AsyncMock(side_effect=_client_exc.ClusterPolicyError("rejected"))
        MockCluster.return_value = instance

        with pytest.raises(_client_exc.ClusterPolicyError):
            await _resolve_leader("seed:9001", timeout=5.0)


# --- _build_and_connect (the wrapping connect path) ---


@pytest.mark.asyncio
async def test_build_and_connect_uses_leader_address_for_dqlite_connection() -> None:
    """Pin the load-bearing wiring: the address passed to
    ``DqliteConnection(...)`` is the leader's address, not the
    seed. Without this, a leader-flip-after-bootstrap would route
    the OPEN_DATABASE to the wrong node."""
    with (
        patch("dqlitedbapi.connection._resolve_leader") as mock_resolve,
        patch("dqlitedbapi.connection.DqliteConnection") as MockConn,
    ):
        mock_resolve.return_value = "leader:9999"
        instance = AsyncMock()
        instance.connect = AsyncMock()
        MockConn.return_value = instance

        await _build_and_connect(
            "seed:9001",
            database="default",
            timeout=5.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=0.5,
        )

    # ``DqliteConnection`` was constructed with the leader's
    # address, not the seed.
    args, _kwargs = MockConn.call_args
    assert args[0] == "leader:9999"


@pytest.mark.asyncio
async def test_build_and_connect_translates_cluster_error_to_operational() -> None:
    """No leader reachable at all: surface as
    ``OperationalError`` so the SA pool's retry loop classifies
    it as transient."""
    with patch("dqlitedbapi.connection._resolve_leader") as mock_resolve:
        mock_resolve.side_effect = _client_exc.ClusterError("no nodes responded")

        with pytest.raises(OperationalError, match="Failed to find leader from seed:9001"):
            await _build_and_connect(
                "seed:9001",
                database="default",
                timeout=5.0,
                max_total_rows=None,
                max_continuation_frames=None,
                trust_server_heartbeat=False,
                close_timeout=0.5,
            )


@pytest.mark.asyncio
async def test_build_and_connect_translates_cluster_policy_to_interface() -> None:
    """Operator allowlist rejected a redirect target: surface as
    ``InterfaceError`` (permanent config mismatch — SA's
    ``is_disconnect`` should NOT enter a retry loop)."""
    with patch("dqlitedbapi.connection._resolve_leader") as mock_resolve:
        mock_resolve.side_effect = _client_exc.ClusterPolicyError("rejected")

        with pytest.raises(
            InterfaceError, match="Cluster policy rejection during leader discovery"
        ):
            await _build_and_connect(
                "seed:9001",
                database="default",
                timeout=5.0,
                max_total_rows=None,
                max_continuation_frames=None,
                trust_server_heartbeat=False,
                close_timeout=0.5,
            )


@pytest.mark.asyncio
async def test_build_and_connect_mid_flip_leader_change_propagates() -> None:
    """``find_leader`` returns X, X then steps down between the
    two round-trips: ``DqliteConnection.connect`` sees
    NOT_LEADER → translated by the existing post-find-leader
    ``DqliteConnectionError`` arm to ``OperationalError`` with
    the canonical ``Failed to connect:`` prefix. SA's pool
    retry then kicks in."""
    with (
        patch("dqlitedbapi.connection._resolve_leader") as mock_resolve,
        patch("dqlitedbapi.connection.DqliteConnection") as MockConn,
    ):
        mock_resolve.return_value = "leader:9999"
        instance = AsyncMock()
        instance.connect = AsyncMock(
            side_effect=_client_exc.DqliteConnectionError(
                "Node leader:9999 is no longer leader: transferred",
                code=10250,
                raw_message="transferred",
            )
        )
        MockConn.return_value = instance

        with pytest.raises(OperationalError, match="Failed to connect:"):
            await _build_and_connect(
                "seed:9001",
                database="default",
                timeout=5.0,
                max_total_rows=None,
                max_continuation_frames=None,
                trust_server_heartbeat=False,
                close_timeout=0.5,
            )


# --- end-to-end via Connection (sync surface) ---


def test_connection_connect_uses_leader_address(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sanity through the public ``Connection.connect`` surface:
    the seed → leader redirect propagates all the way down to the
    inner ``DqliteConnection``."""
    captured_addresses: list[str] = []

    async def fake_resolve(address: str, *, timeout: float) -> str:
        return "leader:9999"

    def capture_dqlite_connection(address: str, *args: object, **kwargs: object) -> AsyncMock:
        captured_addresses.append(address)
        instance = AsyncMock()
        instance.connect = AsyncMock()
        return instance

    monkeypatch.setattr("dqlitedbapi.connection._resolve_leader", fake_resolve)
    monkeypatch.setattr(
        "dqlitedbapi.connection.DqliteConnection",
        capture_dqlite_connection,
    )

    conn = Connection("seed:9001", timeout=2.0)
    try:
        conn.connect()
        assert captured_addresses == ["leader:9999"]
    finally:
        conn.close()
