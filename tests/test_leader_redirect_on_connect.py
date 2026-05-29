"""``dqlitedbapi.connect`` does leader-redirect-on-connect: bootstrap from the
seed, find the leader, then open the database against the leader's address."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.connection import Connection, _build_and_connect, _resolve_leader
from dqlitedbapi.exceptions import InterfaceError, OperationalError

_FakeFindLeader = Callable[[str], Awaitable[str]]


async def test_resolve_leader_returns_seed_when_seed_is_leader() -> None:
    fake_find = AsyncMock(return_value="localhost:9001")
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        instance = MagicMock()
        instance.find_leader = fake_find
        MockCluster.return_value = instance

        result = await _resolve_leader("localhost:9001", timeout=5.0)

    assert result == "localhost:9001"
    fake_find.assert_awaited_once()


async def test_resolve_leader_returns_redirect_address() -> None:
    fake_find = AsyncMock(return_value="node2:9002")
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        instance = MagicMock()
        instance.find_leader = fake_find
        MockCluster.return_value = instance

        result = await _resolve_leader("node1:9001", timeout=5.0)

    assert result == "node2:9002"


async def test_resolve_leader_propagates_cluster_error() -> None:
    """``ClusterError`` propagates so ``_build_and_connect`` can map it."""
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        instance = MagicMock()
        instance.find_leader = AsyncMock(side_effect=_client_exc.ClusterError("no leader known"))
        MockCluster.return_value = instance

        with pytest.raises(_client_exc.ClusterError):
            await _resolve_leader("seed:9001", timeout=5.0)


async def test_resolve_leader_propagates_cluster_policy_error() -> None:
    """``ClusterPolicyError`` (operator allowlist) propagates for mapping upstream."""
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        instance = MagicMock()
        instance.find_leader = AsyncMock(side_effect=_client_exc.ClusterPolicyError("rejected"))
        MockCluster.return_value = instance

        with pytest.raises(_client_exc.ClusterPolicyError):
            await _resolve_leader("seed:9001", timeout=5.0)


async def test_build_and_connect_uses_leader_address_for_dqlite_connection() -> None:
    """``DqliteConnection`` is opened against the leader's address, not the seed."""
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

    args, _kwargs = MockConn.call_args
    assert args[0] == "leader:9999"


async def test_build_and_connect_translates_cluster_error_to_operational() -> None:
    """No leader reachable: surface as ``OperationalError`` (transient for SA pool retry)."""
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


async def test_build_and_connect_translates_cluster_policy_to_interface() -> None:
    """Allowlist rejection: ``InterfaceError`` (permanent, so SA must not retry)."""
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


async def test_build_and_connect_mid_flip_leader_change_propagates() -> None:
    """Leader steps down between find_leader and connect: NOT_LEADER maps to
    ``OperationalError`` (transient) so SA's pool retry kicks in."""
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


async def test_resolve_leader_threads_governors_to_cluster_client() -> None:
    """Governors must reach the ``ClusterClient`` so leader discovery (the first
    round-trip) honours per-connection settings, not defaults."""
    captured: dict[str, object] = {}

    def fake_cluster_client(store: object, **kwargs: object) -> object:
        captured.update(kwargs)
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", fake_cluster_client):
        result = await _resolve_leader(
            "seed:9001",
            timeout=5.0,
            max_total_rows=None,
            max_continuation_frames=42,
            trust_server_heartbeat=True,
        )

    assert result == "leader:9999"
    assert captured["timeout"] == 5.0
    assert captured["max_total_rows"] is None
    assert captured["max_continuation_frames"] == 42
    assert captured["trust_server_heartbeat"] is True


def test_connection_connect_uses_leader_address(monkeypatch: pytest.MonkeyPatch) -> None:
    """The seed -> leader redirect propagates through ``Connection.connect``."""
    captured_addresses: list[str] = []

    async def fake_resolve(address: str, *, timeout: float, **_kw: object) -> str:
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
