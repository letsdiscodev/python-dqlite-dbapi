"""``_resolve_leader`` shares a process-wide ``ClusterClient`` per
``(address, governors)`` tuple to keep the leader-tracker fast-path effective."""

import os
from unittest.mock import AsyncMock, MagicMock, patch

from dqlitedbapi import connection as _conn_mod
from dqlitedbapi.connection import _resolve_leader


async def test_resolve_leader_reuses_cluster_client_for_same_key() -> None:
    """Same address+governors must share a single ``ClusterClient`` instance."""
    construct_count = 0

    def fake_cluster_client(_store: object, **_kwargs: object) -> MagicMock:
        nonlocal construct_count
        construct_count += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", fake_cluster_client):
        await _resolve_leader("seed:9001", timeout=5.0)
        await _resolve_leader("seed:9001", timeout=5.0)
        await _resolve_leader("seed:9001", timeout=5.0)

    assert construct_count == 1


async def test_resolve_leader_isolates_distinct_governors() -> None:
    """Different governor tuples must produce distinct ClusterClient instances:
    sharing would cross-contaminate the trust_server_heartbeat opt-in."""
    constructed_kwargs: list[dict[str, object]] = []

    def fake_cluster_client(_store: object, **kwargs: object) -> MagicMock:
        constructed_kwargs.append(kwargs)
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", fake_cluster_client):
        await _resolve_leader("seed:9001", timeout=5.0, trust_server_heartbeat=False)
        await _resolve_leader("seed:9001", timeout=5.0, trust_server_heartbeat=True)
        await _resolve_leader("seed:9001", timeout=5.0, trust_server_heartbeat=False)
        await _resolve_leader("seed:9001", timeout=5.0, trust_server_heartbeat=True)

    assert len(constructed_kwargs) == 2
    heartbeat_settings = {kw["trust_server_heartbeat"] for kw in constructed_kwargs}
    assert heartbeat_settings == {False, True}


async def test_resolve_leader_cache_invalidates_on_fork_pid_change() -> None:
    """Fork must clear the cache: an inherited ClusterClient holds parent
    asyncio.Lock / Task refs the child's loop cannot progress on."""
    construct_count = 0

    def fake_cluster_client(_store: object, **_kwargs: object) -> MagicMock:
        nonlocal construct_count
        construct_count += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", fake_cluster_client):
        await _resolve_leader("seed:9001", timeout=5.0)
        # Simulate fork by bumping the pid the cache keys on.
        with patch("dqliteclient.connection.os.getpid", return_value=os.getpid() + 1):
            await _resolve_leader("seed:9001", timeout=5.0)

    assert construct_count == 2


async def test_resolve_leader_cache_evicts_at_max_size() -> None:
    """A cache grown past the cap must drop its oldest entry, not leak unbounded."""
    construct_count = 0

    def fake_cluster_client(_store: object, **_kwargs: object) -> MagicMock:
        nonlocal construct_count
        construct_count += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    cap = _conn_mod._RESOLVE_LEADER_CACHE_MAX
    with patch("dqlitedbapi.connection.ClusterClient", fake_cluster_client):
        for i in range(cap):
            await _resolve_leader("seed:9001", timeout=float(i + 1))
        assert len(_conn_mod._RESOLVE_LEADER_CACHE) == cap

        await _resolve_leader("seed:9001", timeout=float(cap + 100))
        assert len(_conn_mod._RESOLVE_LEADER_CACHE) == cap

    assert construct_count == cap + 1
