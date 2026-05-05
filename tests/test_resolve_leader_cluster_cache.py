"""``_resolve_leader`` shares a process-wide ``ClusterClient`` per
``(address, governors)`` tuple.

Without sharing, every dbapi ``connect()`` / SA pool slot warm-up
constructs a fresh ``ClusterClient`` and discards it on return —
defeating the single-flight ``_find_leader_tasks`` collapse and the
``_last_known_leader`` fast-path that ``ClusterClient`` already
implements. Under N concurrent SA pool checkouts after a leader flip,
the cluster sees N independent leader-discovery sweeps where one
would suffice.

This test pins the per-key reuse and the per-governor isolation, and
validates wholesale invalidation on the fork-pid token used by the
underlying ``DqliteConnection`` fork-safety machinery.
"""

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqliteclient import connection as _client_conn_mod
from dqlitedbapi import connection as _conn_mod
from dqlitedbapi.connection import _resolve_leader


@pytest.mark.asyncio
async def test_resolve_leader_reuses_cluster_client_for_same_key() -> None:
    """Two ``_resolve_leader`` calls with the same address+governors
    must share a single ``ClusterClient`` instance — that is what
    keeps the leader-tracker fast-path effective at the dbapi layer."""
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

    # Three calls but only ONE ClusterClient construction.
    assert construct_count == 1


@pytest.mark.asyncio
async def test_resolve_leader_isolates_distinct_governors() -> None:
    """Different governor tuples must produce distinct
    ClusterClient instances — sharing would cross-contaminate
    the trust_server_heartbeat opt-in."""
    constructed_kwargs: list[dict[str, object]] = []

    def fake_cluster_client(_store: object, **kwargs: object) -> MagicMock:
        constructed_kwargs.append(kwargs)
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", fake_cluster_client):
        await _resolve_leader("seed:9001", timeout=5.0, trust_server_heartbeat=False)
        await _resolve_leader("seed:9001", timeout=5.0, trust_server_heartbeat=True)
        # Repeat each — cache hits.
        await _resolve_leader("seed:9001", timeout=5.0, trust_server_heartbeat=False)
        await _resolve_leader("seed:9001", timeout=5.0, trust_server_heartbeat=True)

    # Two distinct keys → two constructions; the repeats are cached.
    assert len(constructed_kwargs) == 2
    heartbeat_settings = {kw["trust_server_heartbeat"] for kw in constructed_kwargs}
    assert heartbeat_settings == {False, True}


@pytest.mark.asyncio
async def test_resolve_leader_cache_invalidates_on_fork_pid_change() -> None:
    """Fork in a child process must wholesale-clear the cache:
    a ClusterClient inherited from the parent carries
    parent-allocated asyncio.Lock / Task references that the
    child's event loop cannot make progress on. The fork-pid
    token that DqliteConnection uses for fork-safety is the
    sentinel."""
    construct_count = 0

    def fake_cluster_client(_store: object, **_kwargs: object) -> MagicMock:
        nonlocal construct_count
        construct_count += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", fake_cluster_client):
        await _resolve_leader("seed:9001", timeout=5.0)
        # Simulate fork: the client-side _current_pid is what
        # _refresh_pid_cache writes after_in_child. Bump it to a
        # value that cannot collide with the cache's recorded pid.
        with patch.object(_client_conn_mod, "_current_pid", os.getpid() + 1):
            await _resolve_leader("seed:9001", timeout=5.0)

    # Two constructions: one pre-fork, one post-fork.
    assert construct_count == 2


@pytest.mark.asyncio
async def test_resolve_leader_cache_evicts_at_max_size() -> None:
    """LRU-ish eviction: a cache that grew past the cap must drop
    its oldest entry rather than leak unbounded under adversarial
    governor-fragmentation."""
    construct_count = 0

    def fake_cluster_client(_store: object, **_kwargs: object) -> MagicMock:
        nonlocal construct_count
        construct_count += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    cap = _conn_mod._RESOLVE_LEADER_CACHE_MAX
    with patch("dqlitedbapi.connection.ClusterClient", fake_cluster_client):
        # Fill to cap with distinct timeouts.
        for i in range(cap):
            await _resolve_leader("seed:9001", timeout=float(i + 1))
        assert len(_conn_mod._RESOLVE_LEADER_CACHE) == cap

        # One more entry must NOT push the cache past the cap.
        await _resolve_leader("seed:9001", timeout=float(cap + 100))
        assert len(_conn_mod._RESOLVE_LEADER_CACHE) == cap

    # Exactly cap+1 constructions occurred.
    assert construct_count == cap + 1
