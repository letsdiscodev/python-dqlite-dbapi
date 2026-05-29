"""``_get_resolve_leader_cluster`` constructs ``ClusterClient`` outside the cache
lock (double-checked init), so a future async ``__init__`` cannot deadlock."""

from __future__ import annotations

import asyncio
import threading
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi import connection as _conn_mod
from dqlitedbapi.connection import _resolve_leader


@pytest.fixture(autouse=True)
def _clear_cache() -> None:
    _conn_mod._RESOLVE_LEADER_CACHE.clear()


@pytest.mark.asyncio
async def test_cluster_client_constructed_without_holding_cache_lock() -> None:
    """The cache lock must NOT be held while ``ClusterClient`` is constructed."""
    lock_held_during_init: list[bool] = []

    def spy_cluster_client(_store: object, **_kwargs: Any) -> MagicMock:
        # Lock is non-reentrant, so a True acquire here proves it is free.
        acquired = _conn_mod._RESOLVE_LEADER_CACHE_LOCK.acquire(blocking=False)
        if acquired:
            lock_held_during_init.append(False)
            _conn_mod._RESOLVE_LEADER_CACHE_LOCK.release()
        else:
            lock_held_during_init.append(True)
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", spy_cluster_client):
        await _resolve_leader("seed:9001", timeout=5.0)

    assert lock_held_during_init == [False], (
        "ClusterClient.__init__ ran while _RESOLVE_LEADER_CACHE_LOCK was "
        "held. The cache lookup-then-construct-then-register sequence "
        "must release the lock for the construction step so a future "
        "async constructor cannot deadlock."
    )


@pytest.mark.asyncio
async def test_concurrent_resolve_leader_calls_publish_single_cluster() -> None:
    """Many concurrent ``_resolve_leader`` calls on the same key must converge
    on one published ``ClusterClient`` instance."""
    constructed: list[MagicMock] = []
    enter_event = threading.Event()
    proceed_event = threading.Event()

    def make_cluster_client(_store: object, **_kwargs: Any) -> MagicMock:
        # Stall the first constructor so others race the recheck path.
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        if not enter_event.is_set():
            enter_event.set()
            proceed_event.wait(timeout=2.0)
        constructed.append(client)
        return client

    async def caller() -> object:
        with patch("dqlitedbapi.connection.ClusterClient", make_cluster_client):
            return await _resolve_leader("seed:9002", timeout=5.0)

    async def release_after_others_queued() -> None:
        await asyncio.to_thread(enter_event.wait, 2.0)
        proceed_event.set()

    results = await asyncio.gather(
        caller(),
        caller(),
        caller(),
        caller(),
        release_after_others_queued(),
    )

    leader_addrs = [r for r in results[:-1]]
    assert all(addr == "leader:9999" for addr in leader_addrs)
    keys = list(_conn_mod._RESOLVE_LEADER_CACHE.keys())
    assert len(keys) == 1, f"expected one cached cluster, got {len(keys)}"
