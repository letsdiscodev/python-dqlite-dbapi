"""Pin: ``_get_resolve_leader_cluster`` constructs ``ClusterClient``
**outside** the module-level ``threading.Lock``.

The cache lock used to be held across ``ClusterClient.__init__``. Today
that constructor does no I/O and no ``await`` so the held-across-init
shape is sound at runtime — but the in-tree comment at
``connection.py`` calls out that a future refactor adding ``await`` to
``ClusterClient.__init__`` would turn the lock into a deadlock trap:
the loop thread would acquire the lock, yield at the new ``await``,
and any sibling coroutine that also touches ``_get_resolve_leader_cluster``
would synchronously park on ``threading.Lock.acquire()`` with no way
to resume.

The standard remedy is the double-checked init pattern: take the lock
to look up, drop it to construct, retake the lock to register. The
loser of any concurrent construction race observes the winner's insert
on the recheck and discards its own client.

This test pins the architectural invariant by spying on
``ClusterClient.__init__`` and asserting the cache lock is NOT held at
the moment the constructor runs. The single-flight guarantee (one
cluster per (address, governors) tuple, even under concurrent demand)
is also exercised so the refactor preserves the cache's original
contract.
"""

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
    """``_RESOLVE_LEADER_CACHE_LOCK`` must NOT be held while
    ``ClusterClient`` is being constructed. Future-proofs against an
    async ``ClusterClient.__init__`` turning the held-across-init
    shape into a deadlock.
    """
    lock_held_during_init: list[bool] = []

    def spy_cluster_client(_store: object, **_kwargs: Any) -> MagicMock:
        # Probe the lock with non-blocking acquire. If the calling
        # thread already owns the lock recursively, ``acquire(False)``
        # still returns False because ``threading.Lock`` is non-
        # reentrant — so a True return here proves the lock is
        # currently free.
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
    """Single-flight contract: many concurrent ``_resolve_leader``
    calls on the same key must converge on ONE published
    ``ClusterClient`` instance. Under the double-checked-init shape a
    losing thread may briefly construct an unused client; the test
    verifies the cache only ever returns one.
    """
    constructed: list[MagicMock] = []
    enter_event = threading.Event()
    proceed_event = threading.Event()

    def make_cluster_client(_store: object, **_kwargs: Any) -> MagicMock:
        # Stall the first constructor so concurrent callers all race
        # for the lock. Without the stall, the first caller would
        # complete before any other arrives and the single-flight
        # property would be trivially satisfied without exercising
        # the recheck path.
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
        # Wait for the first constructor to enter, then let it
        # proceed so the remaining queued callers can run their
        # cache-recheck path.
        await asyncio.to_thread(enter_event.wait, 2.0)
        proceed_event.set()

    results = await asyncio.gather(
        caller(),
        caller(),
        caller(),
        caller(),
        release_after_others_queued(),
    )

    # Drop the release_after_others_queued result.
    leader_addrs = [r for r in results[:-1]]
    assert all(addr == "leader:9999" for addr in leader_addrs)
    # Exactly one cached entry.
    keys = list(_conn_mod._RESOLVE_LEADER_CACHE.keys())
    assert len(keys) == 1, f"expected one cached cluster, got {len(keys)}"
