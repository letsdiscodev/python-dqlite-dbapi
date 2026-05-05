"""Pin: ``_RESOLVE_LEADER_CACHE`` must isolate `ClusterClient`
instances by event loop AND must serialise concurrent
construction across threads.

Without loop isolation, two `dqlitedbapi.Connection` instances
running on different event-loop threads share a `ClusterClient`
whose `_find_leader_tasks` are bound to whichever loop made the
first call. The second loop's `await asyncio.shield(<foreign-loop
task>)` raises `RuntimeError("attached to a different loop")`,
which is NOT a `dbapi.Error` and escapes SA's `is_disconnect`.

Without thread synchronisation, concurrent first-time inserts can
both observe `cluster is None`, each construct a fresh
`ClusterClient`, and race on the dict insert — orphaning whichever
client loses the race and defeating the single-flight collapse the
cache was designed to provide.
"""

import asyncio
import os
import threading
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqliteclient import connection as _client_conn_mod
from dqlitedbapi import connection as _conn_mod
from dqlitedbapi.connection import _get_resolve_leader_cluster
from dqlitedbapi.exceptions import InterfaceError


@pytest.fixture(autouse=True)
def _clear_cache() -> Iterator[None]:
    """The autouse fixture in conftest already clears the cache;
    keep this here for explicit local override."""
    _conn_mod._RESOLVE_LEADER_CACHE.clear()
    yield
    _conn_mod._RESOLVE_LEADER_CACHE.clear()


def _make_cluster_kwargs() -> dict[str, Any]:
    return {
        "address": "h:9001",
        "timeout": 5.0,
        "max_total_rows": None,
        "max_continuation_frames": None,
        "trust_server_heartbeat": False,
    }


def test_resolve_leader_outside_running_loop_raises() -> None:
    """The function is async-only by design; calling it from sync
    context fails loud rather than silently caching against a None
    loop_id."""
    with pytest.raises(InterfaceError, match="running event loop"):
        _get_resolve_leader_cluster(**_make_cluster_kwargs())


def test_two_event_loops_get_distinct_cluster_clients() -> None:
    """Pin: same args, two different loops → two different cached
    `ClusterClient` instances. Without loop isolation the second
    loop reuses the first's cluster whose `_find_leader_tasks` are
    loop-bound."""
    results: list[object] = []

    def thread_target() -> None:
        loop = asyncio.new_event_loop()
        try:

            async def _call() -> object:
                with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
                    MockCluster.side_effect = lambda *_a, **_kw: MagicMock()
                    return _get_resolve_leader_cluster(**_make_cluster_kwargs())

            results.append(loop.run_until_complete(_call()))
        finally:
            loop.close()

    t1 = threading.Thread(target=thread_target)
    t2 = threading.Thread(target=thread_target)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(results) == 2
    # Distinct loops must yield distinct cluster instances.
    assert results[0] is not results[1]


@pytest.mark.asyncio
async def test_same_loop_returns_same_cluster_client() -> None:
    """Positive regression — single loop, two calls, same args →
    same cluster (single-flight contract preserved)."""
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        MockCluster.side_effect = lambda *_a, **_kw: MagicMock()
        c1 = _get_resolve_leader_cluster(**_make_cluster_kwargs())
        c2 = _get_resolve_leader_cluster(**_make_cluster_kwargs())
    assert c1 is c2


def test_concurrent_first_inserts_yield_one_cluster_per_loop() -> None:
    """Pin: N threads each driving their OWN loops, all calling
    with the same address+governors, must produce N distinct
    cluster instances (one per loop). Loops are kept alive
    concurrently via a barrier so CPython does NOT recycle their
    ``id()`` between thread executions — without that, the cache
    key would silently collide on recycled ids and over-share."""
    n = 8
    construct_count = [0]
    construct_lock = threading.Lock()
    results: list[object] = []
    results_lock = threading.Lock()
    # Each thread sets ``acquired`` after capturing its cluster.
    # Main thread waits for all to acquire, then signals release.
    barrier = threading.Barrier(n + 1)
    release = threading.Event()

    def fake_cluster_client(*_a: object, **_kw: object) -> MagicMock:
        with construct_lock:
            construct_count[0] += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    def thread_target() -> None:
        loop = asyncio.new_event_loop()
        try:

            async def _call_and_hold() -> object:
                with patch(
                    "dqlitedbapi.connection.ClusterClient",
                    side_effect=fake_cluster_client,
                ):
                    r = _get_resolve_leader_cluster(**_make_cluster_kwargs())
                # Hold the loop alive until the main thread says go.
                while not release.is_set():
                    await asyncio.sleep(0.005)
                return r

            # Synchronise all threads at the start so all loops are
            # alive concurrently (no id-recycling).
            barrier.wait(timeout=10.0)
            r = loop.run_until_complete(_call_and_hold())
            with results_lock:
                results.append(r)
        finally:
            loop.close()

    threads = [threading.Thread(target=thread_target) for _ in range(n)]
    for t in threads:
        t.start()
    barrier.wait(timeout=10.0)
    # Give every thread a moment inside its loop with the cache call done.
    import time as _time

    _time.sleep(0.2)
    release.set()
    for t in threads:
        t.join(timeout=10.0)

    assert len(results) == n
    # N distinct loops kept alive concurrently → N distinct keys
    # → N distinct constructions. (Lock prevents two threads
    # under the SAME loop_id from racing the dict insert.)
    assert construct_count[0] == n, f"expected {n} constructions, got {construct_count[0]}"
    assert len({id(r) for r in results}) == n


def test_concurrent_same_loop_inserts_serialised_to_one_construct() -> None:
    """Pin: many concurrent callers on the SAME loop must collapse
    to a single ClusterClient construction. Without thread sync on
    the dict, the read-check-construct-insert can race and orphan
    a client. Drive concurrency via run_coroutine_threadsafe against
    a single shared loop."""
    construct_count = [0]
    construct_lock = threading.Lock()

    def fake_cluster_client(*_a: object, **_kw: object) -> MagicMock:
        with construct_lock:
            construct_count[0] += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()
    try:
        results: list[object] = []
        with patch(
            "dqlitedbapi.connection.ClusterClient",
            side_effect=fake_cluster_client,
        ):

            async def _call() -> object:
                return _get_resolve_leader_cluster(**_make_cluster_kwargs())

            futures = [asyncio.run_coroutine_threadsafe(_call(), loop) for _ in range(16)]
            for f in futures:
                results.append(f.result(timeout=5.0))

        # All 16 returns are the same instance (single-flight per loop).
        assert len({id(r) for r in results}) == 1
        # Exactly one construction.
        assert construct_count[0] == 1
    finally:
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=2.0)
        loop.close()


@pytest.mark.asyncio
async def test_fork_pid_change_invalidates_cache() -> None:
    """Regression pin: the existing fork-pid invalidation still
    works under the new loop-keyed and thread-locked code path."""
    construct_count = [0]

    def fake_cluster_client(*_a: object, **_kw: object) -> MagicMock:
        construct_count[0] += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", side_effect=fake_cluster_client):
        _get_resolve_leader_cluster(**_make_cluster_kwargs())
        with patch.object(_client_conn_mod, "_current_pid", os.getpid() + 1):
            _get_resolve_leader_cluster(**_make_cluster_kwargs())

    # Two constructions: pre-fork and post-fork.
    assert construct_count[0] == 2
