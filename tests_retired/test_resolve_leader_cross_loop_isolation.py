"""``_RESOLVE_LEADER_CACHE`` isolates `ClusterClient` instances by event loop
(a foreign-loop task raises a non-`dbapi.Error` `RuntimeError`) and serialises
concurrent construction across threads."""

import asyncio
import os
import threading
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dqlitedbapi import connection as _conn_mod
from dqlitedbapi.connection import _get_resolve_leader_cluster


@pytest.fixture(autouse=True)
def _clear_cache() -> Iterator[None]:
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
    """Async-only: calling from sync context raises bare ``RuntimeError`` (a
    programmer-invariant violation, not a misclassified ``InterfaceError``)."""
    with pytest.raises(RuntimeError, match="running event loop"):
        _get_resolve_leader_cluster(**_make_cluster_kwargs())


def test_two_event_loops_get_distinct_cluster_clients() -> None:
    """Same args, two different loops -> two distinct cached `ClusterClient`s."""
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
    assert results[0] is not results[1]


async def test_same_loop_returns_same_cluster_client() -> None:
    """Single loop, two calls, same args -> same cluster (single-flight)."""
    with patch("dqlitedbapi.connection.ClusterClient") as MockCluster:
        MockCluster.side_effect = lambda *_a, **_kw: MagicMock()
        c1 = _get_resolve_leader_cluster(**_make_cluster_kwargs())
        c2 = _get_resolve_leader_cluster(**_make_cluster_kwargs())
    assert c1 is c2


def test_concurrent_first_inserts_yield_one_cluster_per_loop() -> None:
    """N threads on their own loops, same args -> N distinct clusters. Loops are
    held alive via a barrier so CPython does not recycle their ``id()``."""
    n = 8
    construct_count = [0]
    construct_lock = threading.Lock()
    results: list[object] = []
    results_lock = threading.Lock()
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
    import time as _time

    _time.sleep(0.2)
    release.set()
    for t in threads:
        t.join(timeout=10.0)

    assert len(results) == n
    assert construct_count[0] == n, f"expected {n} constructions, got {construct_count[0]}"
    assert len({id(r) for r in results}) == n


def test_concurrent_same_loop_inserts_serialised_to_one_construct() -> None:
    """Many concurrent callers on the same loop collapse to one construction."""
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

        assert len({id(r) for r in results}) == 1
        assert construct_count[0] == 1
    finally:
        loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(timeout=2.0)
        loop.close()


async def test_fork_pid_change_invalidates_cache() -> None:
    """Fork-pid invalidation still works under the loop-keyed, locked path."""
    construct_count = [0]

    def fake_cluster_client(*_a: object, **_kw: object) -> MagicMock:
        construct_count[0] += 1
        client = MagicMock()
        client.find_leader = AsyncMock(return_value="leader:9999")
        return client

    with patch("dqlitedbapi.connection.ClusterClient", side_effect=fake_cluster_client):
        _get_resolve_leader_cluster(**_make_cluster_kwargs())
        with patch("dqliteclient.connection.os.getpid", return_value=os.getpid() + 1):
            _get_resolve_leader_cluster(**_make_cluster_kwargs())

    assert construct_count[0] == 2
