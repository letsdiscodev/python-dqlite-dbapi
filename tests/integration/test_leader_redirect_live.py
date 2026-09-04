"""Live integration: leader-redirect-on-connect against a real cluster.

Topology-mutating tests restore the original leader on exit so later tests
in the session see a stable starting state.
"""

from __future__ import annotations

import contextlib
import os
from typing import TYPE_CHECKING

import pytest

from dqlitedbapi import connect
from dqlitedbapi.exceptions import OperationalError

if TYPE_CHECKING:
    from dqlitetestlib import TestClusterControl  # type: ignore[import-not-found]


def _node_addresses() -> list[str]:
    raw = os.environ.get(
        "DQLITE_TEST_CLUSTER_NODES",
        "localhost:9001,localhost:9002,localhost:9003",
    )
    return [s.strip() for s in raw.split(",") if s.strip()]


@pytest.mark.integration
def test_connect_with_seed_as_leader_succeeds() -> None:
    seed = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
    conn = connect(seed, timeout=5.0)
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]
    finally:
        conn.close()


@pytest.mark.integration
def test_connect_via_follower_address_redirects_to_leader() -> None:
    """Connecting through a follower must redirect; without redirect it would fail
    with ``SQLITE_IOERR_NOT_LEADER`` from the follower's OPEN handler."""
    import asyncio

    from dqliteclient.cluster import ClusterClient
    from dqliteclient.node_store import MemoryNodeStore

    addresses = _node_addresses()

    async def _resolve() -> str:
        store = MemoryNodeStore(addresses)
        cluster = ClusterClient(store, timeout=5.0)
        return await cluster.find_leader()

    leader_addr = asyncio.run(_resolve())
    follower_addrs = [a for a in addresses if a != leader_addr]
    assert follower_addrs, (
        f"expected at least one follower in {addresses!r}; leader is {leader_addr!r}"
    )

    for follower in follower_addrs:
        conn = connect(follower, timeout=5.0)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            assert cur.fetchall() == [(1,)]
        finally:
            conn.close()


@pytest.mark.integration
def test_connect_after_leader_flip_routes_to_new_leader(
    cluster_control: TestClusterControl,
) -> None:
    """After a leader flip, connect() against the old-leader address (now a follower)
    succeeds via redirect. Restores the original leader on the way out."""
    import asyncio

    starting = asyncio.run(cluster_control.current_leader_node())
    seed = starting.address.replace("127.0.0.1", "localhost")

    flip = asyncio.run(cluster_control.force_leader_flip())
    assert flip.target.node_id != starting.node_id

    try:
        # The seed is now a follower (the demoted ex-leader).
        conn = connect(seed, timeout=5.0)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            assert cur.fetchall() == [(1,)]
        finally:
            conn.close()
    finally:
        with contextlib.suppress(Exception):
            asyncio.run(cluster_control.transfer_leadership_to(starting.node_id))


@pytest.mark.integration
def test_connect_to_unreachable_seed_raises_operational_error() -> None:
    # Port 1: nothing is listening, so leader resolution fails.
    with pytest.raises(OperationalError, match="Failed to connect"):
        conn = connect("127.0.0.1:1", timeout=1.0)
        conn.connect()  # explicit connect for clearer failure point
