"""Live integration: leader-redirect-on-connect against a real cluster.

End-to-end coverage for the ``_resolve_leader`` step added to
``_build_and_connect``. Mocked unit coverage is in
``tests/test_leader_redirect_on_connect.py``; this file pins the
behaviour against the live cluster so a regression in the wire
layer or in ``ClusterClient.find_leader`` would surface here even
if the unit tests stay green.

Tests use ``cluster_control`` from ``dqlitetestlib`` (bootstrapped
in the top-level ``tests/conftest.py``) where they need
deterministic leader manipulation. Each test that mutates cluster
topology restores the original leader on its way out so subsequent
tests in the session see a stable starting state.
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
    """Read DQLITE_TEST_CLUSTER_NODES with the python-dqlite-dev
    default."""
    raw = os.environ.get(
        "DQLITE_TEST_CLUSTER_NODES",
        "localhost:9001,localhost:9002,localhost:9003",
    )
    return [s.strip() for s in raw.split(",") if s.strip()]


# --- normal flows ---


@pytest.mark.integration
def test_connect_with_seed_as_leader_succeeds() -> None:
    """Happy path: the seed address is the current leader.
    ``_resolve_leader`` returns the seed verbatim and the OPEN
    runs against it."""
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
    """The seed is a follower; ``_resolve_leader`` follows the
    redirect and OPEN runs against the actual leader. Without
    leader-redirect-on-connect this would fail with
    ``SQLITE_IOERR_NOT_LEADER`` from the follower's OPEN handler.

    Picks the non-leader nodes from ``DQLITE_TEST_CLUSTER_NODES``
    and connects through each — at least two of the three nodes
    are followers in the steady state, so this is a robust pin.
    """
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
    """Force a leader flip; a brand-new ``connect()`` against the
    OLD-leader address (now a follower) succeeds because
    ``_resolve_leader`` follows the redirect to the new leader.
    Restores the original leader on the way out."""
    import asyncio

    starting = asyncio.run(cluster_control.current_leader_node())
    seed = starting.address.replace("127.0.0.1", "localhost")

    flip = asyncio.run(cluster_control.force_leader_flip())
    assert flip.target.node_id != starting.node_id

    try:
        # The seed is now a follower (the demoted ex-leader).
        # ``_resolve_leader`` should follow the redirect and the
        # OPEN should reach the new leader.
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


# --- failure flows ---


@pytest.mark.integration
def test_connect_to_unreachable_seed_raises_operational_error() -> None:
    """Seed unreachable: ``_resolve_leader`` cannot reach any node
    in its 1-node store; surfaces as ``OperationalError`` with the
    canonical ``Failed to find leader from`` prefix."""
    # Pick a port we know nothing is listening on.
    with pytest.raises(OperationalError, match="Failed to find leader"):
        conn = connect("127.0.0.1:1", timeout=1.0)
        conn.connect()  # explicit connect for clearer failure point
