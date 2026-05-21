"""Pin: ``dial_func`` reaches BOTH legs of the connect sequence —
leader discovery (``_resolve_leader`` → ``ClusterClient``) AND the
post-resolve ``DqliteConnection``.

Companion to ``test_dial_func_propagation.py`` which only verified
that the kwarg reaches the post-resolve ``DqliteConnection.__init__``.
The leader-discovery leg was missed in the original round-six
propagation (``cb54370``): ``_resolve_leader`` and
``_get_resolve_leader_cluster`` did not accept the kwarg, so the
FIRST round-trip ran with the default dialer regardless of the
operator's request.

A TLS-required deployment hitting the seed plaintext is a real
security regression: either the plaintext probe fails the TLS-only
listener with an unhelpful "connection reset" diagnostic, or
(worse) it succeeds against a TLS-terminating proxy that tolerates
plaintext, making the first round-trip silently unencrypted.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from dqlitedbapi.connection import (
    _get_resolve_leader_cluster,
    _resolve_leader,
)


async def _custom_dial_func(
    address: str, *, timeout: float | None = None
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    raise RuntimeError("never called in unit test")


def test_resolve_leader_signature_accepts_dial_func() -> None:
    """``_resolve_leader`` must accept ``dial_func`` so the seam
    that builds the leader-probe ``ClusterClient`` can forward it."""
    params = inspect.signature(_resolve_leader).parameters
    assert "dial_func" in params, list(params)


def test_get_resolve_leader_cluster_signature_accepts_dial_func() -> None:
    """``_get_resolve_leader_cluster`` must accept ``dial_func`` so
    the cache builder can construct a per-dialer ``ClusterClient``."""
    params = inspect.signature(_get_resolve_leader_cluster).parameters
    assert "dial_func" in params, list(params)


def test_resolve_leader_cluster_forwards_dial_func_to_cluster_client() -> None:
    """The constructed ``ClusterClient`` retains the dial_func — the
    seam that was previously dropping it."""

    async def runner() -> None:
        cluster = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=_custom_dial_func,
        )
        # ``ClusterClient`` stores dial_func as ``_dial_func``;
        # confirm the forward landed.
        assert cluster._dial_func is _custom_dial_func

    asyncio.run(runner())


def test_resolve_leader_cluster_cache_key_distinguishes_distinct_dial_funcs() -> None:
    """Two callers passing DIFFERENT dial_func callables must get
    DIFFERENT cached ``ClusterClient`` instances. Sharing would let
    one dialer's transport contract (TLS) serve the other's request
    (plaintext)."""

    async def dialer_a(
        address: str, *, timeout: float | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        raise RuntimeError("a")

    async def dialer_b(
        address: str, *, timeout: float | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        raise RuntimeError("b")

    async def runner() -> None:
        cluster_a = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=dialer_a,
        )
        cluster_b = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=dialer_b,
        )
        cluster_none = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
        )
        assert cluster_a is not cluster_b
        assert cluster_a is not cluster_none
        assert cluster_b is not cluster_none
        assert cluster_a._dial_func is dialer_a
        assert cluster_b._dial_func is dialer_b
        assert cluster_none._dial_func is None

    asyncio.run(runner())


def test_resolve_leader_cluster_cache_hits_same_dial_func_identity() -> None:
    """Same dial_func identity → same cached instance (the common
    'module-level dialer' pattern stays cache-friendly)."""

    async def shared(
        address: str, *, timeout: float | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        raise RuntimeError("shared")

    async def runner() -> None:
        cluster1 = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=shared,
        )
        cluster2 = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=shared,
        )
        assert cluster1 is cluster2

    asyncio.run(runner())


@pytest.mark.asyncio
async def test_resolve_leader_probe_invokes_custom_dial_func() -> None:
    """End-to-end check that the dialer participates in the
    leader-discovery probe, not just the post-resolve data session.
    We do not need a live server — the dialer raising a marker
    ``OSError`` proves the dialer was reached."""
    dial_calls: list[str] = []

    async def recording_dialer(
        address: str, *, timeout: float | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        dial_calls.append(address)
        raise OSError("recorded by custom dialer; refuse")

    # ClusterError / OperationalError / OSError shape depends on
    # the surrounding wrap; the assertion that matters is the
    # dial_calls record, not the exception class. Use BaseException
    # base so any failure-class propagates here.
    with pytest.raises(BaseException) as exc_info:  # noqa: B017, PT011
        await _resolve_leader(
            "127.0.0.1:9999",
            timeout=1.0,
            dial_func=recording_dialer,
        )
    assert exc_info.value is not None
    assert dial_calls, "dialer was never invoked during leader discovery"
    assert dial_calls[0] == "127.0.0.1:9999"
