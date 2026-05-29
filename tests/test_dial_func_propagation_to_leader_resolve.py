"""``dial_func`` must reach BOTH connect legs: leader discovery
(``_resolve_leader`` -> ``ClusterClient``) AND the post-resolve ``DqliteConnection``.

Security: if the discovery leg uses the default dialer, a TLS-required deployment's
first round-trip can run silently unencrypted against a plaintext-tolerant proxy.
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
    params = inspect.signature(_resolve_leader).parameters
    assert "dial_func" in params, list(params)


def test_get_resolve_leader_cluster_signature_accepts_dial_func() -> None:
    params = inspect.signature(_get_resolve_leader_cluster).parameters
    assert "dial_func" in params, list(params)


def test_resolve_leader_cluster_forwards_dial_func_to_cluster_client() -> None:
    """The constructed ``ClusterClient`` retains the dial_func."""

    async def runner() -> None:
        cluster = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=_custom_dial_func,
        )
        assert cluster._dial_func is _custom_dial_func

    asyncio.run(runner())


def test_resolve_leader_cluster_cache_key_distinguishes_distinct_dial_funcs() -> None:
    """Different dial_func callables must get different cached instances; sharing would
    let one dialer's transport contract (TLS) serve the other's request (plaintext)."""

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
    """Same dial_func identity -> same cached instance (module-level dialer stays cached)."""

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
    """The dialer participates in leader discovery; a marker OSError proves it was reached."""
    dial_calls: list[str] = []

    async def recording_dialer(
        address: str, *, timeout: float | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        dial_calls.append(address)
        raise OSError("recorded by custom dialer; refuse")

    # Exception class depends on the surrounding wrap; only dial_calls matters here.
    with pytest.raises(BaseException) as exc_info:  # noqa: B017, PT011
        await _resolve_leader(
            "127.0.0.1:9999",
            timeout=1.0,
            dial_func=recording_dialer,
        )
    assert exc_info.value is not None
    assert dial_calls, "dialer was never invoked during leader discovery"
    assert dial_calls[0] == "127.0.0.1:9999"
