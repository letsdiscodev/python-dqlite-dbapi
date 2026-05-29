"""Pin: the ``_resolve_leader`` cache key holds the ``dial_func`` callable itself, not
``id(dial_func)``. Keying on ``id()`` would let an evicted dialer be GC'd and a fresh
one allocated at the recycled address collide with a resident entry."""

from __future__ import annotations

import asyncio
import gc

from dqlitedbapi.connection import (
    _RESOLVE_LEADER_CACHE,
    _RESOLVE_LEADER_CACHE_MAX,
    _get_resolve_leader_cluster,
)


def test_ephemeral_lambdas_do_not_collide_via_id_recycle() -> None:
    """Allocate ephemeral dialers, dropping each caller-side ref after insert; CPython
    reuses the same address for consecutive short-lived lambdas, so without key-pinning
    the cache would see collisions between distinct dialers."""

    async def runner() -> None:
        _RESOLVE_LEADER_CACHE.clear()

        observed_dial_funcs: list[object] = []
        cap = _RESOLVE_LEADER_CACHE_MAX
        for _ in range(cap):

            def make_dialer() -> object:
                async def dialer(
                    address: str, *, timeout: float | None = None
                ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
                    raise RuntimeError("ephemeral dialer")

                return dialer

            d = make_dialer()
            observed_dial_funcs.append(d)
            cluster = _get_resolve_leader_cluster(
                address="127.0.0.1:9999",
                timeout=10.0,
                max_total_rows=None,
                max_continuation_frames=None,
                trust_server_heartbeat=False,
                dial_func=d,  # type: ignore[arg-type]
            )
            assert cluster._dial_func is d
            # Drop the local; only the cache (key tuple + ClusterClient) keeps it alive.
            del d
            gc.collect()

        clusters = list(_RESOLVE_LEADER_CACHE.values())
        assert len(clusters) == cap, (
            f"expected cap={cap} entries after filling cache, got "
            f"{len(clusters)}; eviction may have over-aggressively "
            f"dropped entries (or key collision dropped distinct "
            f"dialers onto the same slot)"
        )
        dial_funcs = [c._dial_func for c in clusters]
        # Dedup by id() explicitly so a future change to lambda hashability can't weaken this.
        seen_ids: set[int] = set()
        for f in dial_funcs:
            assert id(f) not in seen_ids, (
                "two cache entries share the same dial_func identity — "
                "id-recycle window OR a regression that re-keyed on id()"
            )
            seen_ids.add(id(f))

    asyncio.run(runner())


def test_repeated_calls_with_same_dial_func_hit_cache() -> None:
    """The same dial_func argument yields the SAME cached ClusterClient (reuse fast path)."""

    async def shared(
        address: str, *, timeout: float | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        raise RuntimeError("shared")

    async def runner() -> None:
        _RESOLVE_LEADER_CACHE.clear()
        c1 = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=shared,
        )
        c2 = _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=shared,
        )
        assert c1 is c2

    asyncio.run(runner())
    asyncio.run(runner())


def test_cache_key_does_not_use_id_of_dial_func() -> None:
    """The cache key contains the callable itself, never ``id(dial_func)``."""

    async def custom(
        address: str, *, timeout: float | None = None
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        raise RuntimeError("custom")

    async def runner() -> None:
        _RESOLVE_LEADER_CACHE.clear()
        _get_resolve_leader_cluster(
            address="127.0.0.1:9999",
            timeout=10.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            dial_func=custom,
        )
        for key in _RESOLVE_LEADER_CACHE:
            assert id(custom) not in key, (
                f"cache key contains ``id(dial_func)`` (an int) — the "
                f"key should contain the callable itself to pin it "
                f"against id-recycle: {key!r}"
            )
            assert custom in key, f"cache key does not contain the dial_func callable: {key!r}"

    asyncio.run(runner())
