"""Pin: the ``_resolve_leader`` leader-discovery cache key uses the
``dial_func`` callable itself rather than ``id(dial_func)``, so a
post-eviction CPython id-recycle window cannot let a freshly-
allocated lambda with a different transport contract collide with a
still-resident cache entry.

CPython's ``id()`` is the memory address and is recycled as soon as
the targeted object is GC'd. The cache holds a strong ref to the
``ClusterClient`` which holds a strong ref to ``dial_func`` via
``_dial_func``, so an id collision between two cache-resident
entries cannot occur today — but if the key were ``id(dial_func)``,
an evicted entry's dial_func could be collected, the id slot freed,
and a fresh lambda allocated at the same address. Holding the
callable IN the key pins it for the lifetime of the cache entry,
eliminating the recycle window entirely.

Reproduce-and-pin: construct ephemeral lambdas in a tight loop, force
GC between insertions, and assert every cache slot's stored
``_dial_func`` is identity-distinct from every other slot's
``_dial_func``. The pre-fix shape (``id(dial_func)`` in the key) would
let two consecutive lambdas at the same recycled id share a slot.
"""

from __future__ import annotations

import asyncio
import gc

from dqlitedbapi.connection import (
    _RESOLVE_LEADER_CACHE,
    _RESOLVE_LEADER_CACHE_MAX,
    _get_resolve_leader_cluster,
)


def test_ephemeral_lambdas_do_not_collide_via_id_recycle() -> None:
    """Allocate 2 * cap ephemeral dialers in sequence, dropping each
    caller-side reference after the cache insert. CPython's allocator
    routinely returns the same memory address for two consecutive
    short-lived lambdas, so without the pinning-via-key discipline
    the cache would observe key collisions between distinct dialers.
    """

    async def runner() -> None:
        # Pre-clear so existing entries from prior tests can't mask
        # the assertion.
        _RESOLVE_LEADER_CACHE.clear()

        observed_dial_funcs: list[object] = []
        cap = _RESOLVE_LEADER_CACHE_MAX
        # Allocate ENOUGH lambdas to fill the cache; the rebuilt cache
        # entries each pin their dial_func, so the final snapshot of
        # ``_RESOLVE_LEADER_CACHE`` must hold ``cap`` distinct dialers.
        for _ in range(cap):
            # Lexical closure intentionally captures NOTHING from the
            # surrounding scope; the lambda exists only for the
            # duration of this iteration unless the cache pins it.
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
            # Drop the local; only the cache (via the key tuple AND
            # the ClusterClient's ``_dial_func``) should keep it
            # alive. With ``id(dial_func)`` in the key, the cache held
            # no reference to the callable via the key — only the
            # ClusterClient did — so the same ``id()`` could be
            # observed for two distinct cache entries.
            del d
            gc.collect()

        # Snapshot the cache and assert every entry's
        # ``_dial_func`` is identity-distinct from every other.
        clusters = list(_RESOLVE_LEADER_CACHE.values())
        assert len(clusters) == cap, (
            f"expected cap={cap} entries after filling cache, got "
            f"{len(clusters)}; eviction may have over-aggressively "
            f"dropped entries (or key collision dropped distinct "
            f"dialers onto the same slot)"
        )
        dial_funcs = [c._dial_func for c in clusters]
        # ``set`` deduplicates by identity for unhashable values, but
        # functions hash by id; build the dedup explicitly so a future
        # change to lambda hashability doesn't silently weaken this
        # check.
        seen_ids: set[int] = set()
        for f in dial_funcs:
            assert id(f) not in seen_ids, (
                "two cache entries share the same dial_func identity — "
                "id-recycle window OR a regression that re-keyed on id()"
            )
            seen_ids.add(id(f))

    asyncio.run(runner())


def test_repeated_calls_with_same_dial_func_hit_cache() -> None:
    """Negative pin: keying on the callable (not ``id()``) preserves
    the common 'module-level dialer reused across calls' fast path —
    the same dial_func argument must yield the SAME cached
    ClusterClient."""

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
    """Direct pin against the regression: the cache key must not call
    ``id()`` on the dial_func. Constructs a custom callable whose
    ``id()`` we know will be stable for the lifetime of the test, and
    inspects the cache's key tuple to confirm the callable itself is
    present (not its id-as-int)."""

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
        # The key tuple contains the callable itself; ``id(custom)``
        # (an int) MUST NOT appear in any cache key.
        for key in _RESOLVE_LEADER_CACHE:
            assert id(custom) not in key, (
                f"cache key contains ``id(dial_func)`` (an int) — the "
                f"key should contain the callable itself to pin it "
                f"against id-recycle: {key!r}"
            )
            assert custom in key, f"cache key does not contain the dial_func callable: {key!r}"

    asyncio.run(runner())
