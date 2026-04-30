"""Pin: cycle 22's three-flag gate on
``AsyncConnection``'s GC-time ``ResourceWarning``.

Cycle 22 added the ``connected_flag`` argument and the
gate ``if closed_flag[0] or not connected_flag[0]: return``
plus an early ``self._closed_flag[0] = True`` in
``force_close_transport``. The three behavioural promises:

1. A never-connected instance does NOT emit
   ``ResourceWarning`` on GC. The connected-flag gate
   prevents a misleading false positive on
   ``conn = AsyncConnection(...); del conn`` (test
   fixtures, early-error flows).
2. ``force_close_transport`` flips ``closed_flag[0] = True``
   so SA's ``terminate()`` path (which runs the sync
   force-close, not the async ``close()``) silences the
   subsequent GC warning.
3. A connected-but-not-closed instance DOES emit the
   warning — the original load-bearing behaviour
   (matches stdlib ``sqlite3.Connection.__del__``).

A regression that drops any clause from the gate, or
inverts it, or removes the ``_closed_flag`` set in
force_close_transport silently breaks one of the three
promises without failing any existing test.
"""

from __future__ import annotations

import gc
import warnings
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import _async_unclosed_warning


def test_never_connected_does_not_warn_on_gc() -> None:
    """``connected_flag[0] is False`` short-circuits the warning —
    a never-connected instance has nothing to clean up."""
    closed_flag = [False]
    connected_flag = [False]

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "localhost:9999")

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert not rw, f"never-connected ResourceWarning leaked: {[str(w.message) for w in rw]}"


def test_closed_flag_short_circuits_warning() -> None:
    """``closed_flag[0] is True`` short-circuits the warning —
    user (or terminate) explicitly cleaned up."""
    closed_flag = [True]
    connected_flag = [True]

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "localhost:9999")

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert not rw


def test_connected_unclosed_warns() -> None:
    """The load-bearing case: connected, not closed → emit the
    warning. Mirrors stdlib ``sqlite3.Connection.__del__``."""
    closed_flag = [False]
    connected_flag = [True]

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "localhost:9999")

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert len(rw) == 1
    assert "await close()" in str(rw[0].message)
    assert "localhost:9999" in str(rw[0].message)


def test_force_close_transport_sets_closed_flag() -> None:
    """``force_close_transport`` must set
    ``self._closed_flag[0] = True`` BEFORE any short-circuit
    so a subsequent GC sweep finds the closed-flag set even
    on the inner-None / fork-child branches."""
    from dqlitedbapi.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost:9999", database="x")
    # Simulate a connected state by setting the flags directly.
    conn._connected_flag[0] = True
    conn._closed_flag[0] = False
    inner = MagicMock()
    proto = MagicMock()
    writer = MagicMock()
    proto._writer = writer
    inner._protocol = proto
    conn._async_conn = inner

    conn.force_close_transport()

    assert conn._closed_flag[0] is True, (
        "force_close_transport must flip _closed_flag[0] = True "
        "so the subsequent GC ResourceWarning is silenced after "
        "SA's terminate() path runs the synchronous force-close."
    )


def test_force_close_transport_sets_closed_flag_even_with_no_inner() -> None:
    """Even on the early-return ``inner is None`` branch, the
    flag must be set so the warning gate sees it."""
    from dqlitedbapi.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost:9999", database="x")
    # Never connected — inner is None.
    conn.force_close_transport()
    assert conn._closed_flag[0] is True


def test_resource_warning_silenced_by_force_close_through_module_scope() -> None:
    """End-to-end: a connected instance whose ``force_close_transport``
    ran does NOT emit the dqlite-layer ``ResourceWarning`` when the
    finalizer fires on GC. (Filters to dqlite-emitted warnings —
    asyncio's own ``"unclosed transport"`` warnings can leak from
    sibling tests' GC trails when this test runs in a full suite.)"""
    from dqlitedbapi.aio.connection import AsyncConnection

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        conn = AsyncConnection("localhost:9999", database="x")
        conn._connected_flag[0] = True  # simulate connected
        conn.force_close_transport()
        del conn
        gc.collect()

    # Filter to dqlite-layer ResourceWarnings (the ones our finalizer
    # emits); drop asyncio-layer "unclosed transport" warnings that
    # are unrelated to this test's contract.
    rw = [
        w
        for w in captured
        if issubclass(w.category, ResourceWarning) and "AsyncConnection" in str(w.message)
    ]
    assert not rw, f"force_close should silence warning; got: {[str(w.message) for w in rw]}"
