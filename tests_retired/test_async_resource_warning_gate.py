"""Pin: the three-flag gate on ``AsyncConnection``'s GC-time
``ResourceWarning`` — never-connected does NOT warn, force_close_transport
sets closed_flag to silence SA's terminate() path, and connected-but-not-closed
DOES warn (matches stdlib ``sqlite3.Connection.__del__``)."""

from __future__ import annotations

import gc
import os
import warnings
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import _async_unclosed_warning


def test_never_connected_does_not_warn_on_gc() -> None:
    """``connected_flag[0] is False`` short-circuits: nothing to clean up."""
    closed_flag = [False]
    connected_flag = [False]

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "localhost:9999", os.getpid())

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert not rw, f"never-connected ResourceWarning leaked: {[str(w.message) for w in rw]}"


def test_closed_flag_short_circuits_warning() -> None:
    """``closed_flag[0] is True`` short-circuits: explicitly cleaned up."""
    closed_flag = [True]
    connected_flag = [True]

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "localhost:9999", os.getpid())

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert not rw


def test_connected_unclosed_warns() -> None:
    """Connected, not closed: emit. Mirrors stdlib ``sqlite3.Connection.__del__``."""
    closed_flag = [False]
    connected_flag = [True]

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "localhost:9999", os.getpid())

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert len(rw) == 1
    assert "await close()" in str(rw[0].message)
    assert "localhost:9999" in str(rw[0].message)


def test_force_close_transport_sets_closed_flag() -> None:
    """``force_close_transport`` sets ``_closed_flag[0] = True`` before any
    short-circuit, so a later GC sweep sees it even on inner-None / fork-child."""
    from dqlitedbapi.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost:9999", database="x")
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
    """Even on the early-return ``inner is None`` branch, the flag must be set."""
    from dqlitedbapi.aio.connection import AsyncConnection

    conn = AsyncConnection("localhost:9999", database="x")
    conn.force_close_transport()
    assert conn._closed_flag[0] is True


def test_resource_warning_silenced_by_force_close_through_module_scope() -> None:
    """End-to-end: a connected instance whose ``force_close_transport`` ran
    emits no dqlite-layer ``ResourceWarning`` on GC. Filtered to dqlite
    warnings since asyncio's "unclosed transport" can leak from sibling tests."""
    from dqlitedbapi.aio.connection import AsyncConnection

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        conn = AsyncConnection("localhost:9999", database="x")
        conn._connected_flag[0] = True
        conn.force_close_transport()
        del conn
        gc.collect()

    rw = [
        w
        for w in captured
        if issubclass(w.category, ResourceWarning) and "AsyncConnection" in str(w.message)
    ]
    assert not rw, f"force_close should silence warning; got: {[str(w.message) for w in rw]}"
