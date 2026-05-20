"""Pin: ``AsyncConnection.force_close_transport`` honours its
"force" contract — never raises — even when its defensive
``contextlib.suppress(...)`` arms fire.

Three defensive arms close the gap between the happy-path tests in
``test_force_close_transport_disarms_inner_finalizer.py`` /
``test_aio_force_close_transport_cursor_cascade.py`` and the
"force" contract:

1. ``contextlib.suppress(Exception)`` around
   ``inner_finalizer.detach()`` — guards against a finalizer in a
   partially-constructed state.
2. ``contextlib.suppress(TypeError)`` around
   ``cur._connection = weakref.proxy(cur._connection)`` — guards
   against cursors whose ``_connection`` is non-referenceable
   (already a proxy, certain slot classes).
3. ``contextlib.suppress(AttributeError)`` around
   ``del cur.messages[:]`` — guards against cursors built via
   ``__new__`` that lack the ``messages`` attribute.

The "force" in ``force_close_transport`` is the contract — it must
not raise because callers reach it from non-cooperative cleanup
contexts (SA ``do_terminate``, atexit handlers, signal handlers,
GC sweeps). A regression dropping any of the three suppresses
would turn the force-close into a not-actually-forced path on the
exact edge cases the suppress was added to handle.
"""

from __future__ import annotations

import os
import weakref
from unittest.mock import MagicMock

from dqlitedbapi.aio.connection import AsyncConnection


def _build_conn_skeleton(inner: object | None) -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._creator_pid = os.getpid()
    conn._async_conn = inner  # type: ignore[assignment]
    conn._finalizer = None
    return conn


def test_force_close_transport_tolerates_inner_finalizer_detach_failure() -> None:
    """A finalizer whose ``detach()`` raises must be swallowed —
    force close must complete cleanly and null the finalizer
    reference."""
    inner = MagicMock()
    inner._closed_flag = [False]
    inner._connected_flag = [True]
    inner._protocol = None  # no writer to reap
    bad_finalizer = MagicMock()
    bad_finalizer.detach = MagicMock(side_effect=RuntimeError("synthetic"))
    inner._finalizer = bad_finalizer

    conn = _build_conn_skeleton(inner)

    # Must not raise; the suppress(Exception) arm absorbs the
    # detach() RuntimeError.
    conn.force_close_transport()
    bad_finalizer.detach.assert_called_once()
    assert inner._finalizer is None, (
        "the inner finalizer reference must be nulled even when "
        "detach() raises so a subsequent GC sweep does not see "
        "a stale entry"
    )


def test_force_close_transport_cursor_cascade_tolerates_missing_messages() -> None:
    """A cursor without a ``messages`` attribute (e.g. an
    ``__new__``-built test fixture, or a third-party
    SA-async harness that bypasses ``__init__``) must trip the
    ``suppress(AttributeError)`` arm without breaking the cascade."""

    # Build a bare cursor: no ``messages`` attribute, but the other
    # attributes the cascade writes are settable.
    class _BareCur:
        pass

    cur = _BareCur()
    cur._closed = False  # type: ignore[attr-defined]
    cur._connection = MagicMock()  # type: ignore[attr-defined]

    conn = _build_conn_skeleton(inner=None)
    conn._cursors.add(cur)  # type: ignore[arg-type]

    # Must not raise; the suppress(AttributeError) absorbs the
    # bare-cursor's missing-messages state.
    conn.force_close_transport()
    assert cur._closed is True  # type: ignore[attr-defined]
    # The strong inner reference may or may not have been swapped
    # to a proxy depending on the cur._connection type; what matters
    # is the cascade completed.


def test_force_close_transport_cursor_cascade_tolerates_unreferenceable_connection() -> None:
    """A cursor whose ``_connection`` is non-referenceable (e.g. an
    object on a ``__slots__`` class with no ``__weakref__`` slot)
    must trip the ``suppress(TypeError)`` arm without breaking the
    cascade."""

    class _NoWeakref:
        __slots__ = ()  # forbids weakref.proxy(...) — raises TypeError

    cur = MagicMock()
    cur._closed = False
    cur._connection = _NoWeakref()
    cur.messages = []

    conn = _build_conn_skeleton(inner=None)
    conn._cursors.add(cur)

    # Must not raise; the suppress(TypeError) absorbs the
    # weakref.proxy rejection.
    conn.force_close_transport()
    assert cur._closed is True
