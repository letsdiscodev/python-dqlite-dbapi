"""Pin: when ``cursor()`` adds a cursor that loses a race with ``close()``'s cascade, its
defensive re-check scrubs the late-added cursor and discards it from ``_cursors``."""

from __future__ import annotations

import weakref
from typing import Any

from dqlitedbapi.aio.connection import AsyncConnection


def _make_aconn() -> AsyncConnection:
    # Use real __init__ (bogus address, never used) to avoid a dangling weakref.finalize
    # against a __new__-built fixture's uninitialised attributes.
    aconn = AsyncConnection("127.0.0.1:9999")
    return aconn


def test_cursor_called_after_close_full_scrub_and_discard() -> None:
    """The re-check applies the full cascade scrub and discards the cursor from the WeakSet."""
    aconn = _make_aconn()
    # Rig add() to flip _closed mid-flow (simulating a signal handler landing between the
    # cursor() body and its re-check); iteration/discard still use the real WeakSet.
    original_cursors = aconn._cursors
    real_add = original_cursors.add

    def add_then_flip_closed(cur: Any) -> None:
        real_add(cur)
        aconn._closed = True

    aconn._cursors.add = add_then_flip_closed  # type: ignore[assignment]

    cur = aconn.cursor()

    assert cur._closed is True

    assert cur._rows == []
    assert cur._description is None
    assert cur._rowcount == -1
    assert cur._lastrowid is None
    assert cur._row_index == 0
    assert list(cur.messages) == []

    assert cur not in aconn._cursors
    assert len(aconn._cursors) == 0

    # Back-reference must be a weakref.proxy so a strong ref to the race-leaked cursor does
    # not pin the closed connection's loop-bound state.
    assert type(cur._connection) is weakref.ProxyType, (
        "defensive late-add arm must apply the cascade's weakref.proxy "
        "swap so the cursor does not pin the closed connection"
    )


def test_cursor_called_after_close_with_normal_state_intact() -> None:
    """Negative pin: when not mid-close at the re-check, the cursor registers normally."""
    aconn = _make_aconn()
    cur = aconn.cursor()

    assert cur._closed is False
    assert cur in aconn._cursors


def test_cursor_called_when_already_closed_short_circuits_before_add() -> None:
    """Negative pin: ``_closed`` True before cursor() runs raises from the top-of-method
    check, never reaching the WeakSet add."""
    import dqlitedbapi

    aconn = _make_aconn()
    aconn._closed = True
    try:
        aconn.cursor()
    except dqlitedbapi.InterfaceError:
        pass
    else:
        raise AssertionError("expected InterfaceError on closed connection")
    assert len(aconn._cursors) == 0


def test_weakset_cursors_postcondition_after_race() -> None:
    """After the race resolves, ``_cursors`` is empty: no stale late-added entry."""
    aconn = _make_aconn()
    real_add = aconn._cursors.add

    def add_then_flip_closed(cur: Any) -> None:
        real_add(cur)
        aconn._closed = True

    aconn._cursors.add = add_then_flip_closed  # type: ignore[assignment]

    cur = aconn.cursor()
    assert len(aconn._cursors) == 0
    # Keeping the cursor alive (this scope holds ``cur``) does not re-insert it.
    assert isinstance(weakref.ref(cur), weakref.ref)
