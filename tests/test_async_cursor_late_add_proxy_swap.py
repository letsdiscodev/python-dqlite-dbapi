"""Pin: ``AsyncConnection.cursor()``'s defensive late-add scrub at
``aio/connection.py:1513-1521`` applies the ``weakref.proxy`` swap to
``cur._connection``, matching the cascade pattern at L575-587 and the
per-cursor close arm at ``aio/cursor.py:889-892``.

Without the proxy swap, a user that holds a strong reference to a
race-leaked cursor pins the closed ``AsyncConnection``'s loop-bound
state (lazy ``asyncio.Lock`` / ``weakref.finalize`` / inner client
conn) past the user's intended lifetime — silently violating the
``AsyncCursor.connection`` docstring contract that promises the
back-reference is dropped at close.

The cascade at L575-587 explicitly applies the swap; the defensive
late-add arm must match. Companion to
``test_async_cursor_add_races_cascade_clear.py``.
"""

from __future__ import annotations

import weakref
from typing import Any

from dqlitedbapi.aio.connection import AsyncConnection


def _make_aconn() -> AsyncConnection:
    return AsyncConnection("127.0.0.1:9999")


def test_late_add_defensive_arm_swaps_connection_to_weakref_proxy() -> None:
    """The defensive late-add scrub must swap ``cur._connection`` for a
    ``weakref.proxy`` so a race-leaked cursor does not strong-pin the
    closed connection."""
    aconn = _make_aconn()
    real_add = aconn._cursors.add

    def add_then_flip_closed(cur: Any) -> None:
        real_add(cur)
        aconn._closed = True

    aconn._cursors.add = add_then_flip_closed  # type: ignore[assignment]

    cur = aconn.cursor()

    # The back-reference is now a weakref.proxy — the canonical
    # closed-cursor postcondition. Without the swap, a long-lived
    # adapter holding ``cur`` pins the closed AsyncConnection and
    # its loop-bound state.
    assert type(cur._connection) is weakref.ProxyType, (
        "defensive late-add arm must apply the cascade's weakref.proxy "
        "swap so the cursor does not pin the closed connection"
    )
