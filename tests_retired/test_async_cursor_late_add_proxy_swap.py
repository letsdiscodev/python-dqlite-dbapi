"""cursor()'s defensive late-add scrub swaps ``cur._connection`` to a weakref.proxy.

Without it, a strong ref to a race-leaked cursor pins the closed connection's
loop-bound state past the user's intended lifetime.
"""

from __future__ import annotations

import weakref
from typing import Any

from dqlitedbapi.aio.connection import AsyncConnection


def _make_aconn() -> AsyncConnection:
    return AsyncConnection("127.0.0.1:9999")


def test_late_add_defensive_arm_swaps_connection_to_weakref_proxy() -> None:
    aconn = _make_aconn()
    real_add = aconn._cursors.add

    def add_then_flip_closed(cur: Any) -> None:
        real_add(cur)
        aconn._closed = True

    aconn._cursors.add = add_then_flip_closed  # type: ignore[assignment]

    cur = aconn.cursor()

    assert type(cur._connection) is weakref.ProxyType, (
        "defensive late-add arm must apply the cascade's weakref.proxy "
        "swap so the cursor does not pin the closed connection"
    )
