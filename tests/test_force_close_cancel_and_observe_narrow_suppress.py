"""``force_close_transport``'s ``_cancel_and_observe`` suppresses ``Exception``, not
``BaseException``, so signal-driven KI/SystemExit propagate to drive loop shutdown.

Inspects production source directly (unlike the inlined-body coverage sibling)."""

from __future__ import annotations

import inspect


def test_force_close_transport_cancel_and_observe_uses_narrow_suppress() -> None:
    from dqlitedbapi.aio.connection import AsyncConnection

    src = inspect.getsource(AsyncConnection.force_close_transport)
    assert "contextlib.suppress(BaseException)" not in src, (
        "force_close_transport's _cancel_and_observe must use narrow "
        "Exception suppression — see _observe_drain_exception sibling"
    )
    assert "contextlib.suppress(Exception)" in src
