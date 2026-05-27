"""Pin: ``AsyncConnection.force_close_transport``'s docstring
documents the owning-loop cursor-cascade cost.

When invoked from a coroutine on the owning loop, the cursor-cascade
walk runs inline (no yield) — synchronous per-cursor attribute writes
plus a ``weakref.proxy`` swap. For a connection with hundreds of
open cursors that's measurable loop CPU. The documented intent of
the method is loop-already-dead / GC / atexit / SA-do_terminate
paths where the cascade-on-owning-loop case doesn't arise, so the
behaviour is correct; the docstring just needs to surface the
constraint so an in-loop caller can pick the right method (``close()``).
"""

from __future__ import annotations

from dqlitedbapi.aio.connection import AsyncConnection


def test_force_close_transport_docstring_mentions_owning_loop_cascade() -> None:
    """The docstring must surface the owning-loop O(N_cursors)
    cascade cost so an in-loop caller picks ``close()`` instead."""
    doc = AsyncConnection.force_close_transport.__doc__ or ""
    assert "owning loop" in doc.lower(), (
        "force_close_transport docstring must mention the owning-loop "
        "case so in-loop callers can pick the cooperative ``close()``"
    )
    assert "cursor" in doc.lower(), (
        "force_close_transport docstring must mention cursor-cascade "
        "cost on the owning-loop call shape"
    )
    assert "close()" in doc, (
        "force_close_transport docstring must point at ``close()`` "
        "as the cooperative alternative for in-loop callers"
    )
