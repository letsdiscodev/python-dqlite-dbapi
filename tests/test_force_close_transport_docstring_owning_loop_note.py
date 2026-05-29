"""Pin: ``force_close_transport``'s docstring documents the owning-loop
cursor-cascade cost so an in-loop caller picks ``close()`` instead.
"""

from __future__ import annotations

from dqlitedbapi.aio.connection import AsyncConnection


def test_force_close_transport_docstring_mentions_owning_loop_cascade() -> None:
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
