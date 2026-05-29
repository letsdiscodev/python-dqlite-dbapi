"""Pin: ``_get_resolve_leader_cluster`` raises ``RuntimeError`` (not
``InterfaceError``) when invoked outside a running event loop — a missing
loop is a private-helper invariant violation, not a PEP 249 §7 interface error,
and matches what ``asyncio.get_running_loop()`` itself raises.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.connection import _get_resolve_leader_cluster


def test_invocation_without_running_loop_raises_runtime_error() -> None:
    """A direct sync-context call to the private helper surfaces a plain
    ``RuntimeError``, not a misclassified PEP 249 InterfaceError."""
    with pytest.raises(RuntimeError, match="running event loop"):
        _get_resolve_leader_cluster(
            address="127.0.0.1:9001",
            timeout=1.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
        )
