"""Pin: ``_get_resolve_leader_cluster`` raises ``RuntimeError`` (not
``InterfaceError``) when invoked outside a running event loop.

The helper is structurally private (``_``-prefixed) and reachable
only via ``_resolve_leader`` → ``_build_and_connect`` (inside
``_run_sync``). Production callers always have a running loop. A
contributor or test fixture that invokes the helper directly from
sync code hits this guard.

PEP 249 §7 reserves ``InterfaceError`` for "problems with the
database interface rather than the database itself" — a missing
event loop is neither. ``RuntimeError`` matches what
``asyncio.get_running_loop()`` itself raises and is the right class
for a private-helper invariant violation.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.connection import _get_resolve_leader_cluster


def test_invocation_without_running_loop_raises_runtime_error() -> None:
    """The InterfaceError → RuntimeError reclassification: a
    direct sync-context call to the private helper surfaces a
    plain ``RuntimeError``, not the misclassified PEP 249 class
    that would leak out of public ``connect()`` as a fake
    "interface" failure."""
    with pytest.raises(RuntimeError, match="running event loop"):
        _get_resolve_leader_cluster(
            address="127.0.0.1:9001",
            timeout=1.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
        )
