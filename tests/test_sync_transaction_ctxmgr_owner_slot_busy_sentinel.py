"""Pin: sync ``Connection.transaction()`` parks
``_transaction_owner`` at a sentinel (``_OWNER_INTERNAL_BUSY``)
during the COMMIT/ROLLBACK wire RTT — under tier-2
(``check_same_thread=False``) a sibling thread observing the slot
sees "reserved" (the sentinel) and the nested-transaction reject
fires, instead of seeing a free slot and silently reserving it.

Without the sentinel, the slot was cleared to None during the
wire round-trip, opening a race window where a sibling thread
could reserve, BEGIN, and run a body whose stray
``conn.commit()`` calls miss the owner-token guard — silent
transaction-control divergence.
"""

from __future__ import annotations

import inspect


def test_transaction_uses_owner_busy_sentinel_in_commit_arm() -> None:
    """The COMMIT arm must use the sentinel under the state lock,
    not clear to None.
    """
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.transaction)
    # Sentinel must appear in source.
    assert "_OWNER_INTERNAL_BUSY" in source
    # The state-lock must wrap the sentinel-park.
    assert "with _state_lock:" in source


def test_transaction_uses_owner_busy_sentinel_in_rollback_arm() -> None:
    """The ROLLBACK arm must use the sentinel too (mirror)."""
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.transaction)
    # Both arms park the slot at sentinel — appears twice (commit +
    # rollback). Count occurrences to pin both arms.
    count = source.count("_OWNER_INTERNAL_BUSY")
    assert count >= 2, (
        f"both COMMIT and ROLLBACK arms must park the slot at the "
        f"sentinel; found {count} occurrence(s)"
    )


def test_owner_busy_sentinel_is_unique_object() -> None:
    """The sentinel must be a distinct object so identity checks
    don't false-positive against arbitrary values.
    """
    from dqlitedbapi.connection import _OWNER_INTERNAL_BUSY

    assert _OWNER_INTERNAL_BUSY is not None
    assert _OWNER_INTERNAL_BUSY is not False
    assert _OWNER_INTERNAL_BUSY is not 0  # noqa: F632
