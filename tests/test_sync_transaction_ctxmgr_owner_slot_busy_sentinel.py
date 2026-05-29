"""Sync ``transaction()`` parks ``_transaction_owner`` at ``_OWNER_INTERNAL_BUSY`` during the
COMMIT/ROLLBACK wire RTT so a tier-2 sibling thread sees "reserved" rather than a free slot it
could silently reserve mid-round-trip."""

from __future__ import annotations

import inspect


def test_transaction_uses_owner_busy_sentinel_in_commit_arm() -> None:
    """The COMMIT arm uses the sentinel under the state lock, not clear-to-None."""
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.transaction)
    assert "_OWNER_INTERNAL_BUSY" in source
    assert "with _state_lock:" in source


def test_transaction_uses_owner_busy_sentinel_in_rollback_arm() -> None:
    """The ROLLBACK arm must use the sentinel too (mirror)."""
    import dqlitedbapi.connection as conn_mod

    source = inspect.getsource(conn_mod.Connection.transaction)
    count = source.count("_OWNER_INTERNAL_BUSY")
    assert count >= 2, (
        f"both COMMIT and ROLLBACK arms must park the slot at the "
        f"sentinel; found {count} occurrence(s)"
    )


def test_owner_busy_sentinel_is_unique_object() -> None:
    """The sentinel must be a distinct object so identity checks don't false-positive."""
    from dqlitedbapi.connection import _OWNER_INTERNAL_BUSY

    assert _OWNER_INTERNAL_BUSY is not None
    assert _OWNER_INTERNAL_BUSY is not False
    assert _OWNER_INTERNAL_BUSY is not 0  # noqa: F632
