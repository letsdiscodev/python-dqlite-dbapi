"""Sync ``transaction()`` parks ``_transaction_owner`` at ``_OWNER_INTERNAL_BUSY`` during the
COMMIT/ROLLBACK wire RTT so a tier-2 sibling thread sees "reserved" rather than a free slot it
could silently reserve mid-round-trip."""

from __future__ import annotations


def test_owner_busy_sentinel_is_unique_object() -> None:
    """The sentinel must be a distinct object so identity checks don't false-positive."""
    from dqlitedbapi.connection import _OWNER_INTERNAL_BUSY

    assert _OWNER_INTERNAL_BUSY is not None
    assert _OWNER_INTERNAL_BUSY is not False
    assert _OWNER_INTERNAL_BUSY is not 0  # noqa: F632
