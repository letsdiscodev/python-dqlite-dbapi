"""Pin: `dqlitedbapi.aio` re-exports the three diagnostic-surface
constants from the sync sibling.

Async-only retry middleware authors should be able to import the
canonical disconnect-discrimination prefixes from `dqlitedbapi.aio`
without reaching into the sync surface. The constants are
module-globals shared with the sync side; ``is``-identity holds.
"""

from __future__ import annotations

import dqlitedbapi
import dqlitedbapi.aio


def test_failed_to_connect_prefix_reexported_by_identity() -> None:
    assert dqlitedbapi.aio.FAILED_TO_CONNECT_PREFIX is dqlitedbapi.FAILED_TO_CONNECT_PREFIX


def test_cluster_policy_rejection_prefix_reexported_by_identity() -> None:
    assert (
        dqlitedbapi.aio.CLUSTER_POLICY_REJECTION_PREFIX
        is dqlitedbapi.CLUSTER_POLICY_REJECTION_PREFIX
    )


def test_max_continuation_frames_upper_bound_reexported_by_identity() -> None:
    assert (
        dqlitedbapi.aio.MAX_CONTINUATION_FRAMES_UPPER_BOUND
        is dqlitedbapi.MAX_CONTINUATION_FRAMES_UPPER_BOUND
    )


def test_all_includes_the_three_constants() -> None:
    """Public-surface pin: the constants are in `__all__` so
    `from dqlitedbapi.aio import *` picks them up symmetrically with
    the sync surface."""
    assert "FAILED_TO_CONNECT_PREFIX" in dqlitedbapi.aio.__all__
    assert "CLUSTER_POLICY_REJECTION_PREFIX" in dqlitedbapi.aio.__all__
    assert "MAX_CONTINUATION_FRAMES_UPPER_BOUND" in dqlitedbapi.aio.__all__
