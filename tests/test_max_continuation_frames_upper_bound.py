"""Pin: ``max_continuation_frames`` is rejected above
``MAX_CONTINUATION_FRAMES_UPPER_BOUND`` at the dbapi boundary.

The SA dialect's URL / connect_args validators rejected the same
input above 1 000 000 already; direct dbapi callers (Alembic, ad-hoc
operator scripts) bypassed the cap. The upper bound at the dbapi
layer harmonises the contract so the same input fails consistently
across SA, dbapi, and (via propagation through ``DqliteConnection``)
the wire.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi import MAX_CONTINUATION_FRAMES_UPPER_BOUND
from dqlitedbapi.aio import AsyncConnection


def test_above_upper_bound_rejected_at_sync_connection_constructor() -> None:
    with pytest.raises(dqlitedbapi.ProgrammingError, match="exceeds the upper bound"):
        dqlitedbapi.Connection(
            "127.0.0.1:9999",
            max_continuation_frames=MAX_CONTINUATION_FRAMES_UPPER_BOUND + 1,
        )


def test_above_upper_bound_rejected_at_top_level_connect() -> None:
    with pytest.raises(dqlitedbapi.ProgrammingError, match="exceeds the upper bound"):
        dqlitedbapi.connect(
            "127.0.0.1:9999",
            max_continuation_frames=MAX_CONTINUATION_FRAMES_UPPER_BOUND + 1,
        )


def test_above_upper_bound_rejected_at_async_connection_constructor() -> None:
    with pytest.raises(dqlitedbapi.ProgrammingError, match="exceeds the upper bound"):
        AsyncConnection(
            "127.0.0.1:9999",
            max_continuation_frames=MAX_CONTINUATION_FRAMES_UPPER_BOUND + 1,
        )


def test_exact_cap_value_accepted() -> None:
    """The cap is inclusive — exactly the cap value is accepted; only
    strictly above is rejected. Pin so a future maintainer cannot
    silently tighten the cap to exclusive comparison."""
    conn = dqlitedbapi.Connection(
        "127.0.0.1:9999",
        max_continuation_frames=MAX_CONTINUATION_FRAMES_UPPER_BOUND,
    )
    try:
        assert conn._max_continuation_frames == MAX_CONTINUATION_FRAMES_UPPER_BOUND
    finally:
        conn.close()


def test_upper_bound_is_ten_times_default() -> None:
    """The cap is 10× the wire default — matches the SA URL validator's
    rationale (defence-in-depth against typos that grant absurd
    decode budgets)."""
    from dqlitewire import DEFAULT_MAX_CONTINUATION_FRAMES

    assert MAX_CONTINUATION_FRAMES_UPPER_BOUND == 10 * DEFAULT_MAX_CONTINUATION_FRAMES
