"""``max_continuation_frames`` above ``MAX_CONTINUATION_FRAMES_UPPER_BOUND`` is
rejected at the dbapi boundary (direct callers previously bypassed the SA cap)."""

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
    """The cap is inclusive: the exact value is accepted, only strictly above rejected."""
    conn = dqlitedbapi.Connection(
        "127.0.0.1:9999",
        max_continuation_frames=MAX_CONTINUATION_FRAMES_UPPER_BOUND,
    )
    try:
        assert conn._max_continuation_frames == MAX_CONTINUATION_FRAMES_UPPER_BOUND
    finally:
        conn.close()


def test_upper_bound_is_ten_times_default() -> None:
    """The cap is 10x the wire default (defence against typos granting huge budgets)."""
    from dqlitewire import DEFAULT_MAX_CONTINUATION_FRAMES

    assert MAX_CONTINUATION_FRAMES_UPPER_BOUND == 10 * DEFAULT_MAX_CONTINUATION_FRAMES
