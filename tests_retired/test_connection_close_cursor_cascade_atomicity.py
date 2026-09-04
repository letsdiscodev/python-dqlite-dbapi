"""``Connection.close()`` clears the cursors set even if a per-cursor scrub raises
mid-loop, so no stale references survive an interrupted cascade."""

from __future__ import annotations

import contextlib
from unittest.mock import MagicMock

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio import AsyncConnection


def _raise_on_assign(value: object) -> None:
    raise RuntimeError("simulated mid-scrub failure")


def _install_failing_rows_descriptor(target: object) -> None:
    type(target)._rows = property(  # type: ignore[attr-defined]
        lambda self: [],
        lambda self, v: _raise_on_assign(v),
    )


def _restore_rows_descriptor(target: object) -> None:
    with contextlib.suppress(AttributeError):
        del type(target)._rows  # type: ignore[attr-defined]


def test_sync_close_clears_cursors_set_even_when_scrub_raises_mid_loop() -> None:
    conn = Connection("localhost:9001")
    cursors = [MagicMock() for _ in range(5)]
    _install_failing_rows_descriptor(cursors[3])

    for cur in cursors:
        conn._cursors.add(cur)

    try:
        with pytest.raises(RuntimeError, match="simulated mid-scrub"):
            conn.close()
    finally:
        _restore_rows_descriptor(cursors[3])

    assert len(conn._cursors) == 0


async def test_async_close_clears_cursors_set_even_when_scrub_raises_mid_loop() -> None:
    conn = AsyncConnection("localhost:9001")
    cursors = [MagicMock() for _ in range(5)]
    _install_failing_rows_descriptor(cursors[3])

    for cur in cursors:
        conn._cursors.add(cur)

    try:
        with pytest.raises(RuntimeError, match="simulated mid-scrub"):
            await conn.close()
    finally:
        _restore_rows_descriptor(cursors[3])

    assert len(conn._cursors) == 0
