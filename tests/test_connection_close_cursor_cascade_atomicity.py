"""Pin: ``Connection.close()`` cursor cascade clears the cursors set
even if the per-cursor scrub raises mid-loop.

Without the try/finally wrapper, a KI/SystemExit raised mid-iteration
left ``self._cursors`` populated with stale references — and later
cursors un-iterated (still ``_closed=False``, with stale ``_rows``).

The per-cursor ``_closed = True`` is intentionally the FIRST write so
even an interrupted scrub leaves cursors with the load-bearing flag
set. ``_check_closed()`` gates all reads, so stale ``_rows`` after a
cursor's own ``_closed=True`` is unreachable.
"""

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
    # Make cursor[3]._rows raise on assignment.
    _install_failing_rows_descriptor(cursors[3])

    for cur in cursors:
        conn._cursors.add(cur)

    try:
        with pytest.raises(RuntimeError, match="simulated mid-scrub"):
            conn.close()
    finally:
        _restore_rows_descriptor(cursors[3])

    # The cursors set MUST be empty even though the loop raised.
    assert len(conn._cursors) == 0


@pytest.mark.asyncio
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
