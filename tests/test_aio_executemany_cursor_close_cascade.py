"""connection.close() cascades cursor._closed mid-executemany; the next per-iteration
_check_closed raises InterfaceError."""

from __future__ import annotations

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError


async def test_executemany_observes_cursor_close_cascade() -> None:
    cursor = AsyncCursor(connection=None)  # type: ignore[arg-type]
    cursor._closed = True
    with pytest.raises(InterfaceError, match="closed"):
        cursor._check_closed()


async def test_executemany_loop_resets_state_on_closed_cursor() -> None:
    cursor = AsyncCursor(connection=None)  # type: ignore[arg-type]
    cursor._closed = True
    with pytest.raises(InterfaceError):
        cursor._check_closed()
