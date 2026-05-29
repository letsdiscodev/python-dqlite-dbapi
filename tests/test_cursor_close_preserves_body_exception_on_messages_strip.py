"""Pin: ``Cursor.close`` / ``AsyncCursor.close`` must not raise
``AttributeError`` from the ``messages`` clear when ``messages`` is
stripped, so a body exception under ``__exit__``/``__aexit__`` is not masked.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_cursor_exit_close_with_stripped_messages_preserves_body_exception() -> None:
    """A body ValueError must surface through ``with cur:`` even when the
    close()-side messages clear would otherwise raise AttributeError.

    Strip ``messages`` inside the body so ``__enter__``'s own clear has
    already run — the failure leg under test is the close-side raise.
    """
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()

    with pytest.raises(ValueError, match="primary failure"), cur:
        del cur.messages
        raise ValueError("primary failure")


def test_sync_cursor_close_with_stripped_messages_does_not_raise() -> None:
    """Direct ``close()`` on a cursor with stripped ``messages`` returns cleanly."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    del cur.messages
    cur.close()
    assert cur.closed is True


async def test_async_cursor_aexit_close_with_stripped_messages_preserves_body_exception() -> None:
    """A body ValueError must surface through ``async with cur:`` even when
    the close()-side messages clear would otherwise raise AttributeError."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()

    with pytest.raises(ValueError, match="primary failure"):
        async with cur:
            del cur.messages
            raise ValueError("primary failure")


async def test_async_cursor_close_with_stripped_messages_does_not_raise() -> None:
    """Direct ``close()`` on an AsyncCursor with stripped ``messages`` returns cleanly."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    del cur.messages
    cur.close()
    assert cur.closed is True
