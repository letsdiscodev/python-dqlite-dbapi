"""Pin: ``Cursor.close`` and ``AsyncCursor.close`` must not raise
``AttributeError`` from the ``del self.messages[:]`` clear when a
subclass / test fixture has stripped the ``messages`` attribute. The
raise would supplant a body exception under ``__exit__`` /
``__aexit__`` (PEP 343 default), masking the user's real failure.

This mirrors the setter precedent already in place at
``arraysize.setter`` / ``row_factory.setter`` and matches the
SA-adapter execute finally's "suppress close errors so the primary
exception wins" discipline.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_cursor_exit_close_with_stripped_messages_preserves_body_exception() -> None:
    """A body ``ValueError`` must surface through ``with cur:`` even
    when ``del self.messages[:]`` inside close() would otherwise
    raise ``AttributeError``.

    Strip ``messages`` from INSIDE the body so ``__enter__``'s own
    messages-clear has already run on the attribute that existed at
    enter time — the realistic failure leg is the close-side raise,
    not the enter-side raise.
    """
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()

    with pytest.raises(ValueError, match="primary failure"), cur:
        # Strip ``messages`` to trip ``AttributeError`` on the
        # close()-side del. The setters' guarded
        # ``contextlib.suppress(AttributeError)`` established this
        # as the project's documented hazard.
        del cur.messages
        raise ValueError("primary failure")


def test_sync_cursor_close_with_stripped_messages_does_not_raise() -> None:
    """Direct ``close()`` on a cursor with stripped ``messages`` must
    return cleanly — the suppression is on the close path, not just
    on the ``__exit__`` wrapper."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    del cur.messages
    # Must not raise.
    cur.close()
    assert cur.closed is True


async def test_async_cursor_aexit_close_with_stripped_messages_preserves_body_exception() -> None:
    """A body ``ValueError`` must surface through ``async with cur:``
    even when ``del self.messages[:]`` inside close() would otherwise
    raise ``AttributeError``.

    Strip ``messages`` from INSIDE the body so ``__aenter__``'s own
    messages-clear has already run on the attribute that existed at
    enter time — the realistic failure leg is the close-side raise,
    not the enter-side raise.
    """
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()

    with pytest.raises(ValueError, match="primary failure"):
        async with cur:
            del cur.messages
            raise ValueError("primary failure")


async def test_async_cursor_close_with_stripped_messages_does_not_raise() -> None:
    """Direct ``close()`` on an AsyncCursor with stripped ``messages``
    must return cleanly."""
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    del cur.messages
    # Must not raise.
    cur.close()
    assert cur.closed is True
