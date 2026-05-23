"""PEP 249 §6.4 contract: ``Cursor.__iter__`` and
``AsyncCursor.__aiter__`` clear ``messages`` "prior to executing the
call", symmetric with sibling cursor methods.

Without this, a future driver path that populates ``messages`` would
let ``for row in cur:`` observe stale messages on an empty result
set (``__next__`` raises ``StopIteration`` without calling
``fetchone``'s clear). Latent today (no path populates messages) but
the project's discipline applies to every public cursor method.
"""

from __future__ import annotations

from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection

_WARNING_STALE: tuple[type[Exception], Exception] = (Warning, Warning("stale"))
_WARNING_STALE_AFTER_CLOSE: tuple[type[Exception], Exception] = (
    Warning,
    Warning("stale-after-close"),
)


def test_sync_iter_clears_messages() -> None:
    conn = Connection("127.0.0.1:9001")
    try:
        cur = conn.cursor()
        try:
            cur.messages.append(_WARNING_STALE)
            assert list(cur.messages) == [_WARNING_STALE]
            it = iter(cur)
            assert it is cur
            assert list(cur.messages) == []
        finally:
            cur.close()
    finally:
        conn._closed = True


async def test_async_aiter_clears_messages() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = conn.cursor()
    try:
        cur.messages.append(_WARNING_STALE)
        assert list(cur.messages) == [_WARNING_STALE]
        it = cur.__aiter__()
        assert it is cur
        assert list(cur.messages) == []
    finally:
        cur.close()


def test_sync_enter_clears_messages_on_closed_cursor() -> None:
    """``Cursor.__enter__`` clears messages unconditionally — symmetric
    with the sibling ``__iter__`` (which clears regardless of
    ``_closed`` state). PEP 249 §6.4 applies to every secondary entry
    point, not just the open-cursor ones.

    Pre-fix the ``__enter__`` body had ``if not self._closed: del
    self.messages[:]`` with a comment claiming it matched ``__iter__``'s
    shape — but ``__iter__`` does not skip on a closed cursor. The
    asymmetry would let a future driver path that appends to
    ``messages`` after close be observed by a closed-cursor
    ``with cur:`` even though ``with cur:`` is a documented clear-on-
    entry site.
    """
    conn = Connection("127.0.0.1:9001")
    try:
        cur = conn.cursor()
        cur.close()
        # Append AFTER close (close itself clears messages). Simulates
        # a future driver path that publishes to messages from a
        # background producer.
        cur.messages.append(_WARNING_STALE_AFTER_CLOSE)
        with cur:
            assert list(cur.messages) == [], (
                "Cursor.__enter__ must clear messages on entry (PEP 249 §6.4) "
                "symmetric with __iter__'s unconditional clear"
            )
    finally:
        conn._closed = True


async def test_async_aenter_clears_messages_on_closed_cursor() -> None:
    """``AsyncCursor.__aenter__`` clears messages unconditionally —
    symmetric with the sibling ``__aiter__`` (which clears regardless
    of ``_closed`` state). Mirrors the sync sibling test above."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = conn.cursor()
    cur.close()
    cur.messages.append(_WARNING_STALE_AFTER_CLOSE)
    async with cur:
        assert list(cur.messages) == [], (
            "AsyncCursor.__aenter__ must clear messages on entry "
            "(PEP 249 §6.4) symmetric with __aiter__'s unconditional clear"
        )
