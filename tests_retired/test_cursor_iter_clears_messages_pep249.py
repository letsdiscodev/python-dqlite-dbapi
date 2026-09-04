"""``__iter__`` / ``__aiter__`` clear ``messages`` on entry (PEP 249 §6.4)."""

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
    """``__enter__`` clears messages even on a closed cursor (like ``__iter__``)."""
    conn = Connection("127.0.0.1:9001")
    try:
        cur = conn.cursor()
        cur.close()
        # Append AFTER close (close itself clears messages).
        cur.messages.append(_WARNING_STALE_AFTER_CLOSE)
        with cur:
            assert list(cur.messages) == [], (
                "Cursor.__enter__ must clear messages on entry (PEP 249 §6.4) "
                "symmetric with __iter__'s unconditional clear"
            )
    finally:
        conn._closed = True


async def test_async_aenter_clears_messages_on_closed_cursor() -> None:
    """``__aenter__`` clears messages even on a closed cursor (like ``__aiter__``)."""
    conn = AsyncConnection("127.0.0.1:9001")
    cur = conn.cursor()
    cur.close()
    cur.messages.append(_WARNING_STALE_AFTER_CLOSE)
    async with cur:
        assert list(cur.messages) == [], (
            "AsyncCursor.__aenter__ must clear messages on entry "
            "(PEP 249 §6.4) symmetric with __aiter__'s unconditional clear"
        )
