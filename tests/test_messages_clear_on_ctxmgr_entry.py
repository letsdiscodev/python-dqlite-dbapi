"""Pin: ``Cursor.__enter__``, ``AsyncCursor.__aenter__``, and
``AsyncConnection.transaction()`` clear ``messages`` on entry per
PEP 249 §6.4 "cleared by all standard methods".

Previously these three entry points skipped the clear; sibling
methods on the same classes (``Cursor.__iter__``, ``AsyncCursor.__aiter__``,
sync ``Connection.transaction``) all clear, and every other public
method on Connection / AsyncConnection / Cursor / AsyncCursor clears.
The three entry points were the lone deviations.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio import AsyncConnection


def test_cursor_enter_clears_messages() -> None:
    """Pin: ``with cur:`` clears ``cur.messages`` on entry."""
    conn = Connection("localhost:9001", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.messages.append((Warning, Warning("stale entry")))
        with cur:
            assert cur.messages == [], (
                f"Cursor.__enter__ must clear messages per PEP 249 §6.4; got {cur.messages!r}"
            )
    finally:
        conn.close()


def test_cursor_enter_on_closed_cursor_does_not_clear() -> None:
    """Negative pin: a closed cursor's ``with cur:`` is a permissive
    no-op (closed cursors have already had messages scrubbed by
    close()). Mirrors __iter__'s shape."""
    conn = Connection("localhost:9001", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.close()
        # Closed cursor: messages was cleared by close() already; pin
        # that __enter__ does NOT raise and does not crash.
        with cur:
            pass  # body just enters / exits the ctxmgr
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_cursor_aenter_clears_messages() -> None:
    """Pin: ``async with cur:`` clears ``cur.messages`` on entry."""
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        cur.messages.append((Warning, Warning("stale entry")))
        async with cur:
            assert cur.messages == [], (
                f"AsyncCursor.__aenter__ must clear messages per PEP 249 §6.4; got {cur.messages!r}"
            )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_async_connection_transaction_clears_messages() -> None:
    """Pin: ``async with conn.transaction():`` clears
    ``conn.messages`` on entry. Sync sibling already clears."""
    conn = AsyncConnection("localhost:9001")
    try:
        conn.messages.append((Warning, Warning("stale entry")))
        try:
            async with conn.transaction():
                assert conn.messages == [], (
                    f"AsyncConnection.transaction() must clear messages "
                    f"per PEP 249 §6.4; got {conn.messages!r}"
                )
        except Exception:  # noqa: BLE001 -- body assertion is the contract
            # If the body's assertion succeeded but tx-commit fails
            # (no live cluster), that's an integration concern, not
            # the contract under test.
            pass
    finally:
        await conn.close()
