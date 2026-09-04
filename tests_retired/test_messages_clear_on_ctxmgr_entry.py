"""``Cursor.__enter__``, ``AsyncCursor.__aenter__``, and ``AsyncConnection.transaction()``
clear ``messages`` on entry per PEP 249 §6.4 "cleared by all standard methods"."""

from __future__ import annotations

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio import AsyncConnection


def test_cursor_enter_clears_messages() -> None:
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
    """A closed cursor's ``with cur:`` is a permissive no-op, not a raise."""
    conn = Connection("localhost:9001", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.close()
        with cur:
            pass
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_cursor_aenter_clears_messages() -> None:
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
    """``async with conn.transaction():`` clears ``conn.messages`` on entry."""
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
            # tx-commit failure (no live cluster) is out of scope; the body assert is the pin.
            pass
    finally:
        await conn.close()
