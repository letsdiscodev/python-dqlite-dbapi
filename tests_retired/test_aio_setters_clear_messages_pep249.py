"""Async ``autocommit`` / ``isolation_level`` / ``text_factory`` setters clear
``messages`` first (PEP 249 §6.4, sync-sibling parity)."""

from __future__ import annotations

from typing import Any

import pytest

from dqlitedbapi.aio import aconnect


@pytest.fixture()
async def conn() -> Any:
    c = await aconnect("localhost:9001")
    try:
        yield c
    finally:
        await c.close()


async def test_autocommit_setter_clears_messages(conn: Any) -> None:
    conn.messages.append(("synthetic",))
    conn.autocommit = True
    assert conn.messages == [], "PEP 249 §6.4 messages-clear before autocommit set"


async def test_isolation_level_setter_clears_messages(conn: Any) -> None:
    conn.messages.append(("synthetic",))
    conn.isolation_level = None
    assert conn.messages == [], "PEP 249 §6.4 messages-clear before isolation_level set"


async def test_text_factory_setter_clears_messages(conn: Any) -> None:
    conn.messages.append(("synthetic",))
    conn.text_factory = str
    assert conn.messages == [], "PEP 249 §6.4 messages-clear before text_factory set"
