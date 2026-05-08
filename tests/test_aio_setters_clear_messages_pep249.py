"""Pin: ``AsyncConnection.autocommit``, ``isolation_level``, and
``text_factory`` setters clear ``messages`` first (PEP 249 §6.4 +
project discipline + sync-sibling parity).

Sync ``autocommit.setter`` / ``isolation_level.setter`` /
``text_factory.setter`` already begin with ``del self.messages[:]``.
The async siblings used to skip the clear, so a populated
``messages`` list survived the no-op accept path and observable
attribute mutation.
"""

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


@pytest.mark.asyncio
async def test_autocommit_setter_clears_messages(conn: Any) -> None:
    conn.messages.append(("synthetic",))
    conn.autocommit = True
    assert conn.messages == [], "PEP 249 §6.4 messages-clear before autocommit set"


@pytest.mark.asyncio
async def test_isolation_level_setter_clears_messages(conn: Any) -> None:
    conn.messages.append(("synthetic",))
    conn.isolation_level = None
    assert conn.messages == [], "PEP 249 §6.4 messages-clear before isolation_level set"


@pytest.mark.asyncio
async def test_text_factory_setter_clears_messages(conn: Any) -> None:
    conn.messages.append(("synthetic",))
    conn.text_factory = str
    assert conn.messages == [], "PEP 249 §6.4 messages-clear before text_factory set"
