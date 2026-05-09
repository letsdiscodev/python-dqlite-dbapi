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

import pytest

from dqlitedbapi import Connection
from dqlitedbapi.aio.connection import AsyncConnection


def test_sync_iter_clears_messages() -> None:
    conn = Connection("127.0.0.1:9001")
    try:
        cur = conn.cursor()
        try:
            cur.messages.append((Warning, "stale"))
            assert list(cur.messages) == [(Warning, "stale")]
            it = iter(cur)
            assert it is cur
            assert list(cur.messages) == []
        finally:
            cur.close()
    finally:
        conn._closed = True


@pytest.mark.asyncio
async def test_async_aiter_clears_messages() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    cur = conn.cursor()
    try:
        cur.messages.append((Warning, "stale"))
        assert list(cur.messages) == [(Warning, "stale")]
        it = cur.__aiter__()
        assert it is cur
        assert list(cur.messages) == []
    finally:
        await cur.close()
