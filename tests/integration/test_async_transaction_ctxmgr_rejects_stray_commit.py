"""Pin: ``async with conn.transaction(): await conn.commit()`` raises
``InterfaceError`` instead of silently exiting the transaction.

The ctxmgr owns transaction boundaries. A stray ``await conn.commit()``
inside the body previously routed through to the client and ended the
transaction silently — subsequent body statements then ran in
autocommit and the surrounding ``rollback`` at exit no-op'd because
in_transaction was already False. asyncpg and psycopg both reject
nested explicit transaction control on the same shape.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect


@pytest.mark.asyncio
async def test_commit_inside_transaction_ctxmgr_raises(cluster_address: str) -> None:
    """Pin: a stray ``await conn.commit()`` inside the body raises
    ``InterfaceError`` — the ctxmgr owns transaction boundaries.
    Without this guard, the body's commit silently routed through
    to the client and ended the transaction; subsequent body
    statements then ran in autocommit and the rollback-at-exit
    no-op'd because in_transaction was already False."""
    conn = await aconnect(cluster_address, database="test_tx_stray_commit")
    try:
        async with conn.transaction():
            with pytest.raises(dqlitedbapi.InterfaceError, match="context manager"):
                await conn.commit()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_rollback_inside_transaction_ctxmgr_raises(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, database="test_tx_stray_commit")
    try:
        async with conn.transaction():
            with pytest.raises(dqlitedbapi.InterfaceError, match="context manager"):
                await conn.rollback()
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_commit_outside_transaction_ctxmgr_still_works(
    cluster_address: str,
) -> None:
    """Negative pin: bare ``conn.commit()`` outside the ctxmgr is
    unaffected. The ctxmgr-owns-boundaries guard fires only when the
    owner-task token matches."""
    conn = await aconnect(cluster_address, database="test_tx_stray_commit")
    try:
        await conn.commit()  # no-op (autocommit; no tx active) — must not raise
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_nested_transaction_ctxmgr_rejected(cluster_address: str) -> None:
    """Pin: nested ``async with conn.transaction()`` raises immediately.
    Two levels of ctxmgr would have ambiguous semantics — the inner
    body's commit would close the outer's transaction. asyncpg
    requires explicit savepoints for nesting; we reject up front."""
    conn = await aconnect(cluster_address, database="test_tx_stray_commit")
    try:
        async with conn.transaction():
            with pytest.raises(dqlitedbapi.InterfaceError, match="Nested"):
                async with conn.transaction():
                    pass
    finally:
        await conn.close()
