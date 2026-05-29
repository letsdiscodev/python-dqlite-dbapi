"""A stray ``commit()``/``rollback()`` inside ``conn.transaction()`` raises ``InterfaceError``:
the ctxmgr owns transaction boundaries. Matches asyncpg and psycopg."""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect


async def test_commit_inside_transaction_ctxmgr_raises(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, database="test_tx_stray_commit")
    try:
        async with conn.transaction():
            with pytest.raises(dqlitedbapi.InterfaceError, match="context manager"):
                await conn.commit()
    finally:
        await conn.close()


async def test_rollback_inside_transaction_ctxmgr_raises(cluster_address: str) -> None:
    conn = await aconnect(cluster_address, database="test_tx_stray_commit")
    try:
        async with conn.transaction():
            with pytest.raises(dqlitedbapi.InterfaceError, match="context manager"):
                await conn.rollback()
    finally:
        await conn.close()


async def test_commit_outside_transaction_ctxmgr_still_works(
    cluster_address: str,
) -> None:
    """Negative pin: bare ``conn.commit()`` outside the ctxmgr is unaffected."""
    conn = await aconnect(cluster_address, database="test_tx_stray_commit")
    try:
        await conn.commit()  # no-op (autocommit; no tx active) — must not raise
    finally:
        await conn.close()


async def test_nested_transaction_ctxmgr_rejected(cluster_address: str) -> None:
    """Nested ``async with conn.transaction()`` raises immediately; nesting needs savepoints."""
    conn = await aconnect(cluster_address, database="test_tx_stray_commit")
    try:
        async with conn.transaction():
            with pytest.raises(dqlitedbapi.InterfaceError, match="Nested"):
                async with conn.transaction():
                    pass
    finally:
        await conn.close()
