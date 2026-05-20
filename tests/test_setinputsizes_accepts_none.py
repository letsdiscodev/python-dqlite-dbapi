"""Pin: ``Cursor.setinputsizes(None)`` and ``AsyncCursor.setinputsizes(None)``
accept None silently as a no-op (PEP 249 §6.2 permits "do nothing").

Matches stdlib ``sqlite3``, aiosqlite, psycopg, and asyncpg behavior.
The strict rejection for genuinely invalid types (int, dict, str, bytes,
memoryview) remains in place; only ``None`` is special-cased.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import Cursor
from dqlitedbapi.aio import AsyncConnection, AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import ProgrammingError


def test_sync_setinputsizes_none_is_noop() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = Cursor(conn)
        try:
            cur.setinputsizes(None)
        finally:
            cur.close()
    finally:
        conn.close()


def test_sync_setinputsizes_int_still_rejected() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = Cursor(conn)
        try:
            with pytest.raises(ProgrammingError):
                cur.setinputsizes(5)  # type: ignore[arg-type]
        finally:
            cur.close()
    finally:
        conn.close()


async def test_async_setinputsizes_none_is_noop() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    try:
        cur.setinputsizes(None)
    finally:
        await cur.close()


async def test_async_setinputsizes_int_still_rejected() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    try:
        with pytest.raises(ProgrammingError):
            cur.setinputsizes(5)  # type: ignore[arg-type]
    finally:
        await cur.close()
