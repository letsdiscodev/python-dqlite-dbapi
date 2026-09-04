"""Pin: ``setinputsizes(None)`` is a silent no-op (matching stdlib sqlite3/aiosqlite/psycopg);
only None is special-cased — invalid types (int, etc.) are still rejected."""

from __future__ import annotations

import pytest

from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import ProgrammingError


def test_sync_setinputsizes_none_is_noop() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            cur.setinputsizes(None)
        finally:
            cur.close()
    finally:
        conn.close()


def test_sync_setinputsizes_int_still_rejected() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            with pytest.raises(ProgrammingError):
                cur.setinputsizes(5)  # type: ignore[arg-type]
        finally:
            cur.close()
    finally:
        conn.close()


async def test_async_setinputsizes_none_is_noop() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    try:
        cur.setinputsizes(None)
    finally:
        cur.close()


async def test_async_setinputsizes_int_still_rejected() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError):
            cur.setinputsizes(5)  # type: ignore[arg-type]
    finally:
        cur.close()
