"""setoutputsize(None) is a silent no-op; invalid types (str/float/dict) still rejected."""

from __future__ import annotations

import pytest

from dqlitedbapi import Cursor
from dqlitedbapi.aio import AsyncConnection, AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import ProgrammingError


def test_sync_setoutputsize_none_is_noop() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = Cursor(conn)
        try:
            cur.setoutputsize(None)
            cur.setoutputsize(None, None)
        finally:
            cur.close()
    finally:
        conn.close()


def test_sync_setoutputsize_str_still_rejected() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = Cursor(conn)
        try:
            with pytest.raises(ProgrammingError):
                cur.setoutputsize("five")  # type: ignore[arg-type]
        finally:
            cur.close()
    finally:
        conn.close()


async def test_async_setoutputsize_none_is_noop() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    try:
        cur.setoutputsize(None)
        cur.setoutputsize(None, None)
    finally:
        cur.close()


async def test_async_setoutputsize_str_still_rejected() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    try:
        with pytest.raises(ProgrammingError):
            cur.setoutputsize("five")  # type: ignore[arg-type]
    finally:
        cur.close()
