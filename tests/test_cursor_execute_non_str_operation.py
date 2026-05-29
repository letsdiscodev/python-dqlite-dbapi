"""execute / AsyncCursor.execute reject a non-str operation with
ProgrammingError (not bare AttributeError/TypeError) so cross-driver
except dbapi.Error catches the misuse (PEP 249 §7). Stricter than stdlib
sqlite3, which raises bare TypeError."""

from __future__ import annotations

import pytest

from dqlitedbapi import ProgrammingError, connect


@pytest.mark.parametrize("bad", [None, b"SELECT 1", 42, ["SELECT 1"]])
def test_sync_cursor_execute_rejects_non_str_operation(bad: object) -> None:
    conn = connect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            cur.execute(bad)  # type: ignore[arg-type]
    finally:
        cur.close()
        conn.close()


@pytest.mark.parametrize("bad", [None, b"SELECT 1", 42, ["SELECT 1"]])
async def test_async_cursor_execute_rejects_non_str_operation(bad: object) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            await cur.execute(bad)  # type: ignore[arg-type]
    finally:
        cur.close()
        await conn.close()
