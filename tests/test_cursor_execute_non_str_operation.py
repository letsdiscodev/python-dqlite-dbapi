"""Pin: ``Cursor.execute`` and ``AsyncCursor.execute`` reject a non-str
``operation`` with ``ProgrammingError``, not bare ``AttributeError`` /
``TypeError``.

Stdlib ``sqlite3.Cursor.execute(None)`` raises bare ``TypeError``; this
driver elects to be stricter (per ``done/dbapi-executemany-sql-none-...``)
because PEP 249 §7 says all errors raised by the module are
``Error`` subclasses, and cross-driver code that catches
``except dbapi.Error`` must catch the misuse without falling back to
``except Exception``. The executemany ``seq_of_parameters=None`` arm
already pays the discipline; this is the missing ``execute`` /
``operation`` arm.
"""

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


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [None, b"SELECT 1", 42, ["SELECT 1"]])
async def test_async_cursor_execute_rejects_non_str_operation(bad: object) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            await cur.execute(bad)  # type: ignore[arg-type]
    finally:
        await cur.close()
        await conn.close()
