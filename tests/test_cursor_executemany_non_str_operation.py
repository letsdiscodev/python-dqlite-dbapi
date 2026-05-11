"""Pin: ``Cursor.executemany`` and ``AsyncCursor.executemany`` reject
a non-str ``operation`` with ``ProgrammingError``, not bare
``AttributeError`` / ``TypeError``.

The sibling pin
``test_cursor_execute_non_str_operation.py`` covers ``execute``; this
file covers the symmetric ``executemany`` gap. PEP 249 §7 requires
every error raised by the module to be a ``Error`` subclass —
cross-driver code that catches ``except dbapi.Error`` must catch the
misuse without falling back to ``except Exception``.

Also covers both Connection-level shortcuts (``Connection.executemany``
and ``AsyncConnection.executemany``) which delegate to the cursor.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import ProgrammingError, connect


@pytest.mark.parametrize(
    "bad", [None, b"INSERT INTO t VALUES (?)", 42, ["INSERT INTO t VALUES (?)"]]
)
def test_sync_cursor_executemany_rejects_non_str_operation(bad: object) -> None:
    conn = connect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            cur.executemany(bad, [(1,), (2,)])  # type: ignore[arg-type]
    finally:
        cur.close()
        conn.close()


@pytest.mark.parametrize(
    "bad", [None, b"INSERT INTO t VALUES (?)", 42, ["INSERT INTO t VALUES (?)"]]
)
async def test_async_cursor_executemany_rejects_non_str_operation(bad: object) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            await cur.executemany(bad, [(1,), (2,)])  # type: ignore[arg-type]
    finally:
        await cur.close()
        await conn.close()


@pytest.mark.parametrize(
    "bad", [None, b"INSERT INTO t VALUES (?)", 42, ["INSERT INTO t VALUES (?)"]]
)
def test_sync_connection_executemany_shortcut_rejects_non_str_operation(bad: object) -> None:
    """``Connection.executemany`` shortcut delegates to ``Cursor.executemany``;
    inherit the guard transitively."""
    conn = connect("localhost:9001")
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            conn.executemany(bad, [(1,), (2,)])  # type: ignore[arg-type]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "bad", [None, b"INSERT INTO t VALUES (?)", 42, ["INSERT INTO t VALUES (?)"]]
)
async def test_async_connection_executemany_shortcut_rejects_non_str_operation(
    bad: object,
) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect("localhost:9001")
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            await conn.executemany(bad, [(1,), (2,)])  # type: ignore[arg-type]
    finally:
        await conn.close()
