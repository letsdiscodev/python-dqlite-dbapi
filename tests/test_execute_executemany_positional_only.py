"""Pin: Connection.execute / executemany / Cursor.execute /
executemany are positional-only (`/` after parameters).

Stdlib sqlite3 raises TypeError for `execute(operation="...")` —
the keyword argument is rejected. dqlitedbapi previously accepted
the kwarg, breaking cross-driver portable code.

Symmetric with the existing executescript / setinputsizes
positional-only fixes.
"""

import pytest

import dqlitedbapi
import dqlitedbapi.aio


def test_sync_connection_execute_rejects_keyword_operation() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        with pytest.raises(TypeError):
            conn.execute(operation="SELECT 1")  # type: ignore[call-arg]
    finally:
        conn.close()


def test_sync_connection_executemany_rejects_keyword_operation() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        with pytest.raises(TypeError):
            conn.executemany(operation="INSERT INTO t VALUES (?)", seq_of_parameters=[(1,)])  # type: ignore[call-arg]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_connection_execute_rejects_keyword_operation() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(TypeError):
        await aconn.execute(operation="SELECT 1")  # type: ignore[call-arg]


@pytest.mark.asyncio
async def test_async_connection_executemany_rejects_keyword_operation() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(TypeError):
        await aconn.executemany(  # type: ignore[call-arg]
            operation="INSERT INTO t VALUES (?)",
            seq_of_parameters=[(1,)],
        )


def test_sync_cursor_execute_rejects_keyword_operation() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            with pytest.raises(TypeError):
                cur.execute(operation="SELECT 1")  # type: ignore[call-arg]
        finally:
            cur.close()
    finally:
        conn.close()


def test_sync_cursor_executemany_rejects_keyword_operation() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            with pytest.raises(TypeError):
                cur.executemany(  # type: ignore[call-arg]
                    operation="INSERT INTO t VALUES (?)",
                    seq_of_parameters=[(1,)],
                )
        finally:
            cur.close()
    finally:
        conn.close()
