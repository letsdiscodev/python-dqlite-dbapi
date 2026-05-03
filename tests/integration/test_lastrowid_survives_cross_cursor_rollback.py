"""Pin ``cursor.lastrowid`` cursor-scoped semantics under cross-cursor rollback.

stdlib parity: ``Cursor.lastrowid`` is cursor-scoped. ROLLBACK on the
connection rolls server-side rows back but does NOT clear any cursor's
``lastrowid`` — it remains as the last-known rowid the cursor reported.
``_reset_execute_state`` deliberately does not touch ``lastrowid``; this
test pins that contract for two cursors on the same connection where the
rollback is initiated against the connection (not a single cursor).

dqlite has no implicit BEGIN — statements are autocommit by default.
The test issues an explicit ``BEGIN`` through a cursor to make the
ROLLBACK meaningful.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator, Generator

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect


@pytest.fixture
def conn() -> Generator[dqlitedbapi.Connection]:
    address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
    c = dqlitedbapi.connect(address, timeout=5.0)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
async def aconn() -> AsyncGenerator[dqlitedbapi.aio.AsyncConnection]:
    address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
    c = await aconnect(address, timeout=5.0)
    try:
        yield c
    finally:
        await c.close()


@pytest.mark.integration
class TestLastrowidSurvivesCrossCursorRollback:
    def test_two_cursor_rollback_keeps_each_lastrowid(self, conn: dqlitedbapi.Connection) -> None:
        setup = conn.cursor()
        try:
            setup.execute("DROP TABLE IF EXISTS t_xc_rollback")
            setup.execute("CREATE TABLE t_xc_rollback (id INTEGER PRIMARY KEY, v TEXT)")
        finally:
            setup.close()

        cur_a = conn.cursor()
        cur_b = conn.cursor()
        try:
            # Open an explicit transaction so ROLLBACK is non-trivial.
            cur_a.execute("BEGIN")
            cur_a.execute("INSERT INTO t_xc_rollback (v) VALUES (?)", ("a",))
            a_rowid = cur_a.lastrowid
            assert a_rowid is not None and a_rowid > 0

            cur_b.execute("INSERT INTO t_xc_rollback (v) VALUES (?)", ("b",))
            b_rowid = cur_b.lastrowid
            assert b_rowid is not None and b_rowid == a_rowid + 1

            # ROLLBACK on the connection: rolls back BOTH cursors' inserts
            # on the server, but cursor-scoped lastrowid survives.
            conn.rollback()

            assert cur_a.lastrowid == a_rowid
            assert cur_b.lastrowid == b_rowid

            # Server-side: rows are gone.
            cur_a.execute("SELECT COUNT(*) FROM t_xc_rollback")
            row = cur_a.fetchone()
            assert row is not None and row[0] == 0
        finally:
            cur_a.close()
            cur_b.close()
            cleanup = conn.cursor()
            try:
                cleanup.execute("DROP TABLE IF EXISTS t_xc_rollback")
            finally:
                cleanup.close()


@pytest.mark.integration
class TestAsyncLastrowidSurvivesCrossCursorRollback:
    async def test_two_cursor_rollback_keeps_each_lastrowid(
        self, aconn: dqlitedbapi.aio.AsyncConnection
    ) -> None:
        setup = aconn.cursor()
        try:
            await setup.execute("DROP TABLE IF EXISTS t_xc_rollback_async")
            await setup.execute("CREATE TABLE t_xc_rollback_async (id INTEGER PRIMARY KEY, v TEXT)")
        finally:
            await setup.close()

        cur_a = aconn.cursor()
        cur_b = aconn.cursor()
        try:
            await cur_a.execute("BEGIN")
            await cur_a.execute("INSERT INTO t_xc_rollback_async (v) VALUES (?)", ("a",))
            a_rowid = cur_a.lastrowid
            assert a_rowid is not None and a_rowid > 0

            await cur_b.execute("INSERT INTO t_xc_rollback_async (v) VALUES (?)", ("b",))
            b_rowid = cur_b.lastrowid
            assert b_rowid is not None and b_rowid == a_rowid + 1

            await aconn.rollback()

            assert cur_a.lastrowid == a_rowid
            assert cur_b.lastrowid == b_rowid

            await cur_a.execute("SELECT COUNT(*) FROM t_xc_rollback_async")
            row = await cur_a.fetchone()
            assert row is not None and row[0] == 0
        finally:
            await cur_a.close()
            await cur_b.close()
            cleanup = aconn.cursor()
            try:
                await cleanup.execute("DROP TABLE IF EXISTS t_xc_rollback_async")
            finally:
                await cleanup.close()
