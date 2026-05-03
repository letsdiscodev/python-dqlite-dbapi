"""Pin ``cursor.lastrowid`` end-to-end for INTEGER PRIMARY KEY inserts.

PEP 249 optional extension: ``lastrowid`` carries the autoincrement
primary-key value of the last row inserted. Unit tests cover the
None initial state but not the real server round-trip; this fence
catches any refactor of the ``StmtResponse`` / ``ResultResponse``
decoder that accidentally reads the wrong field into ``lastrowid``.
"""

from __future__ import annotations

import os
from collections.abc import Generator

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


@pytest.mark.integration
class TestLastrowidAutoincrement:
    def test_lastrowid_after_insert(self, conn: dqlitedbapi.Connection) -> None:
        cur = conn.cursor()
        try:
            cur.execute("DROP TABLE IF EXISTS t_rowid_sync")
            cur.execute("CREATE TABLE t_rowid_sync (id INTEGER PRIMARY KEY, v TEXT)")
            # stdlib-parity: DDL does not update ``lastrowid``. Pre-insert
            # the field is ``None`` (its initial value).
            assert cur.lastrowid is None

            cur.execute("INSERT INTO t_rowid_sync (v) VALUES (?)", ("alpha",))
            first = cur.lastrowid
            assert first is not None and first > 0

            cur.execute("INSERT INTO t_rowid_sync (v) VALUES (?)", ("beta",))
            second = cur.lastrowid
            assert second == first + 1

            cur.execute("DROP TABLE t_rowid_sync")
            # DDL does not affect lastrowid — it still points at the
            # last successful INSERT.
            assert cur.lastrowid == second
        finally:
            cur.close()

    def test_lastrowid_with_explicit_autoincrement(self, conn: dqlitedbapi.Connection) -> None:
        cur = conn.cursor()
        try:
            cur.execute("DROP TABLE IF EXISTS t_rowid_auto")
            cur.execute("CREATE TABLE t_rowid_auto (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)")
            cur.execute("INSERT INTO t_rowid_auto (v) VALUES (?)", ("alpha",))
            assert cur.lastrowid is not None and cur.lastrowid > 0
            cur.execute("DROP TABLE t_rowid_auto")
        finally:
            cur.close()


@pytest.mark.integration
class TestAsyncLastrowidAutoincrement:
    async def test_lastrowid_after_insert(self) -> None:
        address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
        aconn = await aconnect(address, timeout=5.0)
        try:
            cur = aconn.cursor()
            await cur.execute("DROP TABLE IF EXISTS t_rowid_async")
            await cur.execute("CREATE TABLE t_rowid_async (id INTEGER PRIMARY KEY, v TEXT)")
            # stdlib-parity: DDL does not update ``lastrowid``.
            assert cur.lastrowid is None

            await cur.execute("INSERT INTO t_rowid_async (v) VALUES (?)", ("alpha",))
            first = cur.lastrowid
            assert first is not None and first > 0

            await cur.execute("INSERT INTO t_rowid_async (v) VALUES (?)", ("beta",))
            assert cur.lastrowid == first + 1

            await cur.execute("DROP TABLE t_rowid_async")
            await cur.close()
        finally:
            await aconn.close()
