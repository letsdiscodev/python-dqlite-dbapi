"""``cursor.description`` survives empty fetchall/fetchone/fetchmany
(PEP 249 §6.6: available after the last fetch)."""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncGenerator, Generator

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection, aconnect


@pytest.fixture
def conn() -> Generator[dqlitedbapi.Connection]:
    address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
    c = dqlitedbapi.connect(address, timeout=5.0)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
async def aconn() -> AsyncGenerator[AsyncConnection]:
    address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")
    c = await aconnect(address, timeout=5.0)
    try:
        yield c
    finally:
        await c.close()


def _setup_empty_table(cur: dqlitedbapi.Cursor) -> str:
    name = f"desc_empty_{uuid.uuid4().hex[:8]}"
    cur.execute(f"CREATE TABLE {name} (id INTEGER, label TEXT)")
    return name


async def _setup_empty_table_async(cur) -> str:  # noqa: ANN001
    name = f"desc_empty_a_{uuid.uuid4().hex[:8]}"
    await cur.execute(f"CREATE TABLE {name} (id INTEGER, label TEXT)")
    return name


@pytest.mark.integration
class TestSyncDescriptionSurvivesEmptyFetch:
    def test_description_survives_empty_fetchall(self, conn: dqlitedbapi.Connection) -> None:
        cur = conn.cursor()
        name = _setup_empty_table(cur)
        try:
            cur.execute(f"SELECT id, label FROM {name}")
            rows = cur.fetchall()
            assert rows == []
            assert cur.description is not None
            assert len(cur.description) == 2
            assert cur.description[0][0] == "id"
            assert cur.description[1][0] == "label"
        finally:
            cur.execute(f"DROP TABLE {name}")

    def test_description_survives_empty_fetchone(self, conn: dqlitedbapi.Connection) -> None:
        cur = conn.cursor()
        name = _setup_empty_table(cur)
        try:
            cur.execute(f"SELECT id FROM {name}")
            row = cur.fetchone()
            assert row is None
            assert cur.description is not None
            assert cur.description[0][0] == "id"
        finally:
            cur.execute(f"DROP TABLE {name}")

    def test_description_survives_empty_fetchmany(self, conn: dqlitedbapi.Connection) -> None:
        cur = conn.cursor()
        name = _setup_empty_table(cur)
        try:
            cur.execute(f"SELECT id FROM {name}")
            rows = cur.fetchmany(10)
            assert rows == []
            assert cur.description is not None
        finally:
            cur.execute(f"DROP TABLE {name}")


@pytest.mark.integration
class TestAsyncDescriptionSurvivesEmptyFetch:
    async def test_description_survives_empty_fetchall(self, aconn: AsyncConnection) -> None:
        cur = aconn.cursor()
        name = await _setup_empty_table_async(cur)
        try:
            await cur.execute(f"SELECT id, label FROM {name}")
            rows = await cur.fetchall()
            assert rows == []
            assert cur.description is not None
            assert len(cur.description) == 2
            assert cur.description[0][0] == "id"
            assert cur.description[1][0] == "label"
        finally:
            await cur.execute(f"DROP TABLE {name}")

    async def test_description_survives_empty_fetchone(self, aconn: AsyncConnection) -> None:
        cur = aconn.cursor()
        name = await _setup_empty_table_async(cur)
        try:
            await cur.execute(f"SELECT id FROM {name}")
            row = await cur.fetchone()
            assert row is None
            assert cur.description is not None
        finally:
            await cur.execute(f"DROP TABLE {name}")
