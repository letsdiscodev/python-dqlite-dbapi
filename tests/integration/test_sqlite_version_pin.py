"""The hard-coded ``sqlite_version_info`` constant must not exceed the live server's version.

It is hard-coded (not read from a connection) because dialect bootstrap happens at import,
before any handshake; advertising a higher version makes SQLAlchemy emit SQL the server rejects.
A stale-low constant stays green; a too-high constant turns red.
"""

import pytest

import dqlitedbapi
from dqlitedbapi import aio, connect
from dqlitedbapi.aio import aconnect


def _parse_version(s: str) -> tuple[int, ...]:
    return tuple(int(p) for p in s.split("."))


@pytest.mark.integration
class TestSqliteVersionPin:
    def test_sync_module_constant_not_ahead_of_server(self, cluster_address: str) -> None:
        with connect(cluster_address, database="test_sqlite_version_pin") as conn:
            cur = conn.cursor()
            cur.execute("SELECT sqlite_version()")
            row = cur.fetchone()
            assert row is not None
            server_version = row[0]
        server_tuple = _parse_version(server_version)
        assert dqlitedbapi.sqlite_version_info <= server_tuple, (
            f"driver hard-codes {dqlitedbapi.sqlite_version}, server reports "
            f"{server_version}; either lower the pin in "
            "src/dqlitedbapi/__init__.py or coordinate a driver bump."
        )

    async def test_async_module_constant_not_ahead_of_server(self, cluster_address: str) -> None:
        conn = await aconnect(cluster_address, database="test_sqlite_version_pin_aio")
        try:
            cur = conn.cursor()
            await cur.execute("SELECT sqlite_version()")
            row = await cur.fetchone()
            assert row is not None
            server_version = row[0]
        finally:
            await conn.close()
        server_tuple = _parse_version(server_version)
        assert aio.sqlite_version_info <= server_tuple, (
            f"async driver hard-codes {aio.sqlite_version}, server reports "
            f"{server_version}; either lower the pin in "
            "src/dqlitedbapi/aio/__init__.py or coordinate a driver bump."
        )

    def test_sync_and_async_constants_agree(self) -> None:
        # The sync and async constants must stay in lockstep across both __init__.py files.
        assert dqlitedbapi.sqlite_version_info == aio.sqlite_version_info
        assert dqlitedbapi.sqlite_version == aio.sqlite_version
