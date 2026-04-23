"""Integration pin: ``sqlite_version_info`` must not advertise more
than the server actually supports.

``dqlitedbapi`` exposes ``sqlite_version_info`` (and its string form)
at module import time — PEP 249 / SQLAlchemy require them to be
synchronously available before any connection is opened. The value is
a hard-coded constant because dialect bootstrap cannot wait for a
connection handshake.

The test interrogates the live server with ``SELECT sqlite_version()``
and asserts the module constant does not exceed what the server
reports. SQLAlchemy's SQLite dialect gates feature code paths on
``sqlite_version_info``; advertising a version higher than the server
supports would make the dialect emit SQL the server rejects.

When this test fails because upstream dqlite ships a newer SQLite
bundle (and the constant is older), the test is still green — the
constant is merely stale. If the constant is advanced past the
server's actual bundle, this test turns red and pins the driver
honest.
"""

import pytest

import dqlitedbapi
from dqlitedbapi import aio, connect
from dqlitedbapi.aio import aconnect


def _parse_version(s: str) -> tuple[int, ...]:
    # ``SELECT sqlite_version()`` returns e.g. "3.45.1".
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
        # The two module-level constants must stay in lockstep; a drift
        # means a coordinated bump was applied to one __init__.py but
        # not the other.
        assert dqlitedbapi.sqlite_version_info == aio.sqlite_version_info
        assert dqlitedbapi.sqlite_version == aio.sqlite_version
