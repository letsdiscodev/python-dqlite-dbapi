"""Pin: optional PEP 249 §7 (TPC) and stdlib-sqlite3-parity helpers
raise ``NotSupportedError`` on both sync and async ``Connection``,
not ``AttributeError``.

PEP 249 §7 says drivers without two-phase-commit support MUST raise
``NotSupportedError`` from ``tpc_*`` methods. AttributeError escapes
the dbapi.Error hierarchy, so a caller's ``except Error:`` skips it
— users porting from psycopg / asyncpg / stdlib sqlite3 expect the
PEP 249 surface to be uniform.

Stdlib sqlite3-parity helpers (``load_extension``, ``backup``,
``iterdump``, ``create_function`` / ``_aggregate`` / ``_collation`` /
``_window_function``) are not part of PEP 249, but stdlib raises
``sqlite3.NotSupportedError`` (a PEP 249 ``NotSupportedError``) when
the underlying SQLite was built without the corresponding feature.
Mirror that contract so cross-driver code branching on
``sqlite3.NotSupportedError`` continues to work.

dqlite-server does not implement any of these; the stubs are
permanent rejections, not "not yet."
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import NotSupportedError


@pytest.fixture
def conn() -> Iterator[dqlitedbapi.Connection]:
    c = dqlitedbapi.connect("127.0.0.1:9999")
    yield c
    c._closed_flag[0] = True


@pytest.fixture
def aconn() -> AsyncConnection:
    return AsyncConnection("127.0.0.1:9999")


class TestSyncTpcStubs:
    def test_tpc_begin(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.tpc_begin(object())

    def test_tpc_prepare(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.tpc_prepare()

    def test_tpc_commit(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.tpc_commit()

    def test_tpc_rollback(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.tpc_rollback()

    def test_tpc_recover(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.tpc_recover()

    def test_xid(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError):
            conn.xid(1, "g", "b")


class TestSyncStdlibParityStubs:
    def test_enable_load_extension(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="extension"):
            conn.enable_load_extension(True)

    def test_load_extension(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="extension"):
            conn.load_extension("foo.so")

    def test_backup(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="backup"):
            conn.backup()

    def test_iterdump(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="iterdump"):
            conn.iterdump()

    def test_create_function(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="function"):
            conn.create_function("name", 0, lambda: 1)

    def test_create_aggregate(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="aggregate"):
            conn.create_aggregate("name", 0, object)

    def test_create_collation(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="collation"):
            conn.create_collation("name", lambda a, b: 0)

    def test_create_window_function(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="window"):
            conn.create_window_function("name", 0, object)


class TestAsyncTpcStubs:
    @pytest.mark.asyncio
    async def test_tpc_begin(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_begin(object())

    @pytest.mark.asyncio
    async def test_tpc_prepare(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_prepare()

    @pytest.mark.asyncio
    async def test_tpc_commit(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_commit()

    @pytest.mark.asyncio
    async def test_tpc_rollback(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_rollback()

    @pytest.mark.asyncio
    async def test_tpc_recover(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_recover()

    def test_xid(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            aconn.xid(1, "g", "b")


class TestAsyncStdlibParityStubs:
    def test_enable_load_extension(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="extension"):
            aconn.enable_load_extension(True)

    def test_load_extension(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="extension"):
            aconn.load_extension("foo.so")

    @pytest.mark.asyncio
    async def test_backup(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="backup"):
            await aconn.backup()

    def test_iterdump(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="iterdump"):
            aconn.iterdump()

    def test_create_function(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="function"):
            aconn.create_function("name", 0, lambda: 1)

    def test_create_aggregate(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="aggregate"):
            aconn.create_aggregate("name", 0, object)

    def test_create_collation(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="collation"):
            aconn.create_collation("name", lambda a, b: 0)

    def test_create_window_function(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="window"):
            aconn.create_window_function("name", 0, object)


def test_close_clears_messages() -> None:
    """PEP 249 §6.1.1 requires Connection.messages to be cleared on
    every standard Connection method invocation. The four sibling
    methods (commit, rollback, cursor) already clear; close() also
    clears so the contract is uniform."""
    import contextlib as _contextlib

    c = dqlitedbapi.connect("127.0.0.1:9999")
    c.messages.append((Exception, "stale"))
    with _contextlib.suppress(Exception):
        c.close()
    assert c.messages == []


@pytest.mark.asyncio
async def test_async_close_clears_messages() -> None:
    """Same as the sync sibling, for AsyncConnection."""
    import contextlib as _contextlib

    c = AsyncConnection("127.0.0.1:9999")
    c.messages.append((Exception, "stale"))
    with _contextlib.suppress(Exception):
        await c.close()
    assert c.messages == []
