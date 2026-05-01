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


class TestSyncCycle22StubFamily:
    """Stubs added alongside the cycle-22 stdlib-parity work
    (executescript, interrupt, set_authorizer / progress /
    trace, total_changes, getlimit / setlimit, getconfig /
    setconfig, serialize / deserialize, blobopen). All return
    ``NotSupportedError`` rather than escaping ``AttributeError``;
    pin the behaviour so a future regression to
    ``AttributeError`` (e.g. accidentally removing the stub)
    surfaces in the unit suite, not just in cross-driver
    integration smoke tests."""

    def test_executescript(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="executescript"):
            conn.executescript("CREATE TABLE t (id INT);")

    def test_interrupt(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="interrupt"):
            conn.interrupt()

    def test_set_authorizer(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="authorization"):
            conn.set_authorizer(lambda *a: 0)

    def test_set_progress_handler(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="progress"):
            conn.set_progress_handler(lambda: None, 1000)

    def test_set_trace_callback(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="trace"):
            conn.set_trace_callback(print)

    def test_total_changes(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="total_changes"):
            _ = conn.total_changes  # property, no parens

    def test_getlimit(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="getlimit"):
            conn.getlimit(0)

    def test_setlimit(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="setlimit"):
            conn.setlimit(0, 1024)

    def test_getconfig(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="getconfig"):
            conn.getconfig(0)

    def test_setconfig(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="setconfig"):
            conn.setconfig(0, True)

    def test_serialize(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="serialize"):
            conn.serialize()

    def test_deserialize(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="deserialize"):
            conn.deserialize(b"\x00")

    def test_blobopen(self, conn: dqlitedbapi.Connection) -> None:
        with pytest.raises(NotSupportedError, match="blob_open"):
            conn.blobopen("main", "t", "data", 1)


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


class TestAsyncCycle22StubFamily:
    """Async sibling of ``TestSyncCycle22StubFamily``."""

    @pytest.mark.asyncio
    async def test_executescript(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="executescript"):
            await aconn.executescript("CREATE TABLE t (id INT);")

    def test_interrupt(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="interrupt"):
            aconn.interrupt()

    def test_set_authorizer(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="authorization"):
            aconn.set_authorizer(lambda *a: 0)

    def test_set_progress_handler(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="progress"):
            aconn.set_progress_handler(lambda: None, 1000)

    def test_set_trace_callback(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="trace"):
            aconn.set_trace_callback(print)

    def test_total_changes(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="total_changes"):
            _ = aconn.total_changes  # property, no parens

    def test_getlimit(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="getlimit"):
            aconn.getlimit(0)

    def test_setlimit(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="setlimit"):
            aconn.setlimit(0, 1024)

    def test_getconfig(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="getconfig"):
            aconn.getconfig(0)

    def test_setconfig(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="setconfig"):
            aconn.setconfig(0, True)

    def test_serialize(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="serialize"):
            aconn.serialize()

    def test_deserialize(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="deserialize"):
            aconn.deserialize(b"\x00")

    def test_blobopen(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError, match="blob_open"):
            aconn.blobopen("main", "t", "data", 1)


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
