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

from collections.abc import Callable, Iterator
from typing import Any

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError, NotSupportedError


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
    async def test_tpc_begin(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_begin(object())

    async def test_tpc_prepare(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_prepare()

    async def test_tpc_commit(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_commit()

    async def test_tpc_rollback(self, aconn: AsyncConnection) -> None:
        with pytest.raises(NotSupportedError):
            await aconn.tpc_rollback()

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

    async def test_executescript(self, aconn: AsyncConnection) -> None:
        # ``executescript`` is plain ``def`` (not ``async def``) so the
        # NotSupportedError fires on the call line — symmetric with the
        # sync sibling. See test_async_executescript_stub_raises_on_call
        # for the call-line-vs-await pin.
        with pytest.raises(NotSupportedError, match="executescript"):
            aconn.executescript("CREATE TABLE t (id INT);")

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


_TPC_INVOCATIONS: list[Callable[[Any], None]] = [
    lambda c: c.tpc_begin(("g", "b", 0)),
    lambda c: c.tpc_prepare(),
    lambda c: c.tpc_commit(),
    lambda c: c.tpc_rollback(),
    lambda c: c.tpc_recover(),
    lambda c: c.xid(1, "g", "b"),
]
_TPC_IDS = ["tpc_begin", "tpc_prepare", "tpc_commit", "tpc_rollback", "tpc_recover", "xid"]


class TestTpcStubsRouteThroughHelper:
    """Pin: the six TPC stubs on both sync and async ``Connection``
    route through the shared ``_stub_unsupported`` helper, which
    enforces PEP 249 §6.4 ``messages`` clear and the stdlib precedence
    of raising ``InterfaceError`` (closed) before ``NotSupportedError``
    (capability gap).

    Round 30 introduced ``_stub_unsupported`` and retrofitted the
    eighteen-plus stdlib-parity stubs (executescript, interrupt,
    serialize/deserialize, blobopen, create_function,
    set_authorizer, ...) but silently omitted the six TPC stubs that
    sit immediately above the helper definition in source order.
    Without this pin a regression that re-introduces a direct ``raise
    NotSupportedError`` in any TPC stub silently re-opens the same
    contract gap.
    """

    @pytest.mark.parametrize("invoke", _TPC_INVOCATIONS, ids=_TPC_IDS)
    def test_sync_tpc_clears_messages_before_raise(
        self,
        conn: dqlitedbapi.Connection,
        invoke: Callable[[dqlitedbapi.Connection], None],
    ) -> None:
        # Pre-load a stale message so the clear-or-not is observable.
        conn.messages.append((Exception, "stale"))
        with pytest.raises(NotSupportedError):
            invoke(conn)
        assert conn.messages == [], (
            "TPC stub must clear conn.messages per PEP 249 §6.4 before raising; "
            "route through _stub_unsupported helper"
        )

    @pytest.mark.parametrize("invoke", _TPC_INVOCATIONS, ids=_TPC_IDS)
    def test_sync_tpc_closed_raises_interface_error_not_notsupported(
        self,
        invoke: Callable[[dqlitedbapi.Connection], None],
    ) -> None:
        c = dqlitedbapi.connect("127.0.0.1:9999")
        # Mark closed without invoking close() (which would touch the
        # event loop / executor). Set the helper-checked attribute
        # ``_closed`` plus the finalizer-checked ``_closed_flag`` so
        # the GC suppression at fixture teardown stays consistent.
        c._closed = True
        c._closed_flag[0] = True
        # Stdlib precedence: closed-state diagnostic wins over the
        # capability-gap diagnostic. Without the helper, the stub
        # raises NotSupportedError regardless of state — masking the
        # closed-connection signal a cross-driver caller relies on.
        with pytest.raises(InterfaceError, match="closed"):
            invoke(c)

    @pytest.mark.parametrize("invoke", _TPC_INVOCATIONS, ids=_TPC_IDS)
    def test_async_tpc_clears_messages_before_raise(
        self,
        aconn: AsyncConnection,
        invoke: Callable[[AsyncConnection], None],
    ) -> None:
        # All six async stubs are plain ``def`` (the call-line raise
        # discipline) so no ``await`` is needed for the raise.
        aconn.messages.append((Exception, "stale"))
        with pytest.raises(NotSupportedError):
            invoke(aconn)
        assert aconn.messages == [], (
            "Async TPC stub must clear conn.messages per PEP 249 §6.4 before raising; "
            "route through _stub_unsupported helper"
        )

    @pytest.mark.parametrize("invoke", _TPC_INVOCATIONS, ids=_TPC_IDS)
    def test_async_tpc_closed_raises_interface_error_not_notsupported(
        self,
        invoke: Callable[[AsyncConnection], None],
    ) -> None:
        c = AsyncConnection("127.0.0.1:9999")
        c._closed = True
        with pytest.raises(InterfaceError, match="closed"):
            invoke(c)


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


async def test_async_close_clears_messages() -> None:
    """Same as the sync sibling, for AsyncConnection."""
    import contextlib as _contextlib

    c = AsyncConnection("127.0.0.1:9999")
    c.messages.append((Exception, "stale"))
    with _contextlib.suppress(Exception):
        await c.close()
    assert c.messages == []
