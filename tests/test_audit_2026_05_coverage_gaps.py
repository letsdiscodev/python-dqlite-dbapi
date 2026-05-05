"""Pin: a batch of coverage gaps from the audit. Each test pins
one previously-uncovered branch so a regression is caught at PR
time. See ``issues/done/dbapi-*-coverage.md`` for per-gap
rationale."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import (
    DatabaseError,
    InterfaceError,
    OperationalError,
    ProgrammingError,
)

# ---------------- async fetchmany arraysize fallback (coverage)


@pytest.mark.asyncio
async def test_async_fetchmany_default_uses_arraysize() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    cur.arraysize = 3
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(i,) for i in range(5)]
    cur._row_index = 0
    rows = await cur.fetchmany()  # no arg → arraysize
    assert len(rows) == 3


@pytest.mark.asyncio
async def test_async_fetchmany_size_exceeds_remaining() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0
    rows = await cur.fetchmany(size=10)
    assert len(rows) == 2


# ---------------- async scroll mode validation (coverage)


@pytest.mark.asyncio
async def test_async_scroll_bad_mode_raises_programming_error() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError):
        await cur.scroll(0, mode="bad-mode")


# ---------------- Error.sqlite_errorcode + __repr__ (coverage)


def test_interface_error_sqlite_errorcode() -> None:
    e = InterfaceError("x", code=42)
    assert e.sqlite_errorcode == 42


def test_database_error_repr_includes_code() -> None:
    e = DatabaseError("y", code=11)
    r = repr(e)
    assert "code=11" in r


def test_operational_error_sqlite_errorcode_none_default() -> None:
    e = OperationalError("z")
    assert e.sqlite_errorcode is None


# ---------------- async cursor parent-GC ReferenceError → InterfaceError


@pytest.mark.asyncio
async def test_async_cursor_parent_gc_reraises_as_interface_error() -> None:
    """When the parent AsyncConnection is GC'd, the cursor's
    ``connection`` property must surface InterfaceError, not
    ReferenceError (which is outside dbapi.Error)."""
    import gc
    import weakref

    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    # Replace _connection with a proxy whose referent is dead.
    fake_referent = AsyncConnection("localhost:9001")
    proxy = weakref.proxy(fake_referent)
    cur._connection = proxy
    del fake_referent
    gc.collect()

    with pytest.raises(InterfaceError):
        _ = cur.connection


# ---------------- async cursor rownumber=None on no result set


@pytest.mark.asyncio
async def test_async_cursor_rownumber_no_result_set() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    assert cur.rownumber is None


# ---------------- empty-result description type-codes-empty fallback


def test_sync_description_empty_result_type_codes_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the wire response has columns but zero rows AND zero
    column_types, synthesise None for every type code so PEP 249's
    description tuple shape stays well-formed."""
    conn = Connection("localhost:9001", timeout=2.0)

    async def fake_query_raw_typed(*_args: object, **_kwargs: object):
        return (["a", "b"], [], [], [])

    fake_async = MagicMock()
    fake_async.query_raw_typed = fake_query_raw_typed
    monkeypatch.setattr(conn, "_get_async_connection", AsyncMock(return_value=fake_async))

    def fake_run_sync(coro: Any) -> Any:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr(conn, "_run_sync", fake_run_sync)

    cur = Cursor(conn)
    cur.execute("SELECT a, b FROM t WHERE 1=0")
    desc = cur.description
    assert desc is not None
    assert len(desc) == 2
    # Each type_code is None per the synthesised fallback.
    assert desc[0][1] is None
    assert desc[1][1] is None


# ---------------- row_factory applied in fetch* paths


def test_sync_row_factory_applied_in_fetchone() -> None:
    """row_factory transform must fire in fetchone."""
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(42,)]
    cur._row_index = 0
    cur.row_factory = lambda c, r: {"a": r[0]}
    row = cur.fetchone()
    assert row == {"a": 42}


def test_sync_row_factory_applied_in_fetchmany() -> None:
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0
    cur.row_factory = lambda c, r: ("x", r[0])
    rows = cur.fetchmany(2)
    assert rows == [("x", 1), ("x", 2)]


def test_sync_row_factory_applied_in_fetchall() -> None:
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0
    cur.row_factory = lambda c, r: ("y", r[0])
    rows = cur.fetchall()
    assert rows == [("y", 1), ("y", 2)]


# ---------------- setoutputsize / setinputsizes validation


def test_setoutputsize_rejects_non_int_sync() -> None:
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    with pytest.raises(ProgrammingError):
        cur.setoutputsize("not-an-int")  # type: ignore[arg-type]


def test_setoutputsize_rejects_bool_sync() -> None:
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    with pytest.raises(ProgrammingError):
        cur.setoutputsize(True)


def test_setinputsizes_rejects_non_sequence_sync() -> None:
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    with pytest.raises(ProgrammingError):
        cur.setinputsizes("not-a-sequence")


@pytest.mark.asyncio
async def test_setoutputsize_rejects_non_int_async() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError):
        cur.setoutputsize("not-an-int")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_setinputsizes_rejects_non_sequence_async() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError):
        cur.setinputsizes("not-a-sequence")


# ---------------- _call_client catch-all forward-compat (test pin)


def test_call_client_arms_cover_all_known_dqlite_error_subclasses() -> None:
    """Forward-compat regression test: every DqliteError subclass
    that exists today must have an explicit arm in ``_call_client``
    so a future addition is a deliberate, reviewed decision rather
    than a silent route-through-DatabaseError."""
    from dqliteclient.exceptions import (
        ClusterError,
        ClusterPolicyError,
        DqliteConnectionError,
    )
    from dqliteclient.exceptions import (
        DataError as ClientDataError,
    )
    from dqliteclient.exceptions import (
        DqliteError as ClientDqliteError,
    )
    from dqliteclient.exceptions import (
        InterfaceError as ClientInterfaceError,
    )
    from dqliteclient.exceptions import (
        OperationalError as ClientOperationalError,
    )
    from dqliteclient.exceptions import (
        ProtocolError as ClientProtocolError,
    )

    # Every concrete subclass of dqliteclient.DqliteError that is
    # NOT itself the base. _call_client has explicit arms for the
    # ones we expect to map to specific dbapi.Error subclasses.
    known = {
        ClientDataError,
        ClientInterfaceError,
        ClientOperationalError,
        ClientProtocolError,
        DqliteConnectionError,
        ClusterError,
        ClusterPolicyError,
    }
    actual = _all_subclasses(ClientDqliteError)
    missing = actual - known - {ClientDqliteError}
    assert not missing, (
        f"_call_client lacks explicit arms for: {missing}; "
        "add an arm or extend the test to cover the new class"
    )


def _all_subclasses(cls: type) -> set[type]:
    out: set[type] = set()
    for c in cls.__subclasses__():
        out.add(c)
        out.update(_all_subclasses(c))
    return out
