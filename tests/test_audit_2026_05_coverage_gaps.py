"""Pins for previously-uncovered branches from the audit."""

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


async def test_async_fetchmany_default_uses_arraysize() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    cur.arraysize = 3
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(i,) for i in range(5)]
    cur._row_index = 0
    rows = await cur.fetchmany()
    assert len(rows) == 3


async def test_async_fetchmany_size_exceeds_remaining() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = [(1,), (2,)]
    cur._row_index = 0
    rows = await cur.fetchmany(size=10)
    assert len(rows) == 2


# Async scroll mode/value validation tests live in
# test_cursor_scroll_mode_validation.py.


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


async def test_async_cursor_parent_gc_reraises_as_interface_error() -> None:
    """Cursor.connection surfaces InterfaceError (not ReferenceError) when parent GC'd."""
    import gc
    import weakref

    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    fake_referent = AsyncConnection("localhost:9001")
    proxy = weakref.proxy(fake_referent)
    cur._connection = proxy
    del fake_referent
    gc.collect()

    with pytest.raises(InterfaceError):
        _ = cur.connection


async def test_async_cursor_rownumber_no_result_set() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    assert cur.rownumber is None


def test_sync_description_empty_result_type_codes_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Columns but zero rows and zero column_types: synthesise UNKNOWN type codes
    so PEP 249 §6.1.2's Type-Object contract holds (None is not a Type Object)."""
    from dqlitedbapi import UNKNOWN

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
    assert desc[0][1] is UNKNOWN
    assert desc[1][1] is UNKNOWN


def test_sync_row_factory_applied_in_fetchone() -> None:
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


async def test_setoutputsize_rejects_non_int_async() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError):
        cur.setoutputsize("not-an-int")  # type: ignore[arg-type]


async def test_setinputsizes_rejects_non_sequence_async() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError):
        cur.setinputsizes("not-a-sequence")


def test_setoutputsize_rejects_non_int_column_sync() -> None:
    """The ``column`` keyword has its own validation arm; pin str and bool cases."""
    conn = Connection("localhost:9001", timeout=2.0)
    cur = Cursor(conn)
    with pytest.raises(ProgrammingError, match="column"):
        cur.setoutputsize(10, "not-an-int")  # type: ignore[arg-type]
    with pytest.raises(ProgrammingError, match="column"):
        cur.setoutputsize(10, True)


async def test_setoutputsize_rejects_non_int_column_async() -> None:
    """Async sibling of ``test_setoutputsize_rejects_non_int_column_sync``."""
    conn = AsyncConnection("localhost:9001")
    cur = AsyncCursor(conn)
    with pytest.raises(ProgrammingError, match="column"):
        cur.setoutputsize(10, "not-an-int")  # type: ignore[arg-type]
    with pytest.raises(ProgrammingError, match="column"):
        cur.setoutputsize(10, True)


def test_call_client_arms_cover_all_known_dqlite_error_subclasses() -> None:
    """Forward-compat: every DqliteError subclass needs an explicit ``_call_client`` arm."""
    from dqliteclient.exceptions import (
        AmbiguousCommitError,
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

    # AmbiguousCommitError routes through the OperationalError arm, so it's
    # in ``known`` to avoid flagging it.
    known = {
        ClientDataError,
        ClientInterfaceError,
        ClientOperationalError,
        ClientProtocolError,
        DqliteConnectionError,
        ClusterError,
        ClusterPolicyError,
        AmbiguousCommitError,
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
