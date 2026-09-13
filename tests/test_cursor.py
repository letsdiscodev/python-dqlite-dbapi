"""Cursor: closed-state, operation type checks, positional-only args, pre-flight SQL checks."""

from __future__ import annotations

import asyncio
import gc

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi import ProgrammingError, connect
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import DataError, InterfaceError
from dqlitedbapi.types import adapt_bind_param


class TestCursorAfterExternalConnectionClose:
    """A live cursor whose connection was closed externally must raise InterfaceError."""

    def test_sync_cursor_execute_after_connection_close(self) -> None:
        from dqlitedbapi.connection import Connection

        conn = Connection("localhost:19001", timeout=2.0)
        cursor = conn.cursor()
        conn.close()

        with pytest.raises(InterfaceError):
            cursor.execute("SELECT 1")

        with pytest.raises(InterfaceError):
            cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

        # close() stays idempotent even after the connection is gone.
        cursor.close()
        cursor.close()

    def test_async_cursor_execute_after_connection_close(self) -> None:
        async def _run() -> None:
            conn = AsyncConnection("localhost:19001")
            # Don't connect — keep this a pure state-machine test.
            cursor = AsyncCursor(conn)
            await conn.close()

            with pytest.raises(InterfaceError):
                await cursor.execute("SELECT 1")

            with pytest.raises(InterfaceError):
                await cursor.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])

        asyncio.run(_run())


def _open_closed_cursor_with_gcd_connection() -> dqlitedbapi.Cursor:
    """Closed cursor holding a stale weakref.proxy to a GC'd Connection."""
    conn = Connection("localhost:9001", timeout=1.0)
    cur = conn.cursor()
    cur.close()
    del conn
    gc.collect()
    return cur


def test_fetchone_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.fetchone()


def test_fetchmany_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.fetchmany(10)


def test_fetchall_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.fetchall()


def test_execute_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.execute("SELECT 1")


def test_executemany_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])


def test_callproc_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.callproc("anything")


def test_nextset_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.nextset()


def test_scroll_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.scroll(0)


def test_executescript_on_closed_cursor_after_connection_gc_raises_dbapi_error() -> None:
    cur = _open_closed_cursor_with_gcd_connection()
    with pytest.raises(dqlitedbapi.Error):
        cur.executescript("SELECT 1")


def test_setinputsizes_on_closed_cursor_after_connection_gc_does_not_raise() -> None:
    """setinputsizes is a permissive no-op (PEP 249 §6.2); must not leak
    ReferenceError on a closed cursor with a GC'd parent."""
    cur = _open_closed_cursor_with_gcd_connection()
    cur.setinputsizes([None])


def test_setoutputsize_on_closed_cursor_after_connection_gc_does_not_raise() -> None:
    """setoutputsize mirrors setinputsizes: permissive no-op (PEP 249 §6.2)."""
    cur = _open_closed_cursor_with_gcd_connection()
    cur.setoutputsize(100)


@pytest.mark.parametrize("bad", [None, b"SELECT 1", 42, ["SELECT 1"]])
def test_sync_cursor_execute_rejects_non_str_operation(bad: object) -> None:
    conn = connect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            cur.execute(bad)  # type: ignore[arg-type]
    finally:
        cur.close()
        conn.close()


@pytest.mark.parametrize("bad", [None, b"SELECT 1", 42, ["SELECT 1"]])
async def test_async_cursor_execute_rejects_non_str_operation(bad: object) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            await cur.execute(bad)  # type: ignore[arg-type]
    finally:
        cur.close()
        await conn.close()


@pytest.mark.parametrize(
    "bad", [None, b"INSERT INTO t VALUES (?)", 42, ["INSERT INTO t VALUES (?)"]]
)
def test_sync_cursor_executemany_rejects_non_str_operation(bad: object) -> None:
    conn = connect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            cur.executemany(bad, [(1,), (2,)])  # type: ignore[arg-type]
    finally:
        cur.close()
        conn.close()


@pytest.mark.parametrize(
    "bad", [None, b"INSERT INTO t VALUES (?)", 42, ["INSERT INTO t VALUES (?)"]]
)
async def test_async_cursor_executemany_rejects_non_str_operation(bad: object) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            await cur.executemany(bad, [(1,), (2,)])  # type: ignore[arg-type]
    finally:
        cur.close()
        await conn.close()


@pytest.mark.parametrize(
    "bad", [None, b"INSERT INTO t VALUES (?)", 42, ["INSERT INTO t VALUES (?)"]]
)
def test_sync_connection_executemany_shortcut_rejects_non_str_operation(bad: object) -> None:
    """Connection.executemany shortcut inherits the cursor guard."""
    conn = connect("localhost:9001")
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            conn.executemany(bad, [(1,), (2,)])  # type: ignore[arg-type]
    finally:
        conn.close()


@pytest.mark.parametrize(
    "bad", [None, b"INSERT INTO t VALUES (?)", 42, ["INSERT INTO t VALUES (?)"]]
)
async def test_async_connection_executemany_shortcut_rejects_non_str_operation(
    bad: object,
) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect("localhost:9001")
    try:
        with pytest.raises(ProgrammingError, match="operation must be a str"):
            await conn.executemany(bad, [(1,), (2,)])  # type: ignore[arg-type]
    finally:
        await conn.close()


def test_sync_connection_execute_rejects_keyword_operation() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        with pytest.raises(TypeError):
            conn.execute(operation="SELECT 1")  # type: ignore[call-arg]
    finally:
        conn.close()


def test_sync_connection_executemany_rejects_keyword_operation() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        with pytest.raises(TypeError):
            conn.executemany(operation="INSERT INTO t VALUES (?)", seq_of_parameters=[(1,)])  # type: ignore[call-arg]
    finally:
        conn.close()


async def test_async_connection_execute_rejects_keyword_operation() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(TypeError):
        await aconn.execute(operation="SELECT 1")  # type: ignore[call-arg]


async def test_async_connection_executemany_rejects_keyword_operation() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(TypeError):
        await aconn.executemany(  # type: ignore[call-arg]
            operation="INSERT INTO t VALUES (?)",
            seq_of_parameters=[(1,)],
        )


def test_sync_cursor_execute_rejects_keyword_operation() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            with pytest.raises(TypeError):
                cur.execute(operation="SELECT 1")  # type: ignore[call-arg]
        finally:
            cur.close()
    finally:
        conn.close()


def test_sync_cursor_executemany_rejects_keyword_operation() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            with pytest.raises(TypeError):
                cur.executemany(  # type: ignore[call-arg]
                    operation="INSERT INTO t VALUES (?)",
                    seq_of_parameters=[(1,)],
                )
        finally:
            cur.close()
    finally:
        conn.close()


def test_text_with_embedded_nul_rejected_with_blob_hint() -> None:
    """The pre-encode guard fires, naming both 'embedded NUL' and the 'BLOB' workaround."""
    with pytest.raises(DataError) as excinfo:
        adapt_bind_param("hello\x00world")
    message = str(excinfo.value)
    assert "embedded NUL" in message
    assert "BLOB" in message
    # Offset is named so operators can identify the byte in a mixed payload.
    assert "offset 5" in message


def test_text_nul_rejection_is_in_dbapi_error_hierarchy() -> None:
    """The rejection inherits from ``dbapi.Error`` for uniform ``except dbapi.Error:`` catch."""
    with pytest.raises(dqlitedbapi.Error):
        adapt_bind_param("\x00")


def test_bytes_with_embedded_nul_unaffected() -> None:
    """The bytes/BLOB path round-trips NUL-containing data — the workaround the diagnostic
    points at; guard must not regress to a blanket reject."""
    payload = b"hello\x00world"
    assert adapt_bind_param(payload) is payload
    payload_ba = bytearray(b"hello\x00world")
    assert adapt_bind_param(payload_ba) is payload_ba
    payload_mv = memoryview(b"hello\x00world")
    assert adapt_bind_param(payload_mv) is payload_mv


def test_text_without_nul_unaffected() -> None:
    """Plain TEXT binds (no embedded NUL) pass through unchanged."""
    assert adapt_bind_param("hello world") == "hello world"
    assert adapt_bind_param("") == ""
    assert adapt_bind_param("unicode: 日本語") == "unicode: 日本語"


def test_adapter_producing_str_with_nul_also_rejected() -> None:
    """The guard runs AFTER the adapter chain, so an adapter producing a NUL-bearing ``str``
    is also caught rather than escaping into the wire encoder."""

    class _NeedsAdapter:
        pass

    dqlitedbapi.register_adapter(_NeedsAdapter, lambda _v: "leading\x00trailing")
    try:
        with pytest.raises(DataError, match="embedded NUL"):
            adapt_bind_param(_NeedsAdapter())
    finally:
        dqlitedbapi.unregister_adapter(_NeedsAdapter)


def test_nul_byte_in_sql_raises_programming_error_pre_flight() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(dqlitedbapi.ProgrammingError, match="null character"):
        cur.execute("SELECT 1\x00")


def test_nul_byte_in_sql_pre_flight_skips_wire_rtt() -> None:
    """Pre-flight runs before any wire activity: an unopened connection
    still raises on the NUL guard (never reaches dial/handshake)."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(dqlitedbapi.ProgrammingError, match="null character"):
        cur.execute("INSERT INTO t VALUES (\x00)")


def test_bind_value_with_nul_raises_data_error() -> None:
    """NUL in a bind value stays DataError (distinct from NUL in SQL,
    which is ProgrammingError); guards against over-correcting all NULs."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(dqlitedbapi.DataError):
        cur.execute("SELECT ?", ("hello\x00world",))


@pytest.mark.parametrize(
    "sql",
    [";", "; ;", "  ;  ;  ;  ", "/* */;-- foo\n;", "\t;\n;\n"],
)
def test_bare_semicolon_sql_raises_empty_statement(sql: str) -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(dqlitedbapi.ProgrammingError, match="empty statement"):
        cur.execute(sql)


def test_real_statement_with_trailing_semicolons_is_multi_statement() -> None:
    """Security pin: a trailing ``;`` after a real statement is still
    detected as multi-statement; the bare-semicolon fix must not relax
    this (``';; DROP TABLE x;'`` relies on the multi-statement reject)."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(dqlitedbapi.ProgrammingError, match="one statement"):
        cur.execute("SELECT 1; DROP TABLE x")


def test_execute_one_qmark_zero_params_raises_pre_flight() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(
        dqlitedbapi.ProgrammingError,
        match="uses 1, and there are 0",
    ):
        cur.execute("SELECT ?")


def test_execute_one_qmark_empty_tuple_raises_pre_flight() -> None:
    """``parameters=()`` takes the same pre-flight path."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(
        dqlitedbapi.ProgrammingError,
        match="uses 1, and there are 0",
    ):
        cur.execute("SELECT ?", ())


def test_executemany_shortcut_closed_state_precedes_outer_shape_sync() -> None:
    """Closed connection → InterfaceError, not shape ProgrammingError
    (closed checks run before input checks, per stdlib)."""
    conn = dqlitedbapi.connect("localhost:9001")
    conn.close()
    with pytest.raises(dqlitedbapi.InterfaceError, match="closed"):
        conn.executemany("INSERT INTO foo VALUES (?)", "not a seq")


def test_executemany_shortcut_closed_state_precedes_outer_shape_async() -> None:
    """Async sibling — same precedence."""
    import asyncio

    from dqlitedbapi.aio import AsyncConnection

    async def _run() -> None:
        conn = AsyncConnection("localhost:9001")
        await conn.close()
        with pytest.raises(dqlitedbapi.InterfaceError, match="closed"):
            await conn.executemany("INSERT INTO foo VALUES (?)", "not a seq")

    asyncio.run(_run())
