"""``_classify_caller_sql`` rejects caller-side mistakes pre-flight with
the right PEP 249 class: NUL-in-SQL and bare-semicolon SQL and
zero-params-for-placeholder → ProgrammingError; bind-value NUL stays
DataError. Plus executemany closed-state precedence over shape check."""

from __future__ import annotations

import pytest

import dqlitedbapi


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
