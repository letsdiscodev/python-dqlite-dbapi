"""Pin: ``_classify_caller_sql`` rejects four caller-side mistakes
pre-flight with the right PEP 249 class:

1. NUL byte in SQL → ``ProgrammingError("the query contains a null
   character")`` (matches stdlib; previously surfaced as
   ``DataError`` via the wire encoder's EncodeError path — wrong
   class for a caller-supplied malformed-SQL bug).
2. Bare-semicolon SQL (``";"``, ``"; ; ;"``) → ``ProgrammingError("empty
   statement")`` (stdlib silently no-ops; we route to the same
   caller-side error class as empty / comment-only SQL).
3. ``cur.execute("SELECT ?")`` with no params (or
   ``parameters=None``) → ``ProgrammingError("Incorrect number of
   bindings supplied. ... uses 1, and there are 0 supplied.")``
   pre-flight (previously surfaced as ``InterfaceError`` after a wire
   RTT via SQLITE_RANGE).
4. Bind-value NUL → ``DataError`` (the WIRE-LAYER NUL-rejection
   path; stays as DataError per PEP 249's "problems with the
   processed data").

Plus the closed-state precedence pin on the
``Connection.executemany`` shortcut — closed connection raises
``InterfaceError`` BEFORE the outer-shape check.
"""

from __future__ import annotations

import pytest

import dqlitedbapi

# --- 1. NUL byte in SQL ---------------------------------------------------


def test_nul_byte_in_sql_raises_programming_error_pre_flight() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(dqlitedbapi.ProgrammingError, match="null character"):
        cur.execute("SELECT 1\x00")


def test_nul_byte_in_sql_pre_flight_skips_wire_rtt() -> None:
    """The pre-flight runs before any wire activity. A connection
    that has not yet been opened still raises ProgrammingError on
    the NUL guard (never reaches the dial / handshake)."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(dqlitedbapi.ProgrammingError, match="null character"):
        cur.execute("INSERT INTO t VALUES (\x00)")


# Bind-value NUL stays as DataError (the WIRE encoder's path).
# Pinned so a future "fix all NULs to ProgrammingError" doesn't
# over-correct.


def test_bind_value_with_nul_raises_data_error() -> None:
    """Pin: NUL in BIND VALUE remains DataError — PEP 249's
    "problems with the processed data". Distinct from NUL in SQL,
    which is ProgrammingError."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(dqlitedbapi.DataError):
        cur.execute("SELECT ?", ("hello\x00world",))


# --- 2. Bare-semicolon SQL ------------------------------------------------


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
    """Security-posture pin: a trailing ``;`` after a real statement
    IS detected as multi-statement (per
    ``_is_multi_statement``'s walker). The bare-semicolon fix must
    NOT relax this — a hostile/typoed ``';; DROP TABLE x;'`` shape
    relies on the multi-statement diagnostic."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    # Real statement + trailing junk = multi-statement (rejected).
    with pytest.raises(dqlitedbapi.ProgrammingError, match="one statement"):
        cur.execute("SELECT 1; DROP TABLE x")


# --- 3. execute("SELECT ?") with no params --------------------------------


def test_execute_one_qmark_zero_params_raises_pre_flight() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(
        dqlitedbapi.ProgrammingError,
        match="uses 1, and there are 0",
    ):
        cur.execute("SELECT ?")


def test_execute_one_qmark_empty_tuple_raises_pre_flight() -> None:
    """``parameters=()`` (explicit empty tuple) takes the same
    pre-flight path."""
    conn = dqlitedbapi.connect("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(
        dqlitedbapi.ProgrammingError,
        match="uses 1, and there are 0",
    ):
        cur.execute("SELECT ?", ())


# --- 4. Connection.executemany closed-state precedence --------------------


def test_executemany_shortcut_closed_state_precedes_outer_shape_sync() -> None:
    """Closed connection → InterfaceError, NOT shape ProgrammingError.
    Mirrors stdlib precedence: closed checks run before input checks."""
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
