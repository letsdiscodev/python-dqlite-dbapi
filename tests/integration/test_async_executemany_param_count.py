"""Async ``executemany`` validates the per-row binding count locally,
matching the sync surface and stdlib ``sqlite3``.

PEP 249 / stdlib ``sqlite3`` raise ``ProgrammingError`` for a row whose
binding count disagrees with the statement's ``?`` placeholders, before
touching the database. The sync surface already does this; the async
``AsyncCursor.executemany`` previously sent a wrong-arity row to the
server and surfaced an ``InterfaceError`` (the server's SQLITE_RANGE)
after a wire round-trip — a different exception class and a worse
diagnostic than the sync sibling for the same deterministic caller bug.

These tests pin the async surface to the sync behaviour: the arity
check is local and raises ``ProgrammingError``, a single wrong-arity
row inserts nothing, and a ``str`` row still gets the sharp structural
diagnostic (a sequence was expected) rather than the misleading
per-character count — pinning that the structural reject runs before
the arity check.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.integration
async def test_async_executemany_wrong_arity_raises_programming_error(
    cluster_address: str,
) -> None:
    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS async_emany_arity")
        await cur.execute("CREATE TABLE async_emany_arity (a, b)")
        await conn.commit()

        # One value supplied for a two-placeholder statement. The sync
        # surface raises ``ProgrammingError`` locally; the async surface
        # must match rather than emitting an ``InterfaceError`` after a
        # wire round-trip.
        with pytest.raises(ProgrammingError, match="Incorrect number of bindings supplied"):
            await cur.executemany("INSERT INTO async_emany_arity (a, b) VALUES (?, ?)", [(3,)])

        # The arity check fires locally, before any wire write — nothing
        # was inserted.
        check = conn.cursor()
        await check.execute("SELECT count(*) FROM async_emany_arity")
        (count,) = await check.fetchone()  # type: ignore[misc]
        assert count == 0
    finally:
        cleanup = conn.cursor()
        await cleanup.execute("DROP TABLE IF EXISTS async_emany_arity")
        await conn.commit()
        await conn.close()


@pytest.mark.integration
async def test_async_executemany_str_row_gets_structural_diagnostic(
    cluster_address: str,
) -> None:
    # A single ``str`` row (two chars) against a one-placeholder
    # statement must surface the structural "sequence of values"
    # diagnostic, NOT the misleading "Incorrect number of bindings ...
    # uses 1 ... 2 supplied" per-character count. This pins that the
    # structural reject runs before the arity check, matching the sync
    # sibling's ordering.
    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS async_emany_str")
        await cur.execute("CREATE TABLE async_emany_str (a)")
        await conn.commit()

        with pytest.raises(ProgrammingError, match="sequence of values"):
            await cur.executemany("INSERT INTO async_emany_str (a) VALUES (?)", ["ab"])
    finally:
        cleanup = conn.cursor()
        await cleanup.execute("DROP TABLE IF EXISTS async_emany_str")
        await conn.commit()
        await conn.close()


@pytest.mark.integration
async def test_async_and_sync_executemany_agree_on_arity_error(
    cluster_address: str,
) -> None:
    # The async and sync surfaces must raise the same exception class
    # and message for an identical wrong-arity batch.
    from dqlitedbapi import connect

    sync_exc: ProgrammingError | None = None
    with connect(cluster_address, database="emany_parity") as sconn:
        scur = sconn.cursor()
        scur.execute("CREATE TABLE IF NOT EXISTS emany_parity (a, b)")
        scur.execute("DELETE FROM emany_parity")
        try:
            scur.executemany("INSERT INTO emany_parity (a, b) VALUES (?, ?)", [(3,)])
        except ProgrammingError as exc:
            sync_exc = exc
        scur.execute("DROP TABLE emany_parity")
    assert sync_exc is not None

    conn = await aconnect(cluster_address)
    async_exc: ProgrammingError | None = None
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS emany_parity_async")
        await cur.execute("CREATE TABLE emany_parity_async (a, b)")
        await conn.commit()
        try:
            await cur.executemany("INSERT INTO emany_parity_async (a, b) VALUES (?, ?)", [(3,)])
        except ProgrammingError as exc:
            async_exc = exc
    finally:
        cleanup = conn.cursor()
        await cleanup.execute("DROP TABLE IF EXISTS emany_parity_async")
        await conn.commit()
        await conn.close()

    assert async_exc is not None
    assert type(async_exc) is type(sync_exc)
    assert str(async_exc) == str(sync_exc)
