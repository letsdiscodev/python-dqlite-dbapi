"""Async ``executemany`` validates per-row binding count locally (``ProgrammingError`` before
any wire write), matching the sync surface and stdlib ``sqlite3``."""

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

        with pytest.raises(ProgrammingError, match="Incorrect number of bindings supplied"):
            await cur.executemany("INSERT INTO async_emany_arity (a, b) VALUES (?, ?)", [(3,)])

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
    # A ``str`` row must get the structural "sequence of values" diagnostic, not the misleading
    # per-character arity count: the structural reject runs before the arity check.
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
