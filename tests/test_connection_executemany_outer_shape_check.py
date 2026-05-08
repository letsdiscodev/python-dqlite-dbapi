"""Pin: ``Connection.executemany`` and ``AsyncConnection.executemany``
shortcuts reject ``dict`` / ``str`` / ``bytes`` / ``bytearray`` /
``memoryview`` / ``set`` / ``frozenset`` outer ``seq_of_parameters``
with ``ProgrammingError``.

Without this pin a caller passing a single mapping or a string would
silently iterate over keys / characters, treating each as a parameter
set — almost certainly a caller bug. ``set`` and ``frozenset`` iterate
in non-deterministic order, so e.g. ``executemany({(1,), (2,)}, ...)``
would produce non-deterministic insert order. Stdlib has the same
hazard; this driver is stricter than stdlib at the connection-shortcut
layer (matching the existing ``_reject_non_sequence_params`` precedent
at the inner level).

The Mapping ABC at large is NOT rejected so a future user passing
``OrderedDict([(0, params0), (1, params1)])`` to iterate over values
is left alone — only ``dict``-typed values that look like a single
parameter set are denied.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_executemany_rejects_str_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        conn.cursor().execute("DROP TABLE IF EXISTS exm_outer")
        conn.cursor().execute("CREATE TABLE exm_outer (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer VALUES (?)", "abc")
    finally:
        conn.close()


def test_sync_executemany_rejects_dict_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        conn.cursor().execute("DROP TABLE IF EXISTS exm_outer2")
        conn.cursor().execute("CREATE TABLE exm_outer2 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer2 VALUES (?)", {"a": 1})
    finally:
        conn.close()


def test_sync_executemany_rejects_bytes_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        conn.cursor().execute("DROP TABLE IF EXISTS exm_outer3")
        conn.cursor().execute("CREATE TABLE exm_outer3 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer3 VALUES (?)", b"abc")  # type: ignore[arg-type]
    finally:
        conn.close()


def test_sync_executemany_rejects_set_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        conn.cursor().execute("DROP TABLE IF EXISTS exm_set")
        conn.cursor().execute("CREATE TABLE exm_set (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_set VALUES (?)", {(1,), (2,)})
    finally:
        conn.close()


def test_sync_executemany_rejects_frozenset_outer_shape() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        conn.cursor().execute("DROP TABLE IF EXISTS exm_fset")
        conn.cursor().execute("CREATE TABLE exm_fset (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany(
                "INSERT INTO exm_fset VALUES (?)",
                frozenset({(1,), (2,)}),
            )
    finally:
        conn.close()


def test_sync_executemany_accepts_list_of_tuples() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        conn.cursor().execute("DROP TABLE IF EXISTS exm_ok")
        conn.cursor().execute("CREATE TABLE exm_ok (n INTEGER)")
        cur = conn.executemany("INSERT INTO exm_ok VALUES (?)", [(1,), (2,)])
        cur.close()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_executemany_rejects_str_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS aexm_outer")
        await cur.execute("CREATE TABLE aexm_outer (n INTEGER)")
        await cur.close()
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer VALUES (?)", "abc")
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_async_executemany_rejects_dict_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS aexm_outer2")
        await cur.execute("CREATE TABLE aexm_outer2 (n INTEGER)")
        await cur.close()
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer2 VALUES (?)", {"a": 1})
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_async_executemany_rejects_memoryview_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS aexm_outer3")
        await cur.execute("CREATE TABLE aexm_outer3 (n INTEGER)")
        await cur.close()
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer3 VALUES (?)", memoryview(b"abc"))
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_async_executemany_rejects_set_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS aexm_set")
        await cur.execute("CREATE TABLE aexm_set (n INTEGER)")
        await cur.close()
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany(
                "INSERT INTO aexm_set VALUES (?)",
                {(1,), (2,)},
            )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_async_executemany_rejects_frozenset_outer_shape() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS aexm_fset")
        await cur.execute("CREATE TABLE aexm_fset (n INTEGER)")
        await cur.close()
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany(
                "INSERT INTO aexm_fset VALUES (?)",
                frozenset({(1,), (2,)}),
            )
    finally:
        await conn.close()


@pytest.mark.asyncio
async def test_async_executemany_accepts_list_of_tuples() -> None:
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS aexm_ok")
        await cur.execute("CREATE TABLE aexm_ok (n INTEGER)")
        await cur.close()
        cur2 = await conn.executemany("INSERT INTO aexm_ok VALUES (?)", [(1,), (2,)])
        await cur2.close()
    finally:
        await conn.close()
