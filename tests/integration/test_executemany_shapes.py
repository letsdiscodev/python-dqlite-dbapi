"""``executemany`` accepts any iterable of parameter rows and rejects the shapes that
would silently iterate wrongly (str, bytes, mappings, sets)."""

from __future__ import annotations

import contextlib
import os

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection

pytestmark = pytest.mark.integration

_ADDR = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")


def _exec(conn: dqlitedbapi.Connection, sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql)


async def _aexec(conn: AsyncConnection, sql: str) -> None:
    cur = conn.cursor()
    try:
        await cur.execute(sql)
    finally:
        cur.close()


def test_sync_executemany_rejects_str_outer_shape() -> None:
    conn = dqlitedbapi.connect(_ADDR)
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_outer")
        _exec(conn, "CREATE TABLE exm_outer (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer VALUES (?)", "abc")
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_outer")
        conn.close()


def test_sync_executemany_rejects_dict_outer_shape() -> None:
    conn = dqlitedbapi.connect(_ADDR)
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_outer2")
        _exec(conn, "CREATE TABLE exm_outer2 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer2 VALUES (?)", {"a": 1})
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_outer2")
        conn.close()


def test_sync_executemany_rejects_bytes_outer_shape() -> None:
    conn = dqlitedbapi.connect(_ADDR)
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_outer3")
        _exec(conn, "CREATE TABLE exm_outer3 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_outer3 VALUES (?)", b"abc")  # type: ignore[arg-type]
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_outer3")
        conn.close()


def test_sync_executemany_rejects_set_outer_shape() -> None:
    conn = dqlitedbapi.connect(_ADDR)
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_set")
        _exec(conn, "CREATE TABLE exm_set (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany("INSERT INTO exm_set VALUES (?)", {(1,), (2,)})
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_set")
        conn.close()


def test_sync_executemany_rejects_frozenset_outer_shape() -> None:
    conn = dqlitedbapi.connect(_ADDR)
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_fset")
        _exec(conn, "CREATE TABLE exm_fset (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            conn.executemany(
                "INSERT INTO exm_fset VALUES (?)",
                frozenset({(1,), (2,)}),
            )
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_fset")
        conn.close()


def test_sync_executemany_accepts_list_of_tuples() -> None:
    conn = dqlitedbapi.connect(_ADDR)
    try:
        _exec(conn, "DROP TABLE IF EXISTS exm_ok")
        _exec(conn, "CREATE TABLE exm_ok (n INTEGER)")
        cur = conn.executemany("INSERT INTO exm_ok VALUES (?)", [(1,), (2,)])
        cur.close()
    finally:
        with contextlib.suppress(Exception):
            _exec(conn, "DROP TABLE IF EXISTS exm_ok")
        conn.close()


async def test_async_executemany_rejects_str_outer_shape() -> None:
    conn = AsyncConnection(_ADDR)
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer")
        await _aexec(conn, "CREATE TABLE aexm_outer (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer VALUES (?)", "abc")
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer")
        await conn.close()


async def test_async_executemany_rejects_dict_outer_shape() -> None:
    conn = AsyncConnection(_ADDR)
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer2")
        await _aexec(conn, "CREATE TABLE aexm_outer2 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer2 VALUES (?)", {"a": 1})
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer2")
        await conn.close()


async def test_async_executemany_rejects_memoryview_outer_shape() -> None:
    conn = AsyncConnection(_ADDR)
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer3")
        await _aexec(conn, "CREATE TABLE aexm_outer3 (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany("INSERT INTO aexm_outer3 VALUES (?)", memoryview(b"abc"))
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_outer3")
        await conn.close()


async def test_async_executemany_rejects_set_outer_shape() -> None:
    conn = AsyncConnection(_ADDR)
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_set")
        await _aexec(conn, "CREATE TABLE aexm_set (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany(
                "INSERT INTO aexm_set VALUES (?)",
                {(1,), (2,)},
            )
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_set")
        await conn.close()


async def test_async_executemany_rejects_frozenset_outer_shape() -> None:
    conn = AsyncConnection(_ADDR)
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_fset")
        await _aexec(conn, "CREATE TABLE aexm_fset (n INTEGER)")
        with pytest.raises(dqlitedbapi.ProgrammingError, match="must be an iterable"):
            await conn.executemany(
                "INSERT INTO aexm_fset VALUES (?)",
                frozenset({(1,), (2,)}),
            )
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_fset")
        await conn.close()


async def test_async_executemany_accepts_list_of_tuples() -> None:
    conn = AsyncConnection(_ADDR)
    try:
        await _aexec(conn, "DROP TABLE IF EXISTS aexm_ok")
        await _aexec(conn, "CREATE TABLE aexm_ok (n INTEGER)")
        cur2 = await conn.executemany("INSERT INTO aexm_ok VALUES (?)", [(1,), (2,)])
        cur2.close()
    finally:
        with contextlib.suppress(Exception):
            await _aexec(conn, "DROP TABLE IF EXISTS aexm_ok")
        await conn.close()
