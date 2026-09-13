"""Cursor.setinputsizes/setoutputsize acceptance semantics."""

from __future__ import annotations

import collections
from collections.abc import Iterator
from typing import Any

import pytest

import dqlitedbapi
from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio import AsyncConnection
from dqlitedbapi.connection import Connection


def test_sync_setinputsizes_none_is_noop() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            cur.setinputsizes(None)
        finally:
            cur.close()
    finally:
        conn.close()


def test_sync_setinputsizes_int_still_rejected() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            with pytest.raises(ProgrammingError):
                cur.setinputsizes(5)  # type: ignore[arg-type]
        finally:
            cur.close()
    finally:
        conn.close()


async def test_async_setinputsizes_none_is_noop() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    try:
        cur.setinputsizes(None)
    finally:
        cur.close()


async def test_async_setinputsizes_int_still_rejected() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError):
            cur.setinputsizes(5)  # type: ignore[arg-type]
    finally:
        cur.close()


@pytest.fixture
def cursor() -> Iterator[dqlitedbapi.Cursor]:
    conn = dqlitedbapi.connect("localhost:9001", timeout=2.0)
    cur = conn.cursor()
    yield cur
    conn.close()


def test_list_accepted(cursor: dqlitedbapi.Cursor) -> None:
    cursor.setinputsizes([10, None])


def test_tuple_accepted(cursor: dqlitedbapi.Cursor) -> None:
    cursor.setinputsizes((10, None))


def test_deque_accepted(cursor: dqlitedbapi.Cursor) -> None:
    cursor.setinputsizes(collections.deque([10, None]))


def test_range_accepted(cursor: dqlitedbapi.Cursor) -> None:
    cursor.setinputsizes(range(3))


def test_str_rejected(cursor: dqlitedbapi.Cursor) -> None:
    with pytest.raises(ProgrammingError, match="size hints"):
        cursor.setinputsizes("ab")


def test_bytes_rejected(cursor: dqlitedbapi.Cursor) -> None:
    with pytest.raises(ProgrammingError, match="size hints"):
        cursor.setinputsizes(b"ab")


def test_bytearray_rejected(cursor: dqlitedbapi.Cursor) -> None:
    with pytest.raises(ProgrammingError, match="size hints"):
        cursor.setinputsizes(bytearray(b"ab"))


def test_memoryview_rejected(cursor: dqlitedbapi.Cursor) -> None:
    """memoryview satisfies Sequence so it needs explicit rejection alongside str/bytes."""
    with pytest.raises(ProgrammingError, match="size hints"):
        cursor.setinputsizes(memoryview(b"ab"))


def test_int_rejected(cursor: dqlitedbapi.Cursor) -> None:
    with pytest.raises(ProgrammingError, match="Sequence"):
        cursor.setinputsizes(42)  # type: ignore[arg-type]


def test_dict_rejected(cursor: dqlitedbapi.Cursor) -> None:
    with pytest.raises(ProgrammingError, match="Sequence"):
        cursor.setinputsizes({"a": 1})  # type: ignore[arg-type]


@pytest.mark.parametrize("bad_sizes", [42, {"k": 1}])
def test_sync_setinputsizes_rejects_non_sequence_iterable(bad_sizes: Any) -> None:
    conn = Connection("localhost:9001", timeout=2.0)
    cur = conn.cursor()
    with pytest.raises(ProgrammingError, match="expects a Sequence"):
        cur.setinputsizes(bad_sizes)


@pytest.mark.parametrize("bad_sizes", [42, {"k": 1}])
async def test_async_setinputsizes_rejects_non_sequence_iterable(bad_sizes: Any) -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(ProgrammingError, match="expects a Sequence"):
        cur.setinputsizes(bad_sizes)


def test_sync_setinputsizes_accepts_deque_and_range() -> None:
    """Sequence-ABC dispatch accepts deque/range (psycopg2/stdlib parity)."""
    conn = Connection("localhost:9001", timeout=2.0)
    cur = conn.cursor()
    cur.setinputsizes(collections.deque([1, 2]))
    cur.setinputsizes(range(3))


async def test_async_setinputsizes_accepts_deque_and_range() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    cur.setinputsizes(collections.deque([1, 2]))
    cur.setinputsizes(range(3))


def test_sync_setinputsizes_rejects_memoryview() -> None:
    """memoryview satisfies Sequence so it needs explicit rejection alongside str/bytes."""
    conn = Connection("localhost:9001", timeout=2.0)
    cur = conn.cursor()
    with pytest.raises(ProgrammingError, match="size hints"):
        cur.setinputsizes(memoryview(b"ab"))


async def test_async_setinputsizes_rejects_memoryview() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    with pytest.raises(ProgrammingError, match="size hints"):
        cur.setinputsizes(memoryview(b"ab"))


def test_sync_setoutputsize_none_is_noop() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            cur.setoutputsize(None)
            cur.setoutputsize(None, None)
        finally:
            cur.close()
    finally:
        conn.close()


def test_sync_setoutputsize_str_still_rejected() -> None:
    conn = Connection("localhost:9001")
    try:
        cur = conn.cursor()
        try:
            with pytest.raises(ProgrammingError):
                cur.setoutputsize("five")  # type: ignore[arg-type]
        finally:
            cur.close()
    finally:
        conn.close()


async def test_async_setoutputsize_none_is_noop() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    try:
        cur.setoutputsize(None)
        cur.setoutputsize(None, None)
    finally:
        cur.close()


async def test_async_setoutputsize_str_still_rejected() -> None:
    conn = AsyncConnection("localhost:9001")
    cur = conn.cursor()
    try:
        with pytest.raises(ProgrammingError):
            cur.setoutputsize("five")  # type: ignore[arg-type]
    finally:
        cur.close()
