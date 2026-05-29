"""``setinputsizes`` accepts any Sequence ABC; str/bytes stay rejected as caller bugs."""

from __future__ import annotations

import collections
from collections.abc import Iterator

import pytest

import dqlitedbapi
from dqlitedbapi import ProgrammingError


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
