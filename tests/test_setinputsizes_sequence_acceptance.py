"""Pin: ``setinputsizes`` accepts any ``collections.abc.Sequence``.

PEP 249 §6.2 specifies ``sizes`` as "a sequence". Stdlib
``sqlite3.Cursor.setinputsizes`` is documented as a no-op and accepts
any value. The dqlite cursor used to validate against the narrow
``(list, tuple)`` tuple, rejecting ``deque`` / ``range`` / custom
``Sequence`` subclasses that work on stdlib + psycopg2. Loosen to the
structural ``Sequence`` ABC; keep the ``str`` / ``bytes`` rejection so
a single-string "passed for N sizes" caller bug still surfaces as
``ProgrammingError``.
"""

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
    cursor.setinputsizes([10, None])  # no raise


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


def test_int_rejected(cursor: dqlitedbapi.Cursor) -> None:
    with pytest.raises(ProgrammingError, match="Sequence"):
        cursor.setinputsizes(42)  # type: ignore[arg-type]


def test_dict_rejected(cursor: dqlitedbapi.Cursor) -> None:
    with pytest.raises(ProgrammingError, match="Sequence"):
        cursor.setinputsizes({"a": 1})  # type: ignore[arg-type]
