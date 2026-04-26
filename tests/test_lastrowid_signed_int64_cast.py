"""Pin: ``Cursor.lastrowid`` and ``Cursor.rowcount`` are re-cast to
signed ``int64`` after coming off the wire.

The wire codec exposes ``ResultResponse.last_insert_id`` and
``rows_affected`` as ``uint64``; the C dqlite server casts SQLite's
signed ``sqlite3_int64`` through ``(uint64_t)`` before sending. A
negative SQLite rowid (legal on ``INTEGER PRIMARY KEY`` tables)
arrives as ``2**64 - abs(rowid)``. Without the re-cast the dbapi
exposes the wire value unchanged, breaking parity with stdlib
``sqlite3.Cursor.lastrowid`` and the Go connector's
``int64(LastInsertID)`` contract.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _to_signed_int64


def test_zero_unchanged() -> None:
    assert _to_signed_int64(0) == 0


def test_small_positive_unchanged() -> None:
    assert _to_signed_int64(42) == 42


def test_max_signed_int64_unchanged() -> None:
    assert _to_signed_int64((1 << 63) - 1) == (1 << 63) - 1


def test_min_signed_int64_round_trip() -> None:
    """Wire encodes ``-2**63`` (INT64_MIN) as exactly ``2**63``."""
    assert _to_signed_int64(1 << 63) == -(1 << 63)


def test_minus_one_round_trip() -> None:
    """Wire encodes ``-1`` as ``2**64 - 1`` (UINT64_MAX)."""
    assert _to_signed_int64((1 << 64) - 1) == -1


def test_minus_five_round_trip() -> None:
    assert _to_signed_int64((1 << 64) - 5) == -5


@pytest.mark.parametrize(
    "wire_value,expected",
    [
        (0, 0),
        (1, 1),
        ((1 << 63) - 1, (1 << 63) - 1),
        (1 << 63, -(1 << 63)),
        ((1 << 64) - 1, -1),
        ((1 << 64) - 1000, -1000),
    ],
)
def test_round_trip_table(wire_value: int, expected: int) -> None:
    assert _to_signed_int64(wire_value) == expected
