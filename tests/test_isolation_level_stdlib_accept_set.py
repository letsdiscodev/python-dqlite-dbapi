"""``Connection.isolation_level`` accepts the stdlib accept-set
(``{None, "", "DEFERRED", "IMMEDIATE", "EXCLUSIVE"}``) as no-ops and rejects
unknown strings as ``ProgrammingError`` (stdlib's default is ``""``, not None).
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio.connection import AsyncConnection


@pytest.mark.parametrize(
    "value",
    [None, "", "DEFERRED", "IMMEDIATE", "EXCLUSIVE", "deferred", "Immediate"],
)
def test_sync_isolation_level_accepts_stdlib_value(value: object) -> None:
    """Stdlib values are accepted (case-insensitive for the string variants)."""
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        conn.isolation_level = value
    finally:
        conn.close()


def test_sync_isolation_level_rejects_unknown_string_as_programming_error() -> None:
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            conn.isolation_level = "SERIALIZABLE"
    finally:
        conn.close()


def test_sync_isolation_level_rejects_integer_as_programming_error() -> None:
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            conn.isolation_level = 42
    finally:
        conn.close()


def test_sync_isolation_level_stdlib_round_trip_idiom() -> None:
    """``dst.isolation_level = src.isolation_level`` works against a stdlib
    source."""
    import sqlite3

    src = sqlite3.connect(":memory:")
    try:
        dst = dqlitedbapi.Connection("127.0.0.1:9999")
        try:
            dst.isolation_level = src.isolation_level  # stdlib default is ""
        finally:
            dst.close()
    finally:
        src.close()


def test_async_isolation_level_accepts_stdlib_default_empty_string() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        aconn.isolation_level = ""
    finally:
        aconn.force_close_transport()


def test_async_isolation_level_rejects_unknown_string_as_programming_error() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            aconn.isolation_level = "SERIALIZABLE"
    finally:
        aconn.force_close_transport()
