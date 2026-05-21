"""Pin: ``Connection.isolation_level`` setter accepts the stdlib
pre-3.12 accept-set (``{None, "", "DEFERRED", "IMMEDIATE",
"EXCLUSIVE"}``) as no-ops and rejects unknown strings as
``ProgrammingError`` (PEP 249 §7 caller-shape misuse).

The previous behaviour rejected all four implicit-BEGIN variants
with ``NotSupportedError``, breaking the canonical cross-driver
``dst.isolation_level = src.isolation_level`` idiom against a stdlib
source connection (whose default is ``""``, NOT ``None``).
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
    """All five stdlib values are accepted (case-insensitive for the
    string variants)."""
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        conn.isolation_level = value
    finally:
        conn._closed = True


def test_sync_isolation_level_rejects_unknown_string_as_programming_error() -> None:
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            conn.isolation_level = "SERIALIZABLE"
    finally:
        conn._closed = True


def test_sync_isolation_level_rejects_integer_as_programming_error() -> None:
    conn = dqlitedbapi.Connection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            conn.isolation_level = 42
    finally:
        conn._closed = True


def test_sync_isolation_level_stdlib_round_trip_idiom() -> None:
    """The canonical cross-driver ``dst.isolation_level =
    src.isolation_level`` idiom works against a stdlib source."""
    import sqlite3

    src = sqlite3.connect(":memory:")
    try:
        dst = dqlitedbapi.Connection("127.0.0.1:9999")
        try:
            dst.isolation_level = src.isolation_level  # stdlib default is ""
        finally:
            dst._closed = True
    finally:
        src.close()


def test_async_isolation_level_accepts_stdlib_default_empty_string() -> None:
    """Mirror pin on the async sibling."""
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        aconn.isolation_level = ""
    finally:
        aconn._closed = True


def test_async_isolation_level_rejects_unknown_string_as_programming_error() -> None:
    aconn = AsyncConnection("127.0.0.1:9999")
    try:
        with pytest.raises(dqlitedbapi.ProgrammingError, match="isolation_level"):
            aconn.isolation_level = "SERIALIZABLE"
    finally:
        aconn._closed = True
