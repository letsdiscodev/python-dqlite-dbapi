"""Pin: ``Error.sqlite_errorname`` mirrors stdlib
``sqlite3.Error.sqlite_errorname`` (Python 3.11+) — companion
to the already-shipped ``sqlite_errorcode``.
"""

from __future__ import annotations

import sqlite3

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import DatabaseError, IntegrityError, InterfaceError


def test_database_error_sqlite_errorname_returns_symbolic_name() -> None:
    err = DatabaseError("constraint failed", code=sqlite3.SQLITE_CONSTRAINT_UNIQUE)
    assert err.sqlite_errorname == "SQLITE_CONSTRAINT_UNIQUE"


def test_integrity_error_sqlite_errorname_returns_symbolic_name() -> None:
    err = IntegrityError(
        "constraint failed",
        code=sqlite3.SQLITE_CONSTRAINT_UNIQUE,
        raw_message="x",
    )
    assert err.sqlite_errorname == "SQLITE_CONSTRAINT_UNIQUE"


def test_interface_error_sqlite_errorname_returns_symbolic_name() -> None:
    err = InterfaceError(
        "library misuse",
        code=sqlite3.SQLITE_MISUSE,
        raw_message="x",
    )
    assert err.sqlite_errorname == "SQLITE_MISUSE"


def test_sqlite_errorname_returns_none_for_none_code() -> None:
    err = DatabaseError("no code", code=None)
    assert err.sqlite_errorname is None


def test_sqlite_errorname_returns_none_for_unknown_code() -> None:
    """dqlite-namespace codes (≥1000) have no upstream symbolic
    name, so the lookup returns None — caller-side code that
    branches on the name still works."""
    err = DatabaseError("dqlite-specific", code=1001)  # DQLITE_PROTO
    assert err.sqlite_errorname is None


def test_module_exports_errorname_alongside_errorcode() -> None:
    """The two stdlib accessors ship together on the code-bearing
    subclasses. Pin both presences."""
    assert hasattr(dqlitedbapi.DatabaseError, "sqlite_errorcode")
    assert hasattr(dqlitedbapi.DatabaseError, "sqlite_errorname")
    assert hasattr(dqlitedbapi.InterfaceError, "sqlite_errorcode")
    assert hasattr(dqlitedbapi.InterfaceError, "sqlite_errorname")


@pytest.mark.parametrize(
    ("code", "expected_name"),
    [
        (1, "SQLITE_ERROR"),
        (2, "SQLITE_INTERNAL"),
        (5, "SQLITE_BUSY"),
        (6, "SQLITE_LOCKED"),
        (7, "SQLITE_NOMEM"),
        (8, "SQLITE_READONLY"),
        (10, "SQLITE_IOERR"),
        (11, "SQLITE_CORRUPT"),
        (13, "SQLITE_FULL"),
        (14, "SQLITE_CANTOPEN"),
        (19, "SQLITE_CONSTRAINT"),
        (20, "SQLITE_MISMATCH"),
        (21, "SQLITE_MISUSE"),
        (24, "SQLITE_FORMAT"),
        (25, "SQLITE_RANGE"),
        (26, "SQLITE_NOTADB"),
    ],
)
def test_primary_error_codes_return_canonical_error_names(code: int, expected_name: str) -> None:
    """Primary SQLite error codes (0-28) must return the canonical
    error symbol — NOT an authorizer / opcode / limit constant
    that happens to share the numeric value.

    stdlib ``sqlite3`` exposes constants like ``SQLITE_CREATE_INDEX
    = 1`` (authorizer) alongside ``SQLITE_ERROR = 1`` (error code).
    A naive ``dir()`` walk produces the wrong name for ~half the
    primary codes when the authorizer / opcode / limit constant
    sorts alphabetically before the error symbol.
    """
    err = DatabaseError("test", code=code)
    assert err.sqlite_errorname == expected_name, (
        f"code {code} should yield {expected_name!r}, got {err.sqlite_errorname!r}"
    )
