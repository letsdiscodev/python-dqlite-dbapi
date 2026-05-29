"""``Error.sqlite_errorname`` mirrors stdlib sqlite3 (Python 3.11+)."""

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
    """dqlite-namespace codes (>=1000) have no upstream symbolic name; lookup returns None."""
    err = DatabaseError("dqlite-specific", code=1001)  # DQLITE_PROTO
    assert err.sqlite_errorname is None


@pytest.mark.parametrize(
    "code",
    [
        1002,  # DQLITE_NOTFOUND — collides with stdlib SQLITE_DBCONFIG_ENABLE_FKEY
        1005,  # DQLITE_PARSE — collides with stdlib SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION
    ],
)
def test_sqlite_errorname_none_for_namespace_codes_colliding_with_dbconfig(code: int) -> None:
    """dqlite codes return None even when colliding with stdlib SQLITE_DBCONFIG_* opcodes."""
    assert DatabaseError("namespace", code=code).sqlite_errorname is None


@pytest.mark.parametrize("code", [10250, 10506, 8202, 8458])
def test_sqlite_errorname_none_for_leader_change_codes(code: int) -> None:
    """Leader-change codes return None even though legacy 8202/8458 collide with IOERR codes."""
    assert DatabaseError("leader", code=code).sqlite_errorname is None


def test_module_exports_errorname_alongside_errorcode() -> None:
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
    """Primary codes return the error symbol, not an authorizer/opcode constant
    sharing the value (e.g. SQLITE_CREATE_INDEX == SQLITE_ERROR == 1)."""
    err = DatabaseError("test", code=code)
    assert err.sqlite_errorname == expected_name, (
        f"code {code} should yield {expected_name!r}, got {err.sqlite_errorname!r}"
    )
