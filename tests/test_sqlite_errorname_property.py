"""Pin: ``Error.sqlite_errorname`` mirrors stdlib
``sqlite3.Error.sqlite_errorname`` (Python 3.11+) — companion
to the already-shipped ``sqlite_errorcode``.
"""

from __future__ import annotations

import sqlite3

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
