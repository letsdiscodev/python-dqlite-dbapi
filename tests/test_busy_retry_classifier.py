"""Pin: ``is_busy_exception`` classifies BUSY retry by type+code, not substring."""

from __future__ import annotations

from dqliteclient.exceptions import DqliteConnectionError
from dqlitedbapi._busy_retry import is_busy_exception
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    IntegrityError,
    OperationalError,
    ProgrammingError,
)
from dqlitewire import SQLITE_BUSY


def test_operational_error_with_busy_code_is_busy() -> None:
    exc = OperationalError("database is locked", code=SQLITE_BUSY)
    assert is_busy_exception(exc) is True


def test_operational_error_with_non_busy_code_is_not_busy() -> None:
    """SQLITE_LOCKED (6) is a different condition; do not retry."""
    exc = OperationalError("locked", code=6)  # SQLITE_LOCKED
    assert is_busy_exception(exc) is False


def test_operational_error_with_none_code_is_not_busy() -> None:
    """Transport-shaped OperationalError (code=None) is handled by pool invalidation, not retry."""
    exc = OperationalError("connection closed", code=None)
    assert is_busy_exception(exc) is False


def test_dqlite_connection_error_with_busy_code_is_not_busy() -> None:
    """Checkpoint-BUSY rewrap produces DqliteConnectionError, which already triggered pool
    invalidation; retrying on the invalidated connection is wrong, so it must not classify."""
    exc = DqliteConnectionError(
        "raft-checkpoint reset the in-flight transaction",
        code=SQLITE_BUSY,
    )
    assert is_busy_exception(exc) is False


def test_integrity_error_with_busy_code_is_not_busy() -> None:
    """The retry gate is on type+code, not code alone."""
    exc = IntegrityError("constraint failed", code=SQLITE_BUSY)
    assert is_busy_exception(exc) is False


def test_programming_error_is_not_busy() -> None:
    exc = ProgrammingError("syntax error", code=1)  # SQLITE_ERROR
    assert is_busy_exception(exc) is False


def test_data_error_is_not_busy() -> None:
    exc = DataError("datatype mismatch", code=20)
    assert is_busy_exception(exc) is False


def test_bare_database_error_with_busy_code_is_not_busy() -> None:
    """The bare-DBE arm covers CORRUPT/FORMAT/NOTADB, not BUSY; catching it would mask
    an upstream misclassification."""
    exc = DatabaseError("bare DBE", code=SQLITE_BUSY)
    assert is_busy_exception(exc) is False


def test_non_exception_object_is_not_busy() -> None:
    """A non-Exception object is not classified."""
    assert is_busy_exception(BaseException("not an exception subclass")) is False
