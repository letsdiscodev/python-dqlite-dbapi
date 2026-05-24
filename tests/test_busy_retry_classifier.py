"""Pin: ``_busy_retry.is_busy_exception`` correctly classifies which
exceptions trigger the busy retry — code-based, not substring-based.

Classification table:

  - ``OperationalError(code=SQLITE_BUSY)`` → True
  - ``OperationalError(code=SQLITE_LOCKED)`` → False
    (different code; this is a separate SQLite condition)
  - ``OperationalError(code=None)`` → False
    (transport-shaped error; not retryable as BUSY)
  - ``DqliteConnectionError(code=SQLITE_BUSY)`` → False
    (checkpoint-BUSY rewrap path; pool invalidation already handles)
  - ``IntegrityError``, ``ProgrammingError``, etc. → False
    (DatabaseError siblings; never retryable as BUSY)
  - ``DatabaseError(code=SQLITE_BUSY)`` (the bare class) → False
    (the bare-DBE arm covers CORRUPT/FORMAT/NOTADB, not BUSY)
"""

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
    """SQLITE_LOCKED (6) is a different condition with different
    semantics; do not retry."""
    exc = OperationalError("locked", code=6)  # SQLITE_LOCKED
    assert is_busy_exception(exc) is False


def test_operational_error_with_none_code_is_not_busy() -> None:
    """Transport-shaped OperationalError (e.g. "connection closed")
    has code=None; retry semantics for those are handled elsewhere
    (the pool invalidation path), not by busy_timeout."""
    exc = OperationalError("connection closed", code=None)
    assert is_busy_exception(exc) is False


def test_dqlite_connection_error_with_busy_code_is_not_busy() -> None:
    """The checkpoint-BUSY rewrap at
    dqliteclient/connection.py:2717 produces DqliteConnectionError,
    not OperationalError. The retry classifier MUST NOT catch this
    shape — the rewrap already triggered SA pool invalidation by
    design, and retrying on the now-invalidated connection is
    wrong."""
    exc = DqliteConnectionError(
        "raft-checkpoint reset the in-flight transaction",
        code=SQLITE_BUSY,
    )
    assert is_busy_exception(exc) is False


def test_integrity_error_with_busy_code_is_not_busy() -> None:
    """IntegrityError carrying SQLITE_BUSY (a code that wouldn't
    occur on IntegrityError in practice) MUST NOT be classified —
    the retry gate is on type+code, not code alone."""
    exc = IntegrityError("constraint failed", code=SQLITE_BUSY)
    assert is_busy_exception(exc) is False


def test_programming_error_is_not_busy() -> None:
    exc = ProgrammingError("syntax error", code=1)  # SQLITE_ERROR
    assert is_busy_exception(exc) is False


def test_data_error_is_not_busy() -> None:
    exc = DataError("datatype mismatch", code=20)
    assert is_busy_exception(exc) is False


def test_bare_database_error_with_busy_code_is_not_busy() -> None:
    """The bare-DBE arm covers CORRUPT/FORMAT/NOTADB (the slot-fatal
    code set), NOT BUSY. A BUSY code arriving on a bare DatabaseError
    would be misclassification at the dbapi layer — the busy retry
    classifier MUST NOT catch it (would mask the misclassification)."""
    exc = DatabaseError("bare DBE", code=SQLITE_BUSY)
    assert is_busy_exception(exc) is False


def test_non_exception_object_is_not_busy() -> None:
    """Belt-and-suspenders: a non-Exception object (e.g. a sentinel
    accidentally passed) is not classified."""
    assert is_busy_exception(BaseException("not an exception subclass")) is False
