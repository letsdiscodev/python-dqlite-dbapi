"""Defense-in-depth pin: every dbapi exception class with a
``raw_message`` field caps that field at ~4 KiB.

The wire layer caps a single ``FailureResponse`` at ~64 KiB; without a
per-instance cap on the dbapi exception classes, a hostile-server fan-
out of 64 KiB messages flowing through cross-process pickled exception
graphs (Celery, ProcessPoolExecutor, structured-error capture) would
produce multi-MB payloads. Mirrors the cap discipline already in place
on the client-layer ``DqliteError`` base and the ``OperationalError``
display-message cap.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
)


@pytest.mark.parametrize(
    "cls",
    [
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
    ],
)
def test_raw_message_capped_at_4kb(cls: type) -> None:
    big = "X" * 63_000
    e = cls("trunc msg", code=42, raw_message=big)
    assert e.raw_message is not None
    assert len(e.raw_message) < 5000
    assert "raw_message truncated" in e.raw_message


@pytest.mark.parametrize(
    "cls",
    [
        InterfaceError,
        DatabaseError,
        DataError,
        OperationalError,
        IntegrityError,
        InternalError,
        ProgrammingError,
    ],
)
def test_short_raw_message_round_trips(cls: type) -> None:
    short = "ordinary error"
    e = cls("msg", code=1, raw_message=short)
    assert e.raw_message == short


@pytest.mark.parametrize(
    "cls",
    [InterfaceError, DatabaseError, OperationalError],
)
def test_default_raw_message_from_message_capped(cls: type) -> None:
    """When ``raw_message`` is omitted, the ``message`` argument is
    used as the source — the cap still applies."""
    big = "Y" * 63_000
    e = cls(big)
    assert e.raw_message is not None
    assert len(e.raw_message) < 5000
    assert "raw_message truncated" in e.raw_message
