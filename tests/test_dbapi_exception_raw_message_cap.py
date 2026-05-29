"""Pin: every dbapi exception with a ``raw_message`` field caps it at
~4 KiB, so a hostile-server fan-out cannot produce multi-MB payloads."""

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
    """When ``raw_message`` is omitted, ``message`` is the source and is capped."""
    big = "Y" * 63_000
    e = cls(big)
    assert e.raw_message is not None
    assert len(e.raw_message) < 5000
    assert "raw_message truncated" in e.raw_message
