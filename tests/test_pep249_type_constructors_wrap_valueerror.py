"""Pin: ``Date`` / ``Time`` / ``Timestamp`` wrap stdlib ``ValueError`` /
``TypeError`` as ``DataError`` per PEP 249 §7.

PEP 249 §7 mandates every error from a driver call subclass ``Error``.
Stdlib's ``datetime.{date,time,datetime}.__init__`` raise bare
``ValueError`` for invalid inputs (month=13, hour=25). Without
wrapping, ``try: dqlitedbapi.Date(2026, 13, 1) except
dqlitedbapi.Error:`` silently misses. Mirrors the discipline already
in ``DateFromTicks`` / ``TimeFromTicks`` / ``TimestampFromTicks``.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi import DataError


@pytest.mark.parametrize(
    "ctor,args",
    [
        (dqlitedbapi.Date, (2026, 13, 1)),
        (dqlitedbapi.Date, (2026, 1, 32)),
        (dqlitedbapi.Time, (25, 0, 0)),
        (dqlitedbapi.Time, (0, 60, 0)),
        (dqlitedbapi.Timestamp, (2026, 1, 1, 25, 0, 0)),
        (dqlitedbapi.Timestamp, (2026, 13, 1, 0, 0, 0)),
    ],
)
def test_invalid_args_raise_dataerror(ctor: object, args: tuple[object, ...]) -> None:
    with pytest.raises(DataError):
        ctor(*args)  # type: ignore[operator]
    # Must not leak bare ValueError outside dbapi.Error.
    with pytest.raises(dqlitedbapi.Error):
        ctor(*args)  # type: ignore[operator]


def test_valid_args_succeed() -> None:
    assert dqlitedbapi.Date(2026, 5, 5).day == 5
    assert dqlitedbapi.Time(12, 0, 0).hour == 12
    assert dqlitedbapi.Timestamp(2026, 5, 5, 12, 0, 0).year == 2026


def test_non_int_args_also_wrapped() -> None:
    """``datetime.date(year='x', ...)`` raises ``TypeError`` —
    mid-driver, must surface as ``DataError`` for PEP 249 §7."""
    with pytest.raises(DataError):
        dqlitedbapi.Date("2026", 5, 5)  # type: ignore[arg-type]
