"""PEP 249 type objects: STRING/BINARY/NUMBER/DATETIME/ROWID are comparison singletons;
Date/Time/Timestamp are constructor functions, not singletons (all grouped under DATETIME).
"""

from __future__ import annotations

import datetime
import itertools

from dqlitedbapi import (
    BINARY,
    DATETIME,
    NUMBER,
    ROWID,
    STRING,
    Date,
    Time,
    Timestamp,
)


def test_date_constructor_returns_stdlib_date() -> None:
    result = Date(2024, 1, 1)
    assert result == datetime.date(2024, 1, 1)
    assert type(result) is datetime.date


def test_time_constructor_returns_stdlib_time() -> None:
    result = Time(12, 0, 0)
    assert result == datetime.time(12, 0, 0)
    assert type(result) is datetime.time


def test_timestamp_constructor_returns_stdlib_datetime() -> None:
    result = Timestamp(2024, 1, 1, 12, 0, 0)
    assert result == datetime.datetime(2024, 1, 1, 12, 0, 0)
    assert type(result) is datetime.datetime


def test_constructors_are_callable_not_type_singletons() -> None:
    """Date/Time/Timestamp are callable functions, not equal to the DATETIME singleton."""
    assert callable(Date)
    assert callable(Time)
    assert callable(Timestamp)
    assert Date is not DATETIME
    assert Time is not DATETIME
    assert Timestamp is not DATETIME


def test_pep249_type_singletons_are_pairwise_inequal() -> None:
    """Each singleton wraps a distinct value set, so pairwise comparison is False.

    NUMBER and ROWID both include INTEGER but differ in the rest of their sets.
    """
    singletons = (STRING, BINARY, NUMBER, DATETIME, ROWID)
    for a, b in itertools.combinations(singletons, 2):
        assert a != b, (
            f"PEP 249 type singletons must be pairwise inequal; "
            f"{a!r} == {b!r} unexpectedly returned True"
        )
