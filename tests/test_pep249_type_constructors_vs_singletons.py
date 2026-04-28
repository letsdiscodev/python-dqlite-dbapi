"""Pin: PEP 249 §"Type Objects and Constructors" defines five
type-comparison singletons (STRING, BINARY, NUMBER, DATETIME, ROWID)
plus three **constructor functions** (Date, Time, Timestamp) that
build temporal values.

Date / Time / Timestamp are NOT type-comparison singletons. PEP 249
explicitly groups all date/time/timestamp values under DATETIME for
``description[i][1] == DATETIME`` comparisons. The stdlib ``sqlite3``
and ``psycopg2`` drivers follow this; we do too.

This file pins the contract so a future cleanup that mistakenly
introduces ``Date``-as-singleton (or removes the constructors)
fails a test rather than silently changing the public surface.
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
    """Date(...) is a constructor; the result is a ``datetime.date``."""
    result = Date(2024, 1, 1)
    assert result == datetime.date(2024, 1, 1)
    assert type(result) is datetime.date


def test_time_constructor_returns_stdlib_time() -> None:
    """Time(...) is a constructor; the result is a ``datetime.time``."""
    result = Time(12, 0, 0)
    assert result == datetime.time(12, 0, 0)
    assert type(result) is datetime.time


def test_timestamp_constructor_returns_stdlib_datetime() -> None:
    """Timestamp(...) is a constructor; the result is a
    ``datetime.datetime``."""
    result = Timestamp(2024, 1, 1, 12, 0, 0)
    assert result == datetime.datetime(2024, 1, 1, 12, 0, 0)
    assert type(result) is datetime.datetime


def test_constructors_are_callable_not_type_singletons() -> None:
    """Date / Time / Timestamp are callable functions, not the
    type-comparison singletons. They are NOT equal to DATETIME."""
    assert callable(Date)
    assert callable(Time)
    assert callable(Timestamp)
    assert Date is not DATETIME
    assert Time is not DATETIME
    assert Timestamp is not DATETIME


def test_pep249_type_singletons_are_pairwise_inequal() -> None:
    """Each type-comparison singleton wraps a distinct set of wire
    codes / declared-type names; pairwise comparison must be False.

    Note: NUMBER and ROWID both include ``ValueType.INTEGER``, but
    NUMBER additionally wraps FLOAT/BOOLEAN and many SQL names, while
    ROWID's set is {INTEGER, ROWID, INTEGER PRIMARY KEY}. The two
    ``values`` sets are distinct, so ``NUMBER == ROWID`` resolves to
    False via the ``isinstance(other, _DBAPIType)`` branch.
    """
    singletons = (STRING, BINARY, NUMBER, DATETIME, ROWID)
    for a, b in itertools.combinations(singletons, 2):
        assert a != b, (
            f"PEP 249 type singletons must be pairwise inequal; "
            f"{a!r} == {b!r} unexpectedly returned True"
        )
