"""``datetime.time`` round-trips through the dbapi encoder/decoder pair.

``_iso8601_from_time`` supports binding ``datetime.time`` on the
parameter path; the decoder must accept ``HH:MM:SS[.ffffff][±HH:MM]``
on the result path so a time column (or an expression that returns
an ISO8601-tagged time literal) round-trips as ``datetime.time``
rather than raising DataError.
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.types import _datetime_from_iso8601


@pytest.mark.parametrize(
    "text,expected",
    [
        ("12:30:45", datetime.time(12, 30, 45)),
        ("00:00:00", datetime.time(0, 0, 0)),
        ("23:59:59.123456", datetime.time(23, 59, 59, 123456)),
    ],
)
def test_parses_naive_time(text: str, expected: datetime.time) -> None:
    assert _datetime_from_iso8601(text) == expected


def test_parses_aware_time() -> None:
    result = _datetime_from_iso8601("12:30:45+05:30")
    assert isinstance(result, datetime.time)
    assert result.hour == 12
    assert result.utcoffset() == datetime.timedelta(hours=5, minutes=30)


def test_malformed_still_raises_dataerror() -> None:
    from dqlitedbapi.exceptions import DataError

    with pytest.raises(DataError, match="Cannot parse ISO 8601"):
        _datetime_from_iso8601("not a time or a date")


def test_datetime_still_parses_as_datetime() -> None:
    result = _datetime_from_iso8601("2024-01-01T12:30:45")
    assert isinstance(result, datetime.datetime)
    assert result == datetime.datetime(2024, 1, 1, 12, 30, 45)


def test_date_still_widens_to_datetime() -> None:
    result = _datetime_from_iso8601("2024-01-01")
    assert isinstance(result, datetime.datetime)
    assert result == datetime.datetime(2024, 1, 1)
