"""Pin: ``_format_utc_offset`` rejects non-``timedelta`` inputs with
``DataError`` rather than letting a bare ``AttributeError`` escape the
dbapi.Error tree (e.g. a buggy ``utcoffset()`` returning an int).
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _format_utc_offset, _iso8601_from_datetime


def test_format_utc_offset_rejects_int() -> None:
    """An int from a buggy ``utcoffset()`` is rejected with DataError."""
    with pytest.raises(DataError, match="non-timedelta"):
        _format_utc_offset(3600)  # type: ignore[arg-type]


def test_format_utc_offset_rejects_str() -> None:
    """A str is also rejected."""
    with pytest.raises(DataError, match="non-timedelta"):
        _format_utc_offset("+01:00")  # type: ignore[arg-type]


def test_iso8601_from_datetime_with_buggy_tzinfo_surfaces_dataerror() -> None:
    """End-to-end: a datetime with a non-timedelta utcoffset surfaces
    DataError at the dbapi boundary, no bare AttributeError leak."""

    class BuggyTzinfo(datetime.tzinfo):
        def utcoffset(self, dt: datetime.datetime | None) -> object:  # type: ignore[override]
            return 3600  # int instead of timedelta — caller bug

        def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
            return None

        def tzname(self, dt: datetime.datetime | None) -> str | None:
            return "BUGGY"

    bad = datetime.datetime(2026, 5, 1, 12, 0, 0, tzinfo=BuggyTzinfo())
    with pytest.raises(DataError):
        _iso8601_from_datetime(bad)


def test_format_utc_offset_accepts_real_timedelta() -> None:
    """Regression: a real timedelta still formats correctly."""
    result = _format_utc_offset(datetime.timedelta(hours=2))
    assert result == "+02:00"
