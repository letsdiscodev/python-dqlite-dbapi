"""The encoders catch ``Exception`` (not just ``TypeError``/``ValueError``)
from ``tzinfo.utcoffset`` so KeyError/AttributeError/OSError also wrap as
``DataError`` — but not ``BaseException``, so CancelledError still propagates.
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _iso8601_from_datetime, _iso8601_from_time


class _ZoneNotFoundTz(datetime.tzinfo):
    """tzinfo subclass that raises KeyError on utcoffset — mimics a
    missing zoneinfo entry."""

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        raise KeyError("Europe/Atlantis")

    def dst(self, dt: datetime.datetime | None) -> None:
        return None

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "ATL"


class _BrokenSubclassTz(datetime.tzinfo):
    """tzinfo subclass that raises AttributeError on utcoffset —
    mimics an incomplete subclass."""

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        raise AttributeError("forgot to implement")

    def dst(self, dt: datetime.datetime | None) -> None:
        return None

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "BR"


class _IoErrorTz(datetime.tzinfo):
    """tzinfo subclass that raises OSError on utcoffset — mimics a
    zoneinfo file read failure."""

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        raise OSError("zoneinfo read failed")

    def dst(self, dt: datetime.datetime | None) -> None:
        return None

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "IO"


@pytest.mark.parametrize("tz_cls", [_ZoneNotFoundTz, _BrokenSubclassTz, _IoErrorTz])
def test_datetime_wraps_tzinfo_exception_as_data_error(tz_cls: type[datetime.tzinfo]) -> None:
    """A tzinfo raising a non-(TypeError, ValueError) surfaces as DataError."""
    dt = datetime.datetime(2024, 1, 1, 12, 0, 0, tzinfo=tz_cls())
    with pytest.raises(DataError, match="utcoffset"):
        _iso8601_from_datetime(dt)


@pytest.mark.parametrize("tz_cls", [_ZoneNotFoundTz, _BrokenSubclassTz, _IoErrorTz])
def test_time_wraps_tzinfo_exception_as_data_error(tz_cls: type[datetime.tzinfo]) -> None:
    t = datetime.time(12, 0, 0, tzinfo=tz_cls())
    with pytest.raises(DataError, match="utcoffset"):
        _iso8601_from_time(t)
