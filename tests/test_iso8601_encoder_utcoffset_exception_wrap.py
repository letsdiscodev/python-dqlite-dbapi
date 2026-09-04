"""A custom ``tzinfo`` whose ``utcoffset()`` raises must surface as
``DataError``, not the bare exception class (PEP 249 hierarchy contract).
"""

from __future__ import annotations

import datetime
from typing import Any

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import iso8601_from_datetime, iso8601_from_time


class _UtcOffsetRaisesType(datetime.tzinfo):
    def __init__(self, exc: type[BaseException], message: str = "synthesised") -> None:
        self._exc = exc
        self._message = message

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        raise self._exc(self._message)

    def tzname(self, dt: datetime.datetime | None) -> str | None:
        return "broken"

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return None


@pytest.mark.parametrize(
    "exc_type",
    [TypeError, ValueError],
)
def test_iso8601_from_datetime_wraps_utcoffset_typeerror_valueerror(
    exc_type: type[BaseException],
) -> None:
    tz = _UtcOffsetRaisesType(exc_type)
    dt = datetime.datetime(2024, 1, 1, tzinfo=tz)
    with pytest.raises(DataError, match="utcoffset"):
        iso8601_from_datetime(dt)


@pytest.mark.parametrize(
    "exc_type",
    [TypeError, ValueError],
)
def test_iso8601_from_time_wraps_utcoffset_typeerror_valueerror(
    exc_type: type[BaseException],
) -> None:
    tz = _UtcOffsetRaisesType(exc_type)
    t = datetime.time(12, 0, tzinfo=tz)
    with pytest.raises(DataError, match="utcoffset"):
        iso8601_from_time(t)


def test_iso8601_from_datetime_does_not_double_wrap_dataerror_from_offset_none() -> None:
    """The ``offset is None`` branch must not be double-wrapped by the
    try/except; the message stays the original ``returned None`` text."""

    class _Tz(datetime.tzinfo):
        def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
            return None

        def tzname(self, dt: datetime.datetime | None) -> str | None:
            return None

        def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
            return None

    dt = datetime.datetime(2024, 1, 1, tzinfo=_Tz())
    with pytest.raises(DataError, match="returned None"):
        iso8601_from_datetime(dt)


def test_iso8601_from_datetime_passes_through_naive_unaffected() -> None:
    """A naive datetime never calls utcoffset(), so the wrap is a no-op."""
    dt = datetime.datetime(2024, 1, 1, 12, 30, 45)
    result = iso8601_from_datetime(dt)
    assert result == "2024-01-01 12:30:45"


def test_iso8601_from_datetime_passes_through_utc_unaffected() -> None:
    """A UTC-aware datetime still gets its ``+00:00`` suffix."""
    dt = datetime.datetime(2024, 1, 1, 12, 30, 45, tzinfo=datetime.UTC)
    result = iso8601_from_datetime(dt)
    assert result.endswith("+00:00")


def _make_broken_tz_returning_non_timedelta() -> Any:
    """A tzinfo whose utcoffset returns a non-timedelta (drives a TypeError)."""

    class _BrokenReturnTz(datetime.tzinfo):
        def utcoffset(self, dt: datetime.datetime | None) -> Any:
            return "not-a-timedelta"

        def tzname(self, dt: datetime.datetime | None) -> str | None:
            return None

        def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
            return None

    return _BrokenReturnTz()


def test_iso8601_from_datetime_wraps_offset_non_timedelta() -> None:
    """A non-timedelta utcoffset() return surfaces as DataError."""
    dt = datetime.datetime(2024, 1, 1, tzinfo=_make_broken_tz_returning_non_timedelta())
    with pytest.raises(DataError):
        iso8601_from_datetime(dt)
