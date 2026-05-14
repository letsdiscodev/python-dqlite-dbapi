"""Pin: a custom ``tzinfo`` whose ``utcoffset()`` raises must surface
as ``DataError``, not the bare exception class.

The ISO8601 encoders at ``types.py`` already reject ``utcoffset()
returning None``. The complementary case — a
broken tzinfo that *raises* — was previously uncovered: bare
``RuntimeError`` / ``ValueError`` / ``TypeError`` from a hand-rolled
tzinfo would leak outside the PEP 249 ``dbapi.Error`` hierarchy,
breaking the contract every other path in ``types.py`` honours
(``_validate_ticks``, ``_datetime_from_unixtime``, ``Date`` /
``Time`` / ``Timestamp`` constructors).

Pin both encoder sites (``_iso8601_from_datetime`` and
``_iso8601_from_time``) and both kinds of stdlib-plausible exception
(``TypeError``, ``ValueError``) plus the broader ``Exception``
fallback.
"""

from __future__ import annotations

import datetime
from typing import Any

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _iso8601_from_datetime, _iso8601_from_time


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
        _iso8601_from_datetime(dt)


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
        _iso8601_from_time(t)


def test_iso8601_from_datetime_does_not_double_wrap_dataerror_from_offset_none() -> None:
    """Sibling pin: the existing ``offset is None`` branch already
    raises ``DataError``; the new try/except must not double-wrap
    that into a chained DataError(...) from DataError(...). The
    error message stays the original ``returned None`` text."""

    class _Tz(datetime.tzinfo):
        def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
            return None  # legitimate "I don't know" signal

        def tzname(self, dt: datetime.datetime | None) -> str | None:
            return None

        def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
            return None

    dt = datetime.datetime(2024, 1, 1, tzinfo=_Tz())
    with pytest.raises(DataError, match="returned None"):
        _iso8601_from_datetime(dt)


def test_iso8601_from_datetime_passes_through_naive_unaffected() -> None:
    """Negative pin: a naive datetime never enters the utcoffset()
    call, so the new wrap doesn't affect the naive path."""
    dt = datetime.datetime(2024, 1, 1, 12, 30, 45)
    result = _iso8601_from_datetime(dt)
    assert result == "2024-01-01 12:30:45"


def test_iso8601_from_datetime_passes_through_utc_unaffected() -> None:
    """Negative pin: a UTC-aware datetime gets ``+00:00`` suffix; new
    wrap doesn't disturb the happy path."""
    dt = datetime.datetime(2024, 1, 1, 12, 30, 45, tzinfo=datetime.UTC)
    result = _iso8601_from_datetime(dt)
    assert result.endswith("+00:00")


def _make_broken_tz_returning_non_timedelta() -> Any:
    """Test helper: a tzinfo whose utcoffset returns a bogus type.
    The stdlib's _format_utc_offset would TypeError on attribute access;
    pin that the wrap turns it into DataError too."""

    class _BrokenReturnTz(datetime.tzinfo):
        def utcoffset(self, dt: datetime.datetime | None) -> Any:
            return "not-a-timedelta"

        def tzname(self, dt: datetime.datetime | None) -> str | None:
            return None

        def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
            return None

    return _BrokenReturnTz()


def test_iso8601_from_datetime_wraps_offset_non_timedelta() -> None:
    """A tzinfo whose utcoffset() returns a non-timedelta drives a
    TypeError in datetime.replace() / arithmetic. Stdlib datetime
    actually validates the return value of utcoffset() — verify the
    behaviour and pin DataError on the surface."""
    dt = datetime.datetime(2024, 1, 1, tzinfo=_make_broken_tz_returning_non_timedelta())
    with pytest.raises(DataError):
        _iso8601_from_datetime(dt)
