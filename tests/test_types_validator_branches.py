"""Pin defensive validator branches in ``types.py`` so every input-validation
failure keeps funnelling through ``DataError`` rather than a raw stdlib error."""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import (
    TimeFromTicks,
    TimestampFromTicks,
    datetime_from_iso8601,
    format_utc_offset,
)


class TestFromTicksOverflowWrapping:
    def test_time_from_ticks_overflow_wraps_as_data_error(self) -> None:
        """Out-of-range tick: fromtimestamp's error wraps as ``DataError``."""
        with pytest.raises(DataError) as excinfo:
            TimeFromTicks(1e30)
        assert "Invalid timestamp ticks" in str(excinfo.value)

    def test_timestamp_from_ticks_overflow_wraps_as_data_error(self) -> None:
        with pytest.raises(DataError) as excinfo:
            TimestampFromTicks(1e30)
        assert "Invalid timestamp ticks" in str(excinfo.value)


class _LyingTzinfo(datetime.tzinfo):
    """``utcoffset`` returns an arbitrary offset, bypassing CPython's
    ``timezone()`` validation so ``format_utc_offset`` branches are reachable."""

    def __init__(self, offset: datetime.timedelta) -> None:
        self._offset = offset

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        return self._offset

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "_LyingTzinfo"

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return None


class TestFormatUtcOffsetRejection:
    def test_rejects_24h_or_greater_offset(self) -> None:
        """``|offset| >= 24h`` would emit a token ``fromisoformat`` rejects."""
        with pytest.raises(DataError) as excinfo:
            format_utc_offset(datetime.timedelta(hours=25))
        assert "tzinfo offset out of range" in str(excinfo.value)

    def test_rejects_negative_24h_offset(self) -> None:
        with pytest.raises(DataError) as excinfo:
            format_utc_offset(datetime.timedelta(hours=-25))
        assert "tzinfo offset out of range" in str(excinfo.value)

    def test_rejects_subsecond_precision_offset(self) -> None:
        """Sub-second offsets rejected: the wire is whole-second and truncation
        flips sign on negative fractional offsets."""
        with pytest.raises(DataError) as excinfo:
            format_utc_offset(datetime.timedelta(seconds=10, microseconds=500_000))
        assert "sub-second precision" in str(excinfo.value)


class TestDatetimeFromIso8601Edges:
    def test_empty_text_returns_none(self) -> None:
        """Empty ISO 8601 → ``None`` (pre-null-patch servers emit empty for NULL)."""
        assert datetime_from_iso8601("") is None

    def test_date_only_returns_datetime(self) -> None:
        """Bare ``YYYY-MM-DD`` widens to ``datetime`` regardless of parser path."""
        result = datetime_from_iso8601("2024-01-15")
        assert isinstance(result, datetime.datetime)
        assert result == datetime.datetime(2024, 1, 15, 0, 0, 0)

    def test_malformed_text_wraps_as_data_error(self) -> None:
        """Malformed ISO raises ``DataError``, not a raw ``ValueError``."""
        with pytest.raises(DataError) as excinfo:
            datetime_from_iso8601("not-a-date")
        assert "Cannot parse ISO 8601 datetime" in str(excinfo.value)
