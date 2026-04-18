"""Encoder-side tests for ``_iso8601_from_datetime``.

The decode side (``_datetime_from_iso8601``) has explicit error-path
tests; the encoder was only exercised by integration tests. These unit
tests pin the microsecond padding, tz-offset sign/magnitude, and the
``date``-vs-``datetime`` fall-through so a regression in any branch
surfaces quickly.
"""

import datetime

from dqlitedbapi.types import _iso8601_from_datetime


class TestIso8601FromDatetime:
    def test_datetime_without_microseconds(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0)
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00"

    def test_datetime_with_microseconds_zero_padded(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, microsecond=7)
        # Six-digit padded microseconds.
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00.000007"

    def test_datetime_with_six_digit_microseconds(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, microsecond=999999)
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00.999999"

    def test_naive_datetime_has_no_offset(self) -> None:
        """Naive datetimes emit the bare ISO string (no trailing offset)."""
        d = datetime.datetime(2025, 1, 1, 12, 0, 0)
        result = _iso8601_from_datetime(d)
        assert "+" not in result and result.count("-") == 2

    def test_utc_offset_emitted_as_plus_zero(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        result = _iso8601_from_datetime(d)
        assert result.endswith("+00:00")

    def test_positive_offset(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert _iso8601_from_datetime(d).endswith("+05:30")

    def test_negative_offset(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-5, minutes=-30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert _iso8601_from_datetime(d).endswith("-05:30")

    def test_negative_offset_with_microseconds(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-8))
        d = datetime.datetime(2025, 6, 15, 9, 30, 45, microsecond=42, tzinfo=tz)
        assert _iso8601_from_datetime(d) == "2025-06-15 09:30:45.000042-08:00"

    def test_date_only_takes_fall_through_branch(self) -> None:
        """``date`` (not ``datetime``) must produce the short YYYY-MM-DD
        form via the ``isoformat()`` fall-through, not the datetime
        branch that would call strftime with time components.
        """
        d = datetime.date(2025, 1, 1)
        assert _iso8601_from_datetime(d) == "2025-01-01"
