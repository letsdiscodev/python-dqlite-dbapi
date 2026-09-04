"""Pin the round-trip of the combined axis: aware datetime + microseconds +
non-UTC offset, which existing per-axis tests miss (e.g. an offset/microseconds
ordering bug would slip through)."""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.types import datetime_from_iso8601, iso8601_from_datetime


@pytest.mark.parametrize(
    "offset",
    [
        datetime.timedelta(hours=5, minutes=30),
        datetime.timedelta(hours=-8),
        -datetime.timedelta(hours=3, minutes=30),
        datetime.timedelta(hours=14),
        datetime.timedelta(hours=-12),
    ],
    ids=["+05:30", "-08:00", "-03:30", "+14:00", "-12:00"],
)
def test_aware_microseconds_nonzero_offset_round_trip(
    offset: datetime.timedelta,
) -> None:
    """Round-trip must preserve aware tzinfo, non-UTC offset, and
    microseconds."""
    tz = datetime.timezone(offset)
    dt = datetime.datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=tz)

    encoded = iso8601_from_datetime(dt)
    decoded = datetime_from_iso8601(encoded)

    assert isinstance(decoded, datetime.datetime)
    assert decoded == dt, (
        f"round-trip mismatch with offset {offset}: encoded={encoded!r}, decoded={decoded!r}"
    )
    assert decoded.microsecond == 123456
    assert decoded.utcoffset() == offset


def test_microseconds_at_max_with_offset() -> None:
    """Boundary: microsecond=999999 (max) with non-UTC offset."""
    tz = datetime.timezone(datetime.timedelta(hours=-5))
    dt = datetime.datetime(2024, 6, 15, 23, 59, 59, 999999, tzinfo=tz)
    decoded = datetime_from_iso8601(iso8601_from_datetime(dt))
    assert isinstance(decoded, datetime.datetime)
    assert decoded == dt
    assert decoded.microsecond == 999999


def test_microseconds_at_min_with_offset() -> None:
    """Boundary: microsecond=1 (smallest non-zero) with offset."""
    tz = datetime.timezone(datetime.timedelta(hours=2))
    dt = datetime.datetime(2024, 6, 15, 12, 0, 0, 1, tzinfo=tz)
    decoded = datetime_from_iso8601(iso8601_from_datetime(dt))
    assert isinstance(decoded, datetime.datetime)
    assert decoded == dt
    assert decoded.microsecond == 1
