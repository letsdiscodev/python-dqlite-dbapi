"""Pin the tz policy for PEP 249 *FromTicks constructors: naive local time
(matching stdlib sqlite3), unlike the UTC-aware wire UNIXTIME decoder.
Pins the result so a switch to UTC-aware surfaces as a semver break.
"""

from __future__ import annotations

import datetime

from dqlitedbapi.types import DateFromTicks, TimeFromTicks, TimestampFromTicks


def test_timestamp_from_ticks_is_naive_local_time() -> None:
    """``TimestampFromTicks`` returns a naive datetime, as local time."""
    ticks = 1700000000  # 2023-11-14 22:13:20 UTC
    result = TimestampFromTicks(ticks)
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is None
    assert result == datetime.datetime.fromtimestamp(ticks)


def test_date_from_ticks_is_naive_local_date() -> None:
    ticks = 1700000000
    result = DateFromTicks(ticks)
    assert isinstance(result, datetime.date)
    assert not isinstance(result, datetime.datetime)  # narrower type
    assert result == datetime.date.fromtimestamp(ticks)


def test_time_from_ticks_is_naive_local_time() -> None:
    ticks = 1700000000
    result = TimeFromTicks(ticks)
    assert isinstance(result, datetime.time)
    assert result.tzinfo is None
    assert result == datetime.datetime.fromtimestamp(ticks).time()
