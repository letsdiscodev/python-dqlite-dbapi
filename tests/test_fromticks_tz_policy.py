"""Pin the tz policy for PEP 249 *FromTicks constructors.

The three constructors return naive local time (matching stdlib
``sqlite3.dbapi2``). The wire UNIXTIME decoder returns UTC-aware. The
asymmetry is documented in the function docstrings; this module pins
the naive-local result so a future change to UTC-aware surfaces
explicitly as a semver-meaningful break.
"""

from __future__ import annotations

import datetime

from dqlitedbapi.types import DateFromTicks, TimeFromTicks, TimestampFromTicks


def test_timestamp_from_ticks_is_naive_local_time() -> None:
    """``TimestampFromTicks(ticks)`` returns a naive (no tzinfo) datetime
    interpreted as local time, matching ``datetime.fromtimestamp(ticks)``.
    """
    ticks = 1700000000  # 2023-11-14 22:13:20 UTC
    result = TimestampFromTicks(ticks)
    assert isinstance(result, datetime.datetime)
    assert result.tzinfo is None
    # The local-time interpretation is ``datetime.fromtimestamp(ticks)``;
    # asserting equality pins the policy.
    assert result == datetime.datetime.fromtimestamp(ticks)


def test_date_from_ticks_is_naive_local_date() -> None:
    ticks = 1700000000
    result = DateFromTicks(ticks)
    assert isinstance(result, datetime.date)
    # Not a datetime — narrower type.
    assert not isinstance(result, datetime.datetime)
    assert result == datetime.date.fromtimestamp(ticks)


def test_time_from_ticks_is_naive_local_time() -> None:
    ticks = 1700000000
    result = TimeFromTicks(ticks)
    assert isinstance(result, datetime.time)
    assert result.tzinfo is None
    assert result == datetime.datetime.fromtimestamp(ticks).time()
