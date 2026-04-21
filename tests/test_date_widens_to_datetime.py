"""Pin the documented ``date`` -> ``datetime`` widening on round-trip.

PEP 249 ``Date()`` constructs a ``datetime.date``. The encoder
serializes bare dates as ``"YYYY-MM-DD"`` (no time component). The
decoder's fallback parses the string with
``datetime.date.fromisoformat`` and returns a ``datetime.datetime``
at midnight — the value widens from date to datetime.

This matches pysqlite's default behaviour and is documented on the
``_datetime_from_iso8601`` docstring. Pinning the behaviour so a
future change surfaces as a deliberate semver-meaningful break.
"""

from __future__ import annotations

import datetime

from dqlitedbapi.types import _datetime_from_iso8601, _iso8601_from_datetime


def test_bare_date_serializes_then_widens_to_datetime() -> None:
    d = datetime.date(2025, 1, 15)
    encoded = _iso8601_from_datetime(d)
    assert encoded == "2025-01-15"
    decoded = _datetime_from_iso8601(encoded)
    assert isinstance(decoded, datetime.datetime)
    assert decoded == datetime.datetime(2025, 1, 15, 0, 0)


def test_datetime_round_trips_without_widen() -> None:
    dt = datetime.datetime(2025, 1, 15, 10, 30, 45)
    encoded = _iso8601_from_datetime(dt)
    decoded = _datetime_from_iso8601(encoded)
    assert decoded == dt
