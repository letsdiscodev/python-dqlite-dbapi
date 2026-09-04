"""A bare ``date`` widens to ``datetime`` (midnight) on round-trip: the encoder writes
``"YYYY-MM-DD"`` and the decoder parses it back as ``datetime.datetime``, matching pysqlite.
"""

from __future__ import annotations

import datetime

from dqlitedbapi.types import datetime_from_iso8601, iso8601_from_datetime


def test_bare_date_serializes_then_widens_to_datetime() -> None:
    d = datetime.date(2025, 1, 15)
    encoded = iso8601_from_datetime(d)
    assert encoded == "2025-01-15"
    decoded = datetime_from_iso8601(encoded)
    assert isinstance(decoded, datetime.datetime)
    assert decoded == datetime.datetime(2025, 1, 15, 0, 0)


def test_datetime_round_trips_without_widen() -> None:
    dt = datetime.datetime(2025, 1, 15, 10, 30, 45)
    encoded = iso8601_from_datetime(dt)
    decoded = datetime_from_iso8601(encoded)
    assert decoded == dt
