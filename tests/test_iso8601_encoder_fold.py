"""ISO 8601 has no ``fold`` notation, so a naive ``fold=1`` datetime
round-trips as ``fold=0`` (matching stdlib sqlite3). Aware datetimes
disambiguate via the offset and round-trip exactly.
"""

from __future__ import annotations

import datetime

from dqlitedbapi.types import _datetime_from_iso8601, _iso8601_from_datetime


def test_naive_fold1_silently_encodes_as_fold0() -> None:
    """Naive datetime with ``fold=1`` round-trips as ``fold=0``."""
    dt = datetime.datetime(2024, 11, 3, 1, 30, fold=1)
    assert dt.fold == 1

    encoded = _iso8601_from_datetime(dt)
    decoded = _datetime_from_iso8601(encoded)

    assert isinstance(decoded, datetime.datetime)
    assert decoded == datetime.datetime(2024, 11, 3, 1, 30)
    assert decoded.fold == 0


def test_naive_fold0_round_trips() -> None:
    """Naive datetime with ``fold=0`` round-trips cleanly."""
    dt = datetime.datetime(2024, 11, 3, 1, 30, fold=0)
    encoded = _iso8601_from_datetime(dt)
    decoded = _datetime_from_iso8601(encoded)

    assert isinstance(decoded, datetime.datetime)
    assert decoded == dt
    assert decoded.fold == 0


def test_aware_fold1_offset_disambiguates() -> None:
    """For aware datetimes the offset disambiguates fall-back; round-trip is
    exact even with ``fold=1``."""
    tz_est = datetime.timezone(datetime.timedelta(hours=-5))
    dt = datetime.datetime(2024, 11, 3, 1, 30, fold=1, tzinfo=tz_est)

    encoded = _iso8601_from_datetime(dt)
    decoded = _datetime_from_iso8601(encoded)

    assert isinstance(decoded, datetime.datetime)
    assert decoded == dt
    assert decoded.utcoffset() == tz_est.utcoffset(None)
