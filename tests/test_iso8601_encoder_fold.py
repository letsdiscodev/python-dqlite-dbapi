"""Pin: the ISO 8601 encoder/decoder silently loses ``datetime.fold``
on naive datetimes during DST fall-back ambiguity.

ISO 8601 has no notation for ``fold``. ``datetime.fromisoformat()``
returns ``fold=0`` always. So a naive datetime with ``fold=1`` (the
"second" reading at a DST fall-back hour, e.g. 1:30 AM EST instead
of EDT) round-trips as ``fold=0`` — silently shifting an hour if a
downstream layer applies DST rules.

The encoder docstring already documents this as matching stdlib
``sqlite3``. Pinning the silent behavior here means a future
"tightening" cleanup (e.g. raising on naive ``fold=1``) is a
deliberate decision, not an accident.

For aware datetimes the offset disambiguates fall-back, so round-trip
is exact even with ``fold=1``.
"""

from __future__ import annotations

import datetime

from dqlitedbapi.types import _datetime_from_iso8601, _iso8601_from_datetime


def test_naive_fold1_silently_encodes_as_fold0() -> None:
    """Naive datetime with ``fold=1`` round-trips as ``fold=0``.

    This is a known stdlib parity limitation: ISO 8601 has no fold
    notation, so the second-reading at a DST fall-back hour is
    indistinguishable from the first on the wire.
    """
    dt = datetime.datetime(2024, 11, 3, 1, 30, fold=1)
    assert dt.fold == 1

    encoded = _iso8601_from_datetime(dt)
    decoded = _datetime_from_iso8601(encoded)

    assert isinstance(decoded, datetime.datetime)
    assert decoded == datetime.datetime(2024, 11, 3, 1, 30)
    # The fold bit is lost; document the loss as a pinned contract.
    assert decoded.fold == 0


def test_naive_fold0_round_trips() -> None:
    """Naive datetime with ``fold=0`` round-trips cleanly (regression
    pin)."""
    dt = datetime.datetime(2024, 11, 3, 1, 30, fold=0)
    encoded = _iso8601_from_datetime(dt)
    decoded = _datetime_from_iso8601(encoded)

    assert isinstance(decoded, datetime.datetime)
    assert decoded == dt
    assert decoded.fold == 0


def test_aware_fold1_offset_disambiguates() -> None:
    """For aware datetimes, the explicit UTC offset disambiguates DST
    fall-back; round-trip is exact even with ``fold=1``.

    This is the recommended migration path: callers worried about DST
    ambiguity should attach an explicit ``tzinfo`` rather than rely on
    naive wall-clock semantics.
    """
    tz_est = datetime.timezone(datetime.timedelta(hours=-5))
    dt = datetime.datetime(2024, 11, 3, 1, 30, fold=1, tzinfo=tz_est)

    encoded = _iso8601_from_datetime(dt)
    decoded = _datetime_from_iso8601(encoded)

    assert isinstance(decoded, datetime.datetime)
    assert decoded == dt
    assert decoded.utcoffset() == tz_est.utcoffset(None)
