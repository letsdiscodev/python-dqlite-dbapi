"""Pin: leap-second strings (`23:59:60`) raise ``DataError`` on the
ISO8601 decoder path.

Python's `datetime.fromisoformat` and `time.fromisoformat` reject
`seconds=60` with `ValueError`, which `_datetime_from_iso8601` catches
and wraps as `DataError`. The behaviour is currently correct, but no
regression test pins it; a future "leniency" patch (e.g. switching to
a permissive parser) would silently land.

Note that `time.fromisoformat("24:00:00")` is *accepted* (parses to
midnight on Python 3.13+), so it is NOT in the rejection set — the
audit checklist's reviewer correctly flagged the misconception.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _datetime_from_iso8601


@pytest.mark.parametrize(
    "leap_second_str",
    [
        "2016-12-31T23:59:60Z",  # canonical leap-second
        "2016-12-31T23:59:60+00:00",  # explicit UTC offset
        "2016-12-31 23:59:60Z",  # space separator
        "2024-06-30T23:59:60Z",  # mid-year leap second hypothetical
        "23:59:60",  # bare time form
        "23:59:60+00:00",  # bare time with offset
    ],
)
def test_iso8601_decoder_rejects_leap_second(leap_second_str: str) -> None:
    """Pin: leap-second strings raise DataError. Python's datetime
    primitives don't model leap seconds; the dbapi must surface this
    as DataError, not silently accept or wrap a non-DataError."""
    with pytest.raises(DataError):
        _datetime_from_iso8601(leap_second_str)
