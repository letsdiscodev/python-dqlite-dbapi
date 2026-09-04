"""Pin: leap-second strings (`23:59:60`) raise DataError on the ISO8601 decoder
path. Note `time.fromisoformat("24:00:00")` is accepted (midnight on 3.13+), so
it is NOT in the rejection set."""

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
    """Leap-second strings raise DataError; Python's datetime primitives don't
    model leap seconds."""
    with pytest.raises(DataError):
        _datetime_from_iso8601(leap_second_str)
