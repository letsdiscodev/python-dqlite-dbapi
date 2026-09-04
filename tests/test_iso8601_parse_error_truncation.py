"""``datetime_from_iso8601`` truncates unparseable text before interpolating
it into the ``DataError`` message, preventing a log/pickle amplification DoS
from a hostile server returning a huge unparseable cell.
"""

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import datetime_from_iso8601


def test_iso8601_parse_error_truncates_long_payload() -> None:
    garbage = "X" + "0" * (17 * 1024 * 1024 - 1)
    with pytest.raises(DataError) as ei:
        datetime_from_iso8601(garbage)

    msg = str(ei.value)
    raw = ei.value.raw_message or ""

    assert len(msg) < 1024, f"message length {len(msg)} exceeds bound"
    assert len(raw) < 1024, f"raw_message length {len(raw)} exceeds bound"
    assert "truncated" in msg
    assert str(17 * 1024 * 1024) in msg or "chars]" in msg


def test_iso8601_parse_error_short_payload_unchanged() -> None:
    short = "not-iso-at-all"
    with pytest.raises(DataError) as ei:
        datetime_from_iso8601(short)
    msg = str(ei.value)
    assert short in msg
    assert "truncated" not in msg


def test_iso8601_parse_error_preserves_dataerror_class() -> None:
    """Truncation must not change the exception class."""
    with pytest.raises(DataError):
        datetime_from_iso8601("garbage")


def test_iso8601_parse_error_pickleable_with_truncated_message() -> None:
    """The truncated payload survives a pickle round-trip without ballooning."""
    import pickle

    garbage = "X" + "0" * (17 * 1024 * 1024 - 1)
    try:
        datetime_from_iso8601(garbage)
    except DataError as e:
        rt = pickle.loads(pickle.dumps(e))
        assert isinstance(rt, DataError)
        assert len(str(rt)) < 1024
        return
    pytest.fail("expected DataError")
