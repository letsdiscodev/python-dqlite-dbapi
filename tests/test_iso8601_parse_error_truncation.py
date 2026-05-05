"""Pin: ``_datetime_from_iso8601`` truncates the server-supplied
unparseable text before interpolation into the ``DataError``
message.

Without truncation, a hostile or compromised server returning a
64 MiB unparseable cell produces a ~128 MiB exception payload
(``{text!r}`` doubles via quoting), preserved across pickle via
``Error.__reduce__`` — log/pickle amplification DoS.
"""

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _datetime_from_iso8601


def test_iso8601_parse_error_truncates_long_payload() -> None:
    # 17 MiB of unparseable garbage that fails BOTH datetime and
    # time fromisoformat.
    garbage = "X" + "0" * (17 * 1024 * 1024 - 1)
    with pytest.raises(DataError) as ei:
        _datetime_from_iso8601(garbage)

    msg = str(ei.value)
    raw = ei.value.raw_message or ""

    # Bounded message length (well under 1 KiB):
    assert len(msg) < 1024, f"message length {len(msg)} exceeds bound"
    assert len(raw) < 1024, f"raw_message length {len(raw)} exceeds bound"
    # Truncation marker present:
    assert "truncated" in msg
    # Carries the original-length hint:
    assert str(17 * 1024 * 1024) in msg or "chars]" in msg


def test_iso8601_parse_error_short_payload_unchanged() -> None:
    # Short unparseable text passes through verbatim — no truncation.
    short = "not-iso-at-all"
    with pytest.raises(DataError) as ei:
        _datetime_from_iso8601(short)
    msg = str(ei.value)
    assert short in msg
    assert "truncated" not in msg


def test_iso8601_parse_error_preserves_dataerror_class() -> None:
    """Regression pin: the truncation must not change the exception
    class. Cross-driver code with ``except DataError:`` continues
    to work."""
    with pytest.raises(DataError):
        _datetime_from_iso8601("garbage")


def test_iso8601_parse_error_pickleable_with_truncated_message() -> None:
    """Round-trip an exception through pickle to confirm the
    truncated payload survives without ballooning."""
    import pickle

    garbage = "X" + "0" * (17 * 1024 * 1024 - 1)
    try:
        _datetime_from_iso8601(garbage)
    except DataError as e:
        rt = pickle.loads(pickle.dumps(e))
        assert isinstance(rt, DataError)
        assert len(str(rt)) < 1024
        return
    pytest.fail("expected DataError")
