"""Pin defensive validator branches in ``types.py`` reported as
uncovered by ``pytest --cov``.

Lines covered by this file (pre-pragma):

- 148-149 — ``TimeFromTicks`` ``OverflowError``/``OSError``/``ValueError``
  wrapper to ``DataError``.
- 167-168 — ``TimestampFromTicks`` symmetric wrapper.
- 296     — ``_format_utc_offset`` rejects ``|offset| >= 24h``.
- 298     — ``_format_utc_offset`` rejects sub-second precision.
- 407     — ``_datetime_from_iso8601`` empty-text → ``None``.
- 420     — ``_datetime_from_iso8601`` date-only widens to
  ``datetime``.

Each branch is the input-validator boundary for a PEP 249 type
constructor or wire-decode helper. A regression that swallowed the
underlying stdlib exception or skipped the validator would silently
break the "all DB errors funnel through ``Error``" contract — pin
the wrapper.
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import (
    TimeFromTicks,
    TimestampFromTicks,
    _datetime_from_iso8601,
    _format_utc_offset,
)


class TestFromTicksOverflowWrapping:
    def test_time_from_ticks_overflow_wraps_as_data_error(self) -> None:
        """``TimeFromTicks`` must wrap the underlying
        ``OverflowError``/``OSError``/``ValueError`` from
        ``datetime.fromtimestamp(...)`` as ``DataError``. Driven with a
        tick that exceeds the platform timestamp range."""
        with pytest.raises(DataError) as excinfo:
            TimeFromTicks(1e30)
        assert "Invalid timestamp ticks" in str(excinfo.value)

    def test_timestamp_from_ticks_overflow_wraps_as_data_error(self) -> None:
        """Symmetric wrapper on the timestamp constructor."""
        with pytest.raises(DataError) as excinfo:
            TimestampFromTicks(1e30)
        assert "Invalid timestamp ticks" in str(excinfo.value)


class _LyingTzinfo(datetime.tzinfo):
    """Hand-rolled ``tzinfo`` whose ``utcoffset`` returns whatever was
    passed to the constructor — bypasses CPython's ``timezone()``
    validation so the ``_format_utc_offset`` validator branches become
    reachable (per the docstring at types.py:282-292)."""

    def __init__(self, offset: datetime.timedelta) -> None:
        self._offset = offset

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        return self._offset

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "_LyingTzinfo"

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return None


class TestFormatUtcOffsetRejection:
    def test_rejects_24h_or_greater_offset(self) -> None:
        """``_format_utc_offset`` must raise ``DataError`` for
        ``|offset| >= 24h`` — these would emit an out-of-range
        ``±HH:MM:SS`` token that ``datetime.fromisoformat`` rejects."""
        with pytest.raises(DataError) as excinfo:
            _format_utc_offset(datetime.timedelta(hours=25))
        assert "tzinfo offset out of range" in str(excinfo.value)

    def test_rejects_negative_24h_offset(self) -> None:
        """Magnitude check: a -25h offset must also be rejected."""
        with pytest.raises(DataError) as excinfo:
            _format_utc_offset(datetime.timedelta(hours=-25))
        assert "tzinfo offset out of range" in str(excinfo.value)

    def test_rejects_subsecond_precision_offset(self) -> None:
        """Sub-second offsets must be rejected — the dqlite wire
        encoding has whole-second resolution and the
        ``int(offset.total_seconds())`` truncation flips sign on
        negative fractional offsets (see types.py:289-292)."""
        with pytest.raises(DataError) as excinfo:
            _format_utc_offset(datetime.timedelta(seconds=10, microseconds=500_000))
        assert "sub-second precision" in str(excinfo.value)


class TestDatetimeFromIso8601Edges:
    def test_empty_text_returns_none(self) -> None:
        """Empty ISO 8601 → ``None``, matching pre-null-patch dqlite
        servers that emit empty text for NULL datetime cells. PEP 249
        NULL semantics."""
        assert _datetime_from_iso8601("") is None

    def test_date_only_returns_datetime(self) -> None:
        """A bare ``YYYY-MM-DD`` string is returned as a
        ``datetime.datetime`` (zero time component). On Python 3.11+
        this is reached via ``datetime.fromisoformat`` (which now
        accepts the date-only form natively) rather than the
        ``date.fromisoformat`` widen-fallback at types.py:420 — the
        fallback is structurally unreachable on supported Python
        versions and pragma'd accordingly. This test pins the
        behavioral contract regardless of which parser path executes."""
        result = _datetime_from_iso8601("2024-01-15")
        assert isinstance(result, datetime.datetime)
        assert result == datetime.datetime(2024, 1, 15, 0, 0, 0)

    def test_malformed_text_wraps_as_data_error(self) -> None:
        """Sanity / negative test: a malformed ISO string still raises
        ``DataError`` rather than letting the raw ``ValueError``
        escape. Driven here so the date-fallback ``except`` arm and
        the ``raise`` are both pinned together."""
        with pytest.raises(DataError) as excinfo:
            _datetime_from_iso8601("not-a-date")
        assert "Cannot parse ISO 8601 datetime" in str(excinfo.value)
