"""Encoder-side tests for ``_iso8601_from_datetime``.

The decode side (``_datetime_from_iso8601``) has explicit error-path
tests; the encoder was only exercised by integration tests. These unit
tests pin the microsecond padding, tz-offset sign/magnitude, and the
``date``-vs-``datetime`` fall-through so a regression in any branch
surfaces quickly.
"""

import datetime

from dqlitedbapi.types import _iso8601_from_datetime


class TestIso8601FromDatetime:
    def test_datetime_without_microseconds(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0)
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00"

    def test_datetime_with_microseconds_zero_padded(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, microsecond=7)
        # Six-digit padded microseconds.
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00.000007"

    def test_datetime_with_six_digit_microseconds(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, microsecond=999999)
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00.999999"

    def test_naive_datetime_has_no_offset(self) -> None:
        """Naive datetimes emit the bare ISO string (no trailing offset)."""
        d = datetime.datetime(2025, 1, 1, 12, 0, 0)
        result = _iso8601_from_datetime(d)
        assert "+" not in result and result.count("-") == 2

    def test_utc_offset_emitted_as_plus_zero(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        result = _iso8601_from_datetime(d)
        assert result.endswith("+00:00")

    def test_positive_offset(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert _iso8601_from_datetime(d).endswith("+05:30")

    def test_negative_offset(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-5, minutes=-30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert _iso8601_from_datetime(d).endswith("-05:30")

    def test_negative_offset_with_microseconds(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-8))
        d = datetime.datetime(2025, 6, 15, 9, 30, 45, microsecond=42, tzinfo=tz)
        assert _iso8601_from_datetime(d) == "2025-06-15 09:30:45.000042-08:00"

    def test_date_only_takes_fall_through_branch(self) -> None:
        """``date`` (not ``datetime``) must produce the short YYYY-MM-DD
        form via the ``isoformat()`` fall-through, not the datetime
        branch that would call strftime with time components.
        """
        d = datetime.date(2025, 1, 1)
        assert _iso8601_from_datetime(d) == "2025-01-01"

    def test_sub_minute_offset_preserves_seconds(self) -> None:
        """Historical tz data (some African, Irish, and Pacific zones in
        the IANA database) carry LMT offsets with sub-minute precision.
        ``datetime.fromisoformat`` on Python 3.11+ round-trips
        ``±HH:MM:SS`` offsets; the encoder must emit them so the
        round-trip through dqlite's TEXT column preserves the offset
        exactly.
        """
        tz = datetime.timezone(datetime.timedelta(minutes=5, seconds=30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00+00:05:30"

    def test_sub_minute_negative_offset_preserves_seconds(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-1, seconds=-15))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00-01:00:15"

    def test_whole_minute_offset_stays_hh_mm(self) -> None:
        """Common tz offsets (whole minutes) must still emit ``±HH:MM``
        exactly — byte-identical with the pre-fix encoder. Only
        sub-minute offsets get the widened ``±HH:MM:SS`` form."""
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert _iso8601_from_datetime(d) == "2025-01-01 12:00:00+05:30"

    def test_sub_minute_offset_round_trips_through_decoder(self) -> None:
        from dqlitedbapi.types import _datetime_from_iso8601

        tz = datetime.timezone(datetime.timedelta(minutes=5, seconds=30))
        original = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        encoded = _iso8601_from_datetime(original)
        decoded = _datetime_from_iso8601(encoded)
        assert decoded == original
        assert decoded is not None and decoded.utcoffset() == original.utcoffset()


class TestIso8601FromTime:
    """Encoder for ``datetime.time`` values. The DB-API ``Time()`` and
    ``TimeFromTicks()`` constructors return ``datetime.time``; bind
    parameters pass through ``_convert_bind_param`` which stringifies
    to ISO 8601 (symmetric with the datetime/date branch)."""

    def test_naive_time_without_microseconds(self) -> None:
        from dqlitedbapi.types import _iso8601_from_time

        assert _iso8601_from_time(datetime.time(12, 30, 45)) == "12:30:45"

    def test_midnight(self) -> None:
        from dqlitedbapi.types import _iso8601_from_time

        assert _iso8601_from_time(datetime.time(0, 0, 0)) == "00:00:00"

    def test_time_with_microseconds(self) -> None:
        from dqlitedbapi.types import _iso8601_from_time

        t = datetime.time(12, 30, 45, 123456)
        assert _iso8601_from_time(t) == "12:30:45.123456"

    def test_time_with_zero_padded_microseconds(self) -> None:
        from dqlitedbapi.types import _iso8601_from_time

        t = datetime.time(12, 30, 45, 7)
        assert _iso8601_from_time(t) == "12:30:45.000007"

    def test_time_with_utc_tzinfo(self) -> None:
        from dqlitedbapi.types import _iso8601_from_time

        t = datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        assert _iso8601_from_time(t) == "12:30:45+00:00"

    def test_time_with_fixed_negative_offset(self) -> None:
        from dqlitedbapi.types import _iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(hours=-5))
        t = datetime.time(12, 30, 45, tzinfo=tz)
        assert _iso8601_from_time(t) == "12:30:45-05:00"

    def test_time_with_fixed_positive_offset_and_microseconds(self) -> None:
        from dqlitedbapi.types import _iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        t = datetime.time(12, 30, 45, microsecond=42, tzinfo=tz)
        assert _iso8601_from_time(t) == "12:30:45.000042+05:30"

    def test_time_sub_minute_offset_preserves_seconds(self) -> None:
        """Matches the datetime encoder's sub-minute-offset preservation:
        ``datetime.time.utcoffset()`` can also return a sub-minute
        timedelta, and ``datetime.time.fromisoformat`` on Python 3.11+
        round-trips ``±HH:MM:SS`` offsets."""
        from dqlitedbapi.types import _iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(minutes=5, seconds=30))
        t = datetime.time(12, 30, 45, tzinfo=tz)
        assert _iso8601_from_time(t) == "12:30:45+00:05:30"

    def test_time_whole_minute_offset_stays_hh_mm(self) -> None:
        from dqlitedbapi.types import _iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        t = datetime.time(12, 30, 45, tzinfo=tz)
        assert _iso8601_from_time(t) == "12:30:45+05:30"

    def test_time_sub_minute_round_trips_through_fromisoformat(self) -> None:
        """The emitted ISO string must decode back to a ``datetime.time``
        with the exact utcoffset — pinning the symmetric decode path
        in stdlib.
        """
        from dqlitedbapi.types import _iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(minutes=5, seconds=30))
        original = datetime.time(12, 30, 45, tzinfo=tz)
        encoded = _iso8601_from_time(original)
        decoded = datetime.time.fromisoformat(encoded)
        assert decoded == original
        assert decoded.utcoffset() == original.utcoffset()


class TestConvertBindParamTime:
    """``_convert_bind_param`` routes ``datetime.time`` to the new
    ISO 8601 encoder (symmetric with the existing datetime/date
    branch). Prior behaviour let ``datetime.time`` fall through to
    the wire encoder, which raised ``EncodeError`` — the driver's
    own ``Time()`` constructor produced values its own binder could
    not consume.
    """

    def test_time_converted_to_iso_string(self) -> None:
        from dqlitedbapi.types import _convert_bind_param

        assert _convert_bind_param(datetime.time(12, 30, 45)) == "12:30:45"

    def test_time_with_tzinfo_converted(self) -> None:
        from dqlitedbapi.types import _convert_bind_param

        t = datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        assert _convert_bind_param(t) == "12:30:45+00:00"

    def test_datetime_branch_still_takes_precedence(self) -> None:
        """``datetime.datetime`` is a subclass of ``datetime.date`` but
        NOT of ``datetime.time``, so the existing branch keeps firing
        first for datetime inputs. Defensive pin in case the order of
        checks is ever refactored.
        """
        from dqlitedbapi.types import _convert_bind_param

        dt = datetime.datetime(2025, 1, 1, 12, 30, 45)
        assert _convert_bind_param(dt) == "2025-01-01 12:30:45"

    def test_non_temporal_value_passes_through(self) -> None:
        from dqlitedbapi.types import _convert_bind_param

        assert _convert_bind_param(42) == 42
        assert _convert_bind_param("hello") == "hello"
        assert _convert_bind_param(b"bytes") == b"bytes"
        assert _convert_bind_param(None) is None


class TestIso8601DecoderTrailingZ:
    """Pin the decoder's trailing-Z handling. Python 3.11+ accepts a
    bare ``Z`` natively in ``datetime.fromisoformat``; the decoder
    previously substituted ``"Z"`` → ``"+00:00"`` unconditionally,
    which mangled malformed inputs like ``"junkZ"`` → ``"junk+00:00"``
    before reaching ``fromisoformat`` and obscured operator
    diagnostics. The substitution is now removed; these tests pin
    that the decoder still handles well-formed Z suffixes correctly
    AND that malformed inputs surface in the ``DataError`` message
    verbatim (no pre-substitution mangling).
    """

    def test_well_formed_z_suffix_decodes_to_utc(self) -> None:
        import pytest

        from dqlitedbapi.types import _datetime_from_iso8601

        # The well-formed cases must still produce a UTC-aware datetime.
        for text in (
            "2024-01-02T03:04:05Z",
            "2024-01-02T03:04:05.123Z",
            "2024-01-02 03:04:05Z",
        ):
            out = _datetime_from_iso8601(text)
            assert out is not None, text
            assert out.tzinfo is not None, text
            offset = out.utcoffset()
            assert offset is not None and offset.total_seconds() == 0, text

        del pytest

    def test_malformed_z_input_dataerror_echoes_original_text(self) -> None:
        """The DataError message must contain the original wire text
        verbatim — not a post-substitution intermediate. An operator
        debugging "all UNIXTIME from server X is bad" needs to see
        what the server actually sent.
        """
        import pytest

        from dqlitedbapi.exceptions import DataError
        from dqlitedbapi.types import _datetime_from_iso8601

        for wire_text in (
            "Z",
            "junkZ",
            "  Z",
            "2024-01-02XYZ",
        ):
            with pytest.raises(DataError) as ei:
                _datetime_from_iso8601(wire_text)
            assert wire_text in str(ei.value), (
                f"DataError message must echo original wire text {wire_text!r}; got {ei.value!s}"
            )

    def test_lowercase_z_is_rejected(self) -> None:
        """``fromisoformat`` rejects lowercase ``z``. Pin this so any
        future Python-version drift surfaces visibly.
        """
        import pytest

        from dqlitedbapi.exceptions import DataError
        from dqlitedbapi.types import _datetime_from_iso8601

        with pytest.raises(DataError):
            _datetime_from_iso8601("2024-01-02T03:04:05z")
