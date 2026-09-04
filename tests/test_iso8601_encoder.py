"""Encoder-side tests for ``iso8601_from_datetime``."""

import datetime

import pytest

from dqlitedbapi.types import iso8601_from_datetime


class TestIso8601FromDatetime:
    def test_datetime_without_microseconds(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0)
        assert iso8601_from_datetime(d) == "2025-01-01 12:00:00"

    def test_datetime_with_microseconds_zero_padded(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, microsecond=7)
        assert iso8601_from_datetime(d) == "2025-01-01 12:00:00.000007"

    def test_datetime_with_six_digit_microseconds(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, microsecond=999999)
        assert iso8601_from_datetime(d) == "2025-01-01 12:00:00.999999"

    def test_naive_datetime_has_no_offset(self) -> None:
        """Naive datetimes emit the bare ISO string (no trailing offset)."""
        d = datetime.datetime(2025, 1, 1, 12, 0, 0)
        result = iso8601_from_datetime(d)
        assert "+" not in result and result.count("-") == 2

    def test_utc_offset_emitted_as_plus_zero(self) -> None:
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.UTC)
        result = iso8601_from_datetime(d)
        assert result.endswith("+00:00")

    def test_positive_offset(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert iso8601_from_datetime(d).endswith("+05:30")

    def test_negative_offset(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-5, minutes=-30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert iso8601_from_datetime(d).endswith("-05:30")

    def test_negative_offset_with_microseconds(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-8))
        d = datetime.datetime(2025, 6, 15, 9, 30, 45, microsecond=42, tzinfo=tz)
        assert iso8601_from_datetime(d) == "2025-06-15 09:30:45.000042-08:00"

    def test_date_only_takes_fall_through_branch(self) -> None:
        """``date`` (not ``datetime``) produces the short YYYY-MM-DD form."""
        d = datetime.date(2025, 1, 1)
        assert iso8601_from_datetime(d) == "2025-01-01"

    def test_sub_minute_offset_preserves_seconds(self) -> None:
        """Historical IANA LMT offsets carry sub-minute precision; the encoder
        must emit ``±HH:MM:SS`` so the offset round-trips exactly."""
        tz = datetime.timezone(datetime.timedelta(minutes=5, seconds=30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert iso8601_from_datetime(d) == "2025-01-01 12:00:00+00:05:30"

    def test_sub_minute_negative_offset_preserves_seconds(self) -> None:
        tz = datetime.timezone(datetime.timedelta(hours=-1, seconds=-15))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert iso8601_from_datetime(d) == "2025-01-01 12:00:00-01:00:15"

    def test_whole_minute_offset_stays_hh_mm(self) -> None:
        """Whole-minute offsets still emit ``±HH:MM`` (only sub-minute widens)."""
        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        d = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        assert iso8601_from_datetime(d) == "2025-01-01 12:00:00+05:30"

    def test_sub_minute_offset_round_trips_through_decoder(self) -> None:
        from dqlitedbapi.types import datetime_from_iso8601

        tz = datetime.timezone(datetime.timedelta(minutes=5, seconds=30))
        original = datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=tz)
        encoded = iso8601_from_datetime(original)
        decoded = datetime_from_iso8601(encoded)
        assert decoded == original
        assert decoded is not None and decoded.utcoffset() == original.utcoffset()


class TestIso8601FromTime:
    """Encoder for ``datetime.time`` values."""

    def test_naive_time_without_microseconds(self) -> None:
        from dqlitedbapi.types import iso8601_from_time

        assert iso8601_from_time(datetime.time(12, 30, 45)) == "12:30:45"

    def test_midnight(self) -> None:
        from dqlitedbapi.types import iso8601_from_time

        assert iso8601_from_time(datetime.time(0, 0, 0)) == "00:00:00"

    def test_time_with_microseconds(self) -> None:
        from dqlitedbapi.types import iso8601_from_time

        t = datetime.time(12, 30, 45, 123456)
        assert iso8601_from_time(t) == "12:30:45.123456"

    def test_time_with_zero_padded_microseconds(self) -> None:
        from dqlitedbapi.types import iso8601_from_time

        t = datetime.time(12, 30, 45, 7)
        assert iso8601_from_time(t) == "12:30:45.000007"

    def test_time_with_utc_tzinfo(self) -> None:
        from dqlitedbapi.types import iso8601_from_time

        t = datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        assert iso8601_from_time(t) == "12:30:45+00:00"

    def test_time_with_fixed_negative_offset(self) -> None:
        from dqlitedbapi.types import iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(hours=-5))
        t = datetime.time(12, 30, 45, tzinfo=tz)
        assert iso8601_from_time(t) == "12:30:45-05:00"

    def test_time_with_fixed_positive_offset_and_microseconds(self) -> None:
        from dqlitedbapi.types import iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        t = datetime.time(12, 30, 45, microsecond=42, tzinfo=tz)
        assert iso8601_from_time(t) == "12:30:45.000042+05:30"

    def test_time_sub_minute_offset_preserves_seconds(self) -> None:
        """``datetime.time`` also preserves sub-minute offsets as ``±HH:MM:SS``."""
        from dqlitedbapi.types import iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(minutes=5, seconds=30))
        t = datetime.time(12, 30, 45, tzinfo=tz)
        assert iso8601_from_time(t) == "12:30:45+00:05:30"

    def test_time_whole_minute_offset_stays_hh_mm(self) -> None:
        from dqlitedbapi.types import iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
        t = datetime.time(12, 30, 45, tzinfo=tz)
        assert iso8601_from_time(t) == "12:30:45+05:30"

    def test_time_sub_minute_round_trips_through_fromisoformat(self) -> None:
        """The emitted ISO string decodes back to the exact utcoffset."""
        from dqlitedbapi.types import iso8601_from_time

        tz = datetime.timezone(datetime.timedelta(minutes=5, seconds=30))
        original = datetime.time(12, 30, 45, tzinfo=tz)
        encoded = iso8601_from_time(original)
        decoded = datetime.time.fromisoformat(encoded)
        assert decoded == original
        assert decoded.utcoffset() == original.utcoffset()


class _AbstractTz(datetime.tzinfo):
    """tzinfo that carries a name but returns ``None`` from ``utcoffset()``."""

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return None

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return None

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "ABSTRACT"


class TestIso8601EncoderBrokenTzinfo:
    """A broken tzinfo (offset unresolvable) raises ``DataError`` rather than
    silently demoting to naive."""

    def test_datetime_broken_tzinfo_raises_data_error(self) -> None:
        from dqlitedbapi.exceptions import DataError

        value = datetime.datetime(2024, 5, 1, 12, 30, 45, tzinfo=_AbstractTz())
        with pytest.raises(DataError, match="utcoffset"):
            iso8601_from_datetime(value)

    def test_time_broken_tzinfo_raises_data_error(self) -> None:
        from dqlitedbapi.exceptions import DataError
        from dqlitedbapi.types import iso8601_from_time

        value = datetime.time(12, 30, 45, tzinfo=_AbstractTz())
        with pytest.raises(DataError, match="utcoffset"):
            iso8601_from_time(value)


class TestConvertBindParamTime:
    """``adapt_bind_param`` routes ``datetime.time`` to the ISO 8601 encoder."""

    def test_time_converted_to_iso_string(self) -> None:
        from dqlitedbapi.types import adapt_bind_param

        assert adapt_bind_param(datetime.time(12, 30, 45)) == "12:30:45"

    def test_time_with_tzinfo_converted(self) -> None:
        from dqlitedbapi.types import adapt_bind_param

        t = datetime.time(12, 30, 45, tzinfo=datetime.UTC)
        assert adapt_bind_param(t) == "12:30:45+00:00"

    def test_datetime_branch_still_takes_precedence(self) -> None:
        """``datetime`` subclasses ``date`` but not ``time``, so its branch
        fires first."""
        from dqlitedbapi.types import adapt_bind_param

        dt = datetime.datetime(2025, 1, 1, 12, 30, 45)
        assert adapt_bind_param(dt) == "2025-01-01 12:30:45"

    def test_non_temporal_value_passes_through(self) -> None:
        from dqlitedbapi.types import adapt_bind_param

        assert adapt_bind_param(42) == 42
        assert adapt_bind_param("hello") == "hello"
        assert adapt_bind_param(b"bytes") == b"bytes"
        assert adapt_bind_param(None) is None


class TestIso8601DecoderTrailingZ:
    """Decoder relies on 3.11+ native ``Z`` parsing; well-formed suffixes
    decode to UTC and malformed inputs surface verbatim (no pre-substitution
    mangling)."""

    def test_well_formed_z_suffix_decodes_to_utc(self) -> None:
        import pytest

        from dqlitedbapi.types import datetime_from_iso8601

        for text in (
            "2024-01-02T03:04:05Z",
            "2024-01-02T03:04:05.123Z",
            "2024-01-02 03:04:05Z",
        ):
            out = datetime_from_iso8601(text)
            assert out is not None, text
            assert out.tzinfo is not None, text
            offset = out.utcoffset()
            assert offset is not None and offset.total_seconds() == 0, text

        del pytest

    def test_malformed_z_input_dataerror_echoes_original_text(self) -> None:
        """The DataError message echoes the original wire text verbatim."""
        import pytest

        from dqlitedbapi.exceptions import DataError
        from dqlitedbapi.types import datetime_from_iso8601

        for wire_text in (
            "Z",
            "junkZ",
            "  Z",
            "2024-01-02XYZ",
        ):
            with pytest.raises(DataError) as ei:
                datetime_from_iso8601(wire_text)
            assert wire_text in str(ei.value), (
                f"DataError message must echo original wire text {wire_text!r}; got {ei.value!s}"
            )

    def test_lowercase_z_is_rejected(self) -> None:
        """``fromisoformat`` rejects lowercase ``z``."""
        import pytest

        from dqlitedbapi.exceptions import DataError
        from dqlitedbapi.types import datetime_from_iso8601

        with pytest.raises(DataError):
            datetime_from_iso8601("2024-01-02T03:04:05z")


class TestIso8601YearBoundaryRoundTrip:
    """ISO 8601 round-trip at ``MINYEAR``/``MAXYEAR``, plus decoder rejection
    of year > 9999 (which a Go peer with no MAXYEAR cap could deliver)."""

    @pytest.mark.parametrize(
        "value",
        [
            datetime.datetime(datetime.MINYEAR, 1, 1, 0, 0, 0),
            datetime.datetime(datetime.MINYEAR, 1, 1, 0, 0, 0, tzinfo=datetime.UTC),
            datetime.datetime(datetime.MAXYEAR, 12, 31, 23, 59, 59, 999999),
            datetime.datetime(datetime.MAXYEAR, 12, 31, 23, 59, 59, 999999, tzinfo=datetime.UTC),
            datetime.date(datetime.MINYEAR, 1, 1),
            datetime.date(datetime.MAXYEAR, 12, 31),
        ],
    )
    def test_year_boundary_round_trip(self, value: datetime.datetime | datetime.date) -> None:
        from dqlitedbapi.types import datetime_from_iso8601

        encoded = iso8601_from_datetime(value)
        decoded = datetime_from_iso8601(encoded)
        # The decoder widens ``date`` to ``datetime`` on round-trip (pysqlite parity).
        if isinstance(value, datetime.datetime):
            assert decoded == value, (
                f"year-boundary round-trip lost: original={value!r}, "
                f"encoded={encoded!r}, decoded={decoded!r}"
            )
        else:
            assert decoded == datetime.datetime(value.year, value.month, value.day), (
                f"date-branch year-boundary round-trip lost: original={value!r}, "
                f"encoded={encoded!r}, decoded={decoded!r}"
            )

    def test_date_branch_year_zero_padded_to_four_digits(self) -> None:
        """The date branch must zero-pad year to 4 digits (else a Go/C peer
        fails to parse it)."""
        encoded = iso8601_from_datetime(datetime.date(1, 1, 1))
        assert encoded == "0001-01-01", (
            f"date branch must zero-pad year to 4 digits even at year=1; got {encoded!r}"
        )

    def test_decoder_rejects_year_above_python_maxyear(self) -> None:
        """Year > 9999 (deliverable by a Go peer) wraps as DataError with the
        original wire text."""
        import pytest

        from dqlitedbapi.exceptions import DataError
        from dqlitedbapi.types import datetime_from_iso8601

        with pytest.raises(DataError) as ei:
            datetime_from_iso8601("10000-01-01T00:00:00")
        assert "10000-01-01T00:00:00" in str(ei.value), (
            f"DataError must echo original wire text; got {ei.value!s}"
        )


class TestIso8601FractionalSecondsVariants:
    """The decoder accepts the full matrix of fractional-second shapes a peer
    client might emit (0/3/6 digits, T or space separator, Z/offset/none)."""

    @pytest.mark.parametrize(
        ("encoded", "expected"),
        [
            (
                "2024-01-01 12:34:56",
                datetime.datetime(2024, 1, 1, 12, 34, 56),
            ),
            (
                "2024-01-01 12:34:56.000000",
                datetime.datetime(2024, 1, 1, 12, 34, 56),
            ),
            (
                "2024-01-01 12:34:56.123456",
                datetime.datetime(2024, 1, 1, 12, 34, 56, 123456),
            ),
            (
                "2024-01-01 12:34:56.123",
                datetime.datetime(2024, 1, 1, 12, 34, 56, 123000),
            ),
            (
                "2024-01-01T12:34:56.5",
                datetime.datetime(2024, 1, 1, 12, 34, 56, 500000),
            ),
        ],
    )
    def test_decoder_accepts_fractional_seconds_variants(
        self, encoded: str, expected: datetime.datetime
    ) -> None:
        from dqlitedbapi.types import datetime_from_iso8601

        assert datetime_from_iso8601(encoded) == expected

    def test_decoder_accepts_bare_date_via_datetime_fromisoformat(self) -> None:
        """3.11+ accepts bare ``YYYY-MM-DD`` as a midnight datetime."""
        from dqlitedbapi.types import datetime_from_iso8601

        result = datetime_from_iso8601("2024-01-15")
        assert isinstance(result, datetime.datetime)
        assert result == datetime.datetime(2024, 1, 15, 0, 0)
