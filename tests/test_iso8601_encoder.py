"""Encoder-side tests for ``_iso8601_from_datetime``.

The decode side (``_datetime_from_iso8601``) has explicit error-path
tests; the encoder was only exercised by integration tests. These unit
tests pin the microsecond padding, tz-offset sign/magnitude, and the
``date``-vs-``datetime`` fall-through so a regression in any branch
surfaces quickly.
"""

import datetime

import pytest

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


class _AbstractTz(datetime.tzinfo):
    """tzinfo that carries a name but declines to produce an offset.

    Python's stdlib contract explicitly allows ``utcoffset()`` to return
    ``None`` — real-world tzinfo subclasses sometimes do so during
    initialisation before their zone tables are loaded, or when the
    tzinfo is "abstract" by design. Both ``_iso8601_from_datetime`` and
    ``_iso8601_from_time`` carry a ``if offset is None: return base``
    early-return to handle that case; these tests pin the fallback-to-
    naive-format contract so a future cleanup cannot silently
    reinstate an ``assert offset is not None`` (see done/ISSUE-108 for
    the paired dead-assert removal).
    """

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return None

    def dst(self, dt: datetime.datetime | None) -> datetime.timedelta | None:
        return None

    def tzname(self, dt: datetime.datetime | None) -> str:
        return "ABSTRACT"


class TestIso8601EncoderBrokenTzinfo:
    """Pin the ``utcoffset() is None`` rejection branch on both encoders.

    A tzinfo subclass that declares itself but cannot resolve an
    offset for the given datetime/time is a broken contract. Cycle 22
    flipped this from silent demotion (encoded as naive, losing the
    user's tz-awareness intent) to a hard ``DataError``. Pin the new
    contract so a regression that re-introduces silent demotion is
    caught.
    """

    def test_datetime_broken_tzinfo_raises_data_error(self) -> None:
        from dqlitedbapi.exceptions import DataError

        value = datetime.datetime(2024, 5, 1, 12, 30, 45, tzinfo=_AbstractTz())
        with pytest.raises(DataError, match="utcoffset"):
            _iso8601_from_datetime(value)

    def test_time_broken_tzinfo_raises_data_error(self) -> None:
        from dqlitedbapi.exceptions import DataError
        from dqlitedbapi.types import _iso8601_from_time

        value = datetime.time(12, 30, 45, tzinfo=_AbstractTz())
        with pytest.raises(DataError, match="utcoffset"):
            _iso8601_from_time(value)


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


class TestIso8601YearBoundaryRoundTrip:
    """Pin ISO 8601 round-trip at Python's ``MINYEAR`` (1) and
    ``MAXYEAR`` (9999).

    The datetime branch of the encoder builds the year prefix
    explicitly (``f"{value.year:04d}"``); the date branch delegates to
    ``value.isoformat()`` and inherits CPython's documented zero-pad.
    A future "simplify" pass on either branch could silently break
    cluster-shared cells written by a Go/C peer at the year
    boundaries (Go's ``time.RFC3339Nano`` zero-pads unconditionally).

    Also pins decoder rejection at year > 9999 — Go has no MAXYEAR
    cap, so a peer could deliver a value outside Python's range; the
    decoder must wrap the resulting ``ValueError`` as ``DataError``
    with the original wire text.
    """

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
        from dqlitedbapi.types import _datetime_from_iso8601

        encoded = _iso8601_from_datetime(value)
        decoded = _datetime_from_iso8601(encoded)
        # ``_datetime_from_iso8601`` widens ``date`` to ``datetime``
        # on round-trip (documented behaviour matching pysqlite).
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
        """The date branch delegates to ``value.isoformat()`` so the
        zero-pad guarantee is inherited from CPython. A regression
        that produces ``"1-01-01"`` would round-trip locally only
        until a Go/C peer fails to parse it.
        """
        encoded = _iso8601_from_datetime(datetime.date(1, 1, 1))
        assert encoded == "0001-01-01", (
            f"date branch must zero-pad year to 4 digits even at year=1; got {encoded!r}"
        )

    def test_decoder_rejects_year_above_python_maxyear(self) -> None:
        """Go has no MAXYEAR cap; a non-Python cluster peer could
        deliver a year > 9999. The decoder must wrap the resulting
        ``ValueError`` as ``DataError`` with the original wire text.
        """
        import pytest

        from dqlitedbapi.exceptions import DataError
        from dqlitedbapi.types import _datetime_from_iso8601

        with pytest.raises(DataError) as ei:
            _datetime_from_iso8601("10000-01-01T00:00:00")
        assert "10000-01-01T00:00:00" in str(ei.value), (
            f"DataError must echo original wire text; got {ei.value!s}"
        )


class TestIso8601FractionalSecondsVariants:
    """Pin the matrix of fractional-second representations the decoder
    accepts. Python's ``datetime.fromisoformat`` is lenient on the
    input side: 0 / 3 / 6 fractional digits, ``T`` or space separator,
    and ``Z`` / explicit-offset / no-offset are all valid. The dqlite
    server emits canonical space-separated 6-digit values, but a peer
    client (Go, C, custom) might emit a different shape; the decoder
    must accept all of them.

    A future change to the parser (custom regex, strict validation)
    would silently break the no-µs case which is the canonical wire
    form for whole-second wall clocks.
    """

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
        from dqlitedbapi.types import _datetime_from_iso8601

        assert _datetime_from_iso8601(encoded) == expected

    def test_decoder_accepts_bare_date_via_datetime_fromisoformat(self) -> None:
        """Python 3.11+ relaxed ``datetime.fromisoformat`` to accept
        bare ``YYYY-MM-DD`` without a time component (returning a
        midnight datetime). Pin the contract so a regression that
        re-tightens the parser, or that re-introduces a dead
        ``date.fromisoformat`` fallback, cannot land silently."""
        from dqlitedbapi.types import _datetime_from_iso8601

        result = _datetime_from_iso8601("2024-01-15")
        assert isinstance(result, datetime.datetime)
        assert result == datetime.datetime(2024, 1, 15, 0, 0)
