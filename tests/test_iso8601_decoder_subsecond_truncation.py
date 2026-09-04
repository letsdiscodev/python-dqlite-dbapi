"""``datetime.fromisoformat`` silently truncates fractional digits beyond
the sixth without rounding (so Go RFC3339Nano peers lose precision on read).
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.types import datetime_from_iso8601, iso8601_from_datetime


class TestIso8601DecoderSubsecondTruncation:
    @pytest.mark.parametrize(
        ("encoded", "expected_us"),
        [
            ("2026-05-21 12:34:56.1234567", 123456),
            ("2026-05-21 12:34:56.12345678", 123456),
            ("2026-05-21 12:34:56.123456789", 123456),
            # Truncation does NOT round up into the next second.
            ("2026-05-21 12:34:56.999999999", 999999),
        ],
    )
    def test_decoder_truncates_subsecond_below_microsecond(
        self, encoded: str, expected_us: int
    ) -> None:
        result = datetime_from_iso8601(encoded)
        assert isinstance(result, datetime.datetime)
        assert result.microsecond == expected_us, (
            f"decoder dropped/preserved trailing nanoseconds in an "
            f"unexpected way: got microsecond={result.microsecond!r}, "
            f"expected {expected_us!r}; full result={result!r}"
        )
        assert result.second == 56

    def test_decoder_truncation_breaks_byte_identical_round_trip(self) -> None:
        """A peer-written 9-digit value does not round-trip byte-identically
        through the 6-digit-emitting encoder."""
        peer_emitted = "2026-05-21 12:34:56.123456789"
        decoded = datetime_from_iso8601(peer_emitted)
        assert isinstance(decoded, datetime.datetime)
        roundtrip = iso8601_from_datetime(decoded)
        assert roundtrip == "2026-05-21 12:34:56.123456"
        assert roundtrip != peer_emitted
