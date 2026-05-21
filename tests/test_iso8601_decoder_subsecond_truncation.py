"""Behavioural + doc pin for ``_datetime_from_iso8601``: CPython's
``datetime.fromisoformat`` (widened in 3.11 via gh-80010 to accept
any fractional-digit count) silently truncates digits beyond the
sixth without rounding. Peer encoders that emit nanosecond-precision
text (Go ``time.RFC3339Nano``, custom SQLite triggers) therefore
lose precision on read and do not round-trip byte-identically
through the dbapi decoder + encoder pair.

This file pins:

1. The truncation behaviour for 7-, 8-, and 9-digit fractional
   inputs (parametrised). The ``.999999999`` case checks that
   truncation does NOT round up into the next second.
2. The asymmetric-round-trip claim against the 6-digit-emitting
   encoder ``_iso8601_from_datetime``.
3. The docstring of ``_datetime_from_iso8601`` documents the
   truncation explicitly — so a future reader inspecting the
   docstring sees the boundary at the same place the code surface
   is observable.
"""

from __future__ import annotations

import datetime

import pytest

from dqlitedbapi.types import _datetime_from_iso8601, _iso8601_from_datetime


class TestIso8601DecoderSubsecondTruncation:
    """Pin the silent-truncation contract for ≥7-digit fractional
    inputs."""

    @pytest.mark.parametrize(
        ("encoded", "expected_us"),
        [
            # 7 digits — last digit silently truncated.
            ("2026-05-21 12:34:56.1234567", 123456),
            # 8 digits.
            ("2026-05-21 12:34:56.12345678", 123456),
            # 9 digits (Go RFC3339Nano shape).
            ("2026-05-21 12:34:56.123456789", 123456),
            # Truncation does NOT round up — would carry into the
            # next second if it rounded.
            ("2026-05-21 12:34:56.999999999", 999999),
        ],
    )
    def test_decoder_truncates_subsecond_below_microsecond(
        self, encoded: str, expected_us: int
    ) -> None:
        result = _datetime_from_iso8601(encoded)
        assert isinstance(result, datetime.datetime)
        assert result.microsecond == expected_us, (
            f"decoder dropped/preserved trailing nanoseconds in an "
            f"unexpected way: got microsecond={result.microsecond!r}, "
            f"expected {expected_us!r}; full result={result!r}"
        )
        # Truncation does not cross the second boundary.
        assert result.second == 56

    def test_decoder_truncation_breaks_byte_identical_round_trip(self) -> None:
        """A peer-written 9-digit value, decoded here and re-written
        via the 6-digit-emitting encoder, is NOT byte-identical to
        the peer-written original. This pins the asymmetric
        round-trip claim in the docstring."""
        peer_emitted = "2026-05-21 12:34:56.123456789"
        decoded = _datetime_from_iso8601(peer_emitted)
        assert isinstance(decoded, datetime.datetime)
        roundtrip = _iso8601_from_datetime(decoded)
        assert roundtrip == "2026-05-21 12:34:56.123456"
        assert roundtrip != peer_emitted


def test_docstring_documents_subsecond_truncation() -> None:
    """Doc pin: the ``_datetime_from_iso8601`` docstring documents
    the silent-truncation behaviour for fractional-digit counts ≥7.
    Pins specific substrings so a future docstring edit cannot drop
    the truncation callout silently."""
    doc = _datetime_from_iso8601.__doc__ or ""
    # The truncation callout uses a recognisable header phrase.
    assert "Sub-microsecond" in doc or "sub-microsecond" in doc, (
        "_datetime_from_iso8601 docstring no longer mentions the "
        "sub-microsecond truncation boundary; operators interop'ing "
        "with Go RFC3339Nano peers rely on this paragraph."
    )
    # The asymmetric-round-trip consequence is the most operationally
    # useful artifact for cross-driver users — pin its presence.
    assert "round-trip" in doc, (
        "_datetime_from_iso8601 docstring no longer mentions the "
        "asymmetric-round-trip consequence vs the 6-digit encoder."
    )
