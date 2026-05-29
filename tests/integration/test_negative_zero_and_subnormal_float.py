"""Pin -0.0 sign-bit and IEEE 754 subnormal/boundary round-trip through the REAL column path."""

from __future__ import annotations

import math
import struct
import sys

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
class TestNegativeZeroAndSubnormalRoundTrip:
    def test_negative_zero_normalises_to_positive_zero_on_server(
        self, cluster_address: str
    ) -> None:
        """SQLite REAL storage normalises -0.0 to +0.0 on insert; the wire layer
        preserves the sign bit, so the loss is server-side."""
        conn = connect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS neg_zero_pin")
            cur.execute("CREATE TABLE neg_zero_pin (v REAL)")
            cur.execute("INSERT INTO neg_zero_pin VALUES (?)", (-0.0,))
            cur.execute("SELECT v FROM neg_zero_pin")
            rows = cur.fetchall()
            assert len(rows) == 1
            assert isinstance(rows[0][0], float)
            # Currently observed: server normalises to +0.0.
            assert math.copysign(1.0, rows[0][0]) == 1.0, (
                f"server-side -0.0 normalisation drifted: got "
                f"sign={math.copysign(1.0, rows[0][0])} for {rows[0][0]!r}"
            )
        finally:
            conn.close()

    @pytest.mark.parametrize(
        "value",
        [
            sys.float_info.min,  # smallest normal positive
            -sys.float_info.min,
            sys.float_info.max,  # largest finite
            -sys.float_info.max,
            sys.float_info.epsilon,  # machine epsilon
            5e-324,  # smallest positive (subnormal)
            -5e-324,
        ],
    )
    def test_ieee754_boundary_round_trip(self, cluster_address: str, value: float) -> None:
        """IEEE-754 boundary values round-trip bit-exact through SQLite REAL storage."""
        conn = connect(cluster_address, timeout=2.0)
        try:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS subnormal_pin")
            cur.execute("CREATE TABLE subnormal_pin (v REAL)")
            cur.execute("INSERT INTO subnormal_pin VALUES (?)", (value,))
            cur.execute("SELECT v FROM subnormal_pin")
            rows = cur.fetchall()
            assert len(rows) == 1
            decoded = rows[0][0]
            assert isinstance(decoded, float)
            assert struct.pack("<d", decoded) == struct.pack("<d", value), (
                f"{value!r} did not round-trip bit-exact: got {decoded!r}"
            )
        finally:
            conn.close()
