"""Pin observed NaN / ±Inf float round-trip behaviour.

SQLite's handling of these IEEE 754 edge cases is implementation-defined (NULL vs
preserved bit pattern); a failure after a server upgrade means the SQLite build changed.
"""

from __future__ import annotations

import math

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
class TestNanInfRoundTrip:
    def test_nan_inf_neg_inf_round_trip(self, cluster_address: str) -> None:
        """Record (not prescribe) what the test cluster returns for NaN / ±Inf on REAL."""
        with connect(cluster_address, database="test_nan_inf") as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS nan_inf_test")
            cursor.execute("CREATE TABLE nan_inf_test (id INTEGER PRIMARY KEY, v REAL)")
            cursor.execute("INSERT INTO nan_inf_test VALUES (1, ?)", (float("nan"),))
            cursor.execute("INSERT INTO nan_inf_test VALUES (2, ?)", (float("inf"),))
            cursor.execute("INSERT INTO nan_inf_test VALUES (3, ?)", (float("-inf"),))
            cursor.execute("SELECT id, v FROM nan_inf_test ORDER BY id")
            rows = cursor.fetchall()

            by_id = {row[0]: row[1] for row in rows}

            assert set(by_id.keys()) == {1, 2, 3}

            # Current build: sqlite3_bind_double converts NaN to NULL.
            nan_value = by_id[1]
            assert nan_value is None or (isinstance(nan_value, float) and math.isnan(nan_value)), (
                f"unexpected NaN handling: {nan_value!r}"
            )

            # SQLite 3.38+ preserves ±Inf; older builds store NULL. Accept either.
            plus_inf = by_id[2]
            assert plus_inf is None or plus_inf == float("inf"), (
                f"unexpected +Inf handling: {plus_inf!r}"
            )
            minus_inf = by_id[3]
            assert minus_inf is None or minus_inf == float("-inf"), (
                f"unexpected -Inf handling: {minus_inf!r}"
            )

            cursor.execute("DROP TABLE nan_inf_test")
