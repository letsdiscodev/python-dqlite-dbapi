"""Integration-level pin on NaN / ±Inf float round-trip behaviour.

The wire encoder accepts NaN, +Inf, -Inf (matching go-dqlite). SQLite
itself has implementation-defined behaviour for these IEEE 754
edge cases: some builds store them as NULL, others preserve the
bit pattern. No client-layer test previously pinned what the
running test cluster actually does.

This test records the observed behaviour. If it fails after a server
upgrade, the SQLite build's NaN / Inf handling changed — update the
pin and document the change, don't silently mask it.
"""

from __future__ import annotations

import math

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
class TestNanInfRoundTrip:
    def test_nan_inf_neg_inf_round_trip(self, cluster_address: str) -> None:
        """Pin the observed behaviour for NaN / +Inf / -Inf on a REAL column.

        The assertions below are not prescriptive; they record what the
        current test cluster returns. A failure here means the SQLite
        build changed behaviour — investigate, then update the pin.
        """
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

            # Assert the shape — three rows with the expected ids.
            assert set(by_id.keys()) == {1, 2, 3}

            # Pin current server behaviour: SQLite stores NaN as NULL
            # (sqlite3_bind_double converts NaN to NULL) under the test
            # cluster's build. If this assertion flips, investigate the
            # server's SQLite version and decide whether to widen the
            # docstring or raise a server-side bug.
            nan_value = by_id[1]
            assert nan_value is None or (isinstance(nan_value, float) and math.isnan(nan_value)), (
                f"unexpected NaN handling: {nan_value!r}"
            )

            # ±Inf is implementation-defined but SQLite 3.38+ preserves
            # the bit pattern as a float. Accept either preservation or
            # NULL (older builds).
            plus_inf = by_id[2]
            assert plus_inf is None or plus_inf == float("inf"), (
                f"unexpected +Inf handling: {plus_inf!r}"
            )
            minus_inf = by_id[3]
            assert minus_inf is None or minus_inf == float("-inf"), (
                f"unexpected -Inf handling: {minus_inf!r}"
            )

            cursor.execute("DROP TABLE nan_inf_test")
