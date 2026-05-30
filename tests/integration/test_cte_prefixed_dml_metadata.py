"""CTE-prefixed DML (``WITH ... INSERT/UPDATE/DELETE`` without RETURNING) routes as
DML: it sets ``lastrowid`` and reports the affected-row count, rather than being
misclassified as a query (rowcount=-1, lastrowid=None). ``WITH ... SELECT`` and
``WITH ... RETURNING`` still return rows."""

from __future__ import annotations

import pytest

import dqlitedbapi


@pytest.mark.integration
def test_cte_prefixed_insert_sets_rowcount_and_lastrowid(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS cte_dml")
        cur.execute("CREATE TABLE cte_dml (id INTEGER PRIMARY KEY, v INTEGER)")
        conn.commit()

        cur.execute("WITH s(x) AS (VALUES (7), (8)) INSERT INTO cte_dml (v) SELECT x FROM s")
        assert cur.rowcount == 2
        assert cur.lastrowid is not None
        assert cur.description is None
        conn.commit()

        # WITH ... SELECT still returns rows.
        cur.execute("WITH s AS (SELECT v FROM cte_dml) SELECT * FROM s ORDER BY v")
        assert [r[0] for r in cur.fetchall()] == [7, 8]
        assert cur.description is not None

        # WITH ... INSERT ... RETURNING still returns rows.
        cur.execute("WITH s(x) AS (VALUES (9)) INSERT INTO cte_dml (v) SELECT x FROM s RETURNING v")
        assert [r[0] for r in cur.fetchall()] == [9]
        conn.commit()


@pytest.mark.integration
def test_cte_prefixed_update_reports_affected(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS cte_upd")
        cur.execute("CREATE TABLE cte_upd (id INTEGER PRIMARY KEY, v INTEGER)")
        cur.executemany("INSERT INTO cte_upd (v) VALUES (?)", [(1,), (2,), (3,)])
        conn.commit()

        cur.execute("WITH s AS (SELECT id FROM cte_upd) UPDATE cte_upd SET v = v + 1")
        assert cur.rowcount == 3
        assert cur.description is None
        conn.commit()
