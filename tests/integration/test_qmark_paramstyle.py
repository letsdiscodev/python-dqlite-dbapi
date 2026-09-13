"""The advertised qmark placeholder style executes against a live cluster."""

from __future__ import annotations

import pytest

import dqlitedbapi

pytestmark = pytest.mark.integration


def test_qmark_sql_works(cluster_address: str) -> None:
    conn = dqlitedbapi.connect(cluster_address, timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("SELECT ?", ("x",))
        assert cur.fetchone() == ("x",)
    finally:
        conn.close()
