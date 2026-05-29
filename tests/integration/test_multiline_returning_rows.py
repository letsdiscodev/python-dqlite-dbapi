"""Multi-line ``INSERT ... \\n RETURNING id`` returns its rows.

Regression: the classification heuristic once matched only literal-space RETURNING,
so multi-line SQL was mis-dispatched as exec-only and dropped rows silently.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import connect


@pytest.mark.integration
def test_multiline_returning_returns_rows(cluster_address: str) -> None:
    """INSERT with RETURNING on its own line must return the row."""
    conn = connect(cluster_address, database="test_multiline_returning", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ml_ret")
        cur.execute("CREATE TABLE ml_ret (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute(
            "INSERT INTO ml_ret (id, name) VALUES (?, ?)\nRETURNING id",
            (1, "alice"),
        )
        rows = cur.fetchall()
        assert rows == [(1,)], f"multi-line RETURNING dropped rows: got {rows!r}"
        assert cur.description is not None
        assert cur.description[0][0] == "id"
    finally:
        conn.close()


@pytest.mark.integration
def test_multiline_update_returning_returns_rows(cluster_address: str) -> None:
    """UPDATE with RETURNING on its own line."""
    conn = connect(cluster_address, database="test_multiline_returning", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ml_upd")
        cur.execute("CREATE TABLE ml_upd (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute("INSERT INTO ml_upd (id, name) VALUES (?, ?)", (1, "old"))
        cur.execute(
            "UPDATE ml_upd SET name = ?\nWHERE id = ?\nRETURNING id, name",
            ("new", 1),
        )
        rows = cur.fetchall()
        assert rows == [(1, "new")], f"multi-line RETURNING dropped rows: got {rows!r}"
    finally:
        conn.close()


@pytest.mark.integration
def test_multiline_delete_returning_returns_rows(cluster_address: str) -> None:
    """DELETE with RETURNING on its own line."""
    conn = connect(cluster_address, database="test_multiline_returning", timeout=2.0)
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS ml_del")
        cur.execute("CREATE TABLE ml_del (id INTEGER PRIMARY KEY, name TEXT)")
        cur.execute("INSERT INTO ml_del (id, name) VALUES (?, ?)", (1, "alice"))
        cur.execute(
            "DELETE FROM ml_del WHERE id = ?\nRETURNING id, name",
            (1,),
        )
        rows = cur.fetchall()
        assert rows == [(1, "alice")], f"multi-line RETURNING dropped rows: got {rows!r}"
    finally:
        conn.close()
