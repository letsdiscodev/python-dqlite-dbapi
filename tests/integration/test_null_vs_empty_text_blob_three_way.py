"""Pin: NULL, empty TEXT (``""``), and empty BLOB (``b""``) stay distinct end-to-end.

All three have byte-identical 8-zero payloads on the wire; only the type tag
disambiguates them, matching SQLite's typeof() null/text/blob distinction.
"""

from __future__ import annotations

import pytest

import dqlitedbapi


@pytest.mark.integration
def test_null_text_blob_three_way_distinction(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address, database="test_three_way") as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS tw (id INTEGER PRIMARY KEY, v BLOB)")
        c.execute("DELETE FROM tw")
        c.execute("INSERT INTO tw VALUES (1, NULL)")
        c.execute("INSERT INTO tw VALUES (2, ?)", ("",))
        c.execute("INSERT INTO tw VALUES (3, ?)", (b"",))
        c.execute("SELECT id, v, typeof(v) FROM tw ORDER BY id")
        rows = c.fetchall()
        assert rows[0] == (1, None, "null")
        assert rows[1] == (2, "", "text")
        assert isinstance(rows[1][1], str)
        assert rows[2] == (3, b"", "blob")
        assert isinstance(rows[2][1], bytes)
        c.execute("DROP TABLE tw")


@pytest.mark.integration
def test_empty_text_compares_distinctly_from_null(cluster_address: str) -> None:
    """``'' = ''`` is 1; ``NULL = NULL`` is NULL (unknown) — same as stdlib sqlite3."""
    with dqlitedbapi.connect(cluster_address, database="test_empty_eq") as conn:
        c = conn.cursor()
        c.execute("SELECT '' = ''")
        assert c.fetchone() == (1,)
        c.execute("SELECT NULL = NULL")
        assert c.fetchone() == (None,)
        c.execute("SELECT '' IS NULL")
        assert c.fetchone() == (0,)
        c.execute("SELECT '' = NULL")
        assert c.fetchone() == (None,)


@pytest.mark.integration
def test_executemany_mixed_null_empty_text_blob(cluster_address: str) -> None:
    """Mixed NULL / empty-TEXT / empty-BLOB in one executemany batch pins per-row type tags."""
    with dqlitedbapi.connect(cluster_address, database="test_em_mix") as conn:
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS em (id INTEGER PRIMARY KEY, t TEXT, b BLOB)")
        c.execute("DELETE FROM em")
        c.executemany(
            "INSERT INTO em VALUES (?, ?, ?)",
            [
                (1, None, None),
                (2, "", b""),
                (3, "", None),
                (4, None, b""),
            ],
        )
        c.execute("SELECT id, t, b FROM em ORDER BY id")
        rows = c.fetchall()
        assert rows[0] == (1, None, None)
        assert rows[1] == (2, "", b"")
        assert rows[2] == (3, "", None)
        assert rows[3] == (4, None, b"")
        c.execute("DROP TABLE em")
