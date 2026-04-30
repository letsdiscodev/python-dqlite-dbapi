"""Pin: NULL, empty TEXT (``""``), and empty BLOB (``b""``) are
distinct on the wire and through the dbapi readback path.

Wire-level shape:

* ``None`` → ``ValueType.NULL`` (5), payload ``b"\\x00" * 8``
* ``""`` → ``ValueType.TEXT`` (3), payload ``b"\\x00" * 8`` (NUL +
  pad)
* ``b""`` → ``ValueType.BLOB`` (4), payload ``b"\\x00" * 8`` (uint64
  length=0)

The payloads are byte-identical (8 zero bytes); the type tags
disambiguate. SQLite preserves the same three-way distinction
(``typeof(NULL) = 'null'``, ``typeof('') = 'text'``,
``typeof(X'') = 'blob'``).

A future "coerce empty to NULL" change would silently break round-
trip across cluster nodes that share data with non-Python peers
(Go / C clients seeing the original distinction). Pin so the change
must be deliberate.
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
    """``'' = ''`` is 1 (TEXT comparison); ``NULL = NULL`` is NULL
    (unknown). The dbapi must surface these as ``1`` and ``None``
    respectively — same as stdlib ``sqlite3``."""
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
    """Mixed NULL / empty-TEXT / empty-BLOB across a multi-row
    executemany batch. Pins per-row type-tag selection so a
    refactor of the row-encoder cannot silently coerce one shape
    into another."""
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
