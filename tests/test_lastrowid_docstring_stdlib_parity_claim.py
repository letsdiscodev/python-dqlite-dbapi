"""The ``lastrowid`` docstrings must frame cursor-scope as stdlib parity
(stdlib's is also cursor-scoped), while keeping the separate
``INSERT ... RETURNING`` divergence note accurate.
"""

from __future__ import annotations

import sqlite3


def test_stdlib_lastrowid_is_per_cursor_not_per_connection() -> None:
    """Stdlib ``sqlite3.Cursor.lastrowid`` is cursor-scoped: a sibling cursor
    that ran no INSERT keeps ``None``."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
    c1 = conn.cursor()
    c2 = conn.cursor()
    c1.execute("INSERT INTO t (v) VALUES ('a')")
    assert c1.lastrowid == 1
    assert c2.lastrowid is None
