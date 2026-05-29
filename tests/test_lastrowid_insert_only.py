"""``Cursor.lastrowid`` updates only on successful INSERT / REPLACE (matching
stdlib); UPDATE / DELETE / DDL leave the previous rowid in place.
"""

from __future__ import annotations

from dqlitedbapi import Connection


async def test_lastrowid_sticky_across_update_and_delete(monkeypatch) -> None:
    """Across INSERT -> UPDATE -> DELETE (wire returns 42, 0, 0) the cursor
    exposes 42 throughout."""
    conn = Connection("127.0.0.1:9001")
    cur = conn.cursor()

    # Fake execute returning the ``(last_insert_id, affected)`` wire sequence.
    class _FakeAsyncConn:
        def __init__(self) -> None:
            self.calls = 0
            self.responses = [(42, 1), (0, 1), (0, 1)]

        async def execute(self, sql: str, params=None):
            idx = self.calls
            self.calls += 1
            return self.responses[idx]

    fake = _FakeAsyncConn()

    async def _fake_get_async() -> _FakeAsyncConn:
        return fake

    conn._get_async_connection = _fake_get_async  # type: ignore[assignment]

    cur.execute("INSERT INTO t (v) VALUES (?)", ("x",))
    assert cur.lastrowid == 42
    cur.execute("UPDATE t SET v = 'y' WHERE id = 42")
    # Sticky: the UPDATE wire response did not zero it.
    assert cur.lastrowid == 42
    cur.execute("DELETE FROM t WHERE id = 42")
    assert cur.lastrowid == 42


def test_is_insert_or_replace_prefix_detection() -> None:
    """Detect INSERT / INSERT OR REPLACE / INSERT OR IGNORE / REPLACE;
    reject UPDATE / DELETE / DDL / WITH."""
    from dqlitedbapi.cursor import _is_insert_or_replace

    for sql in (
        "INSERT INTO t VALUES (1)",
        "INSERT OR REPLACE INTO t VALUES (1)",
        "INSERT OR IGNORE INTO t VALUES (1)",
        "insert into t values (1)",  # case-insensitive
        "REPLACE INTO t VALUES (1)",
        "  -- c\n  INSERT INTO t VALUES (1)",  # leading comment
        "/* c */ INSERT INTO t VALUES (1)",
    ):
        assert _is_insert_or_replace(sql), sql
    for sql in (
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "CREATE TABLE t (x INT)",
        "DROP TABLE t",
        "SELECT * FROM t",
        "WITH cte AS (SELECT 1) INSERT INTO t SELECT * FROM cte",
        "COMMIT",
    ):
        assert not _is_insert_or_replace(sql), sql
