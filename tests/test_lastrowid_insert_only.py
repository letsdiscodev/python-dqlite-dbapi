"""``Cursor.lastrowid`` updates only on successful INSERT / REPLACE,
matching stdlib ``sqlite3.Cursor.lastrowid``. UPDATE / DELETE / DDL
leave the previous INSERT's rowid in place.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import Connection


@pytest.mark.asyncio
async def test_lastrowid_sticky_across_update_and_delete(monkeypatch) -> None:
    """Simulate a realistic INSERT → UPDATE → DELETE sequence where
    the wire returns 42, then 0, then 0. The cursor must expose 42
    throughout, matching stdlib sqlite3."""
    conn = Connection("127.0.0.1:9001")
    cur = conn.cursor()

    # Patch the async client's ``execute`` to return the sequence
    # of ``(last_insert_id, affected)`` the wire would produce.
    class _FakeAsyncConn:
        def __init__(self) -> None:
            self.calls = 0
            self.responses = [(42, 1), (0, 1), (0, 1)]

        async def execute(self, sql: str, params=None):
            idx = self.calls
            self.calls += 1
            return self.responses[idx]

    fake = _FakeAsyncConn()

    # Cursor calls conn._get_async_connection(); patch it to return fake.
    async def _fake_get_async() -> _FakeAsyncConn:
        return fake

    conn._get_async_connection = _fake_get_async  # type: ignore[method-assign]

    # Drive the cursor through the sync public API.
    cur.execute("INSERT INTO t (v) VALUES (?)", ("x",))
    assert cur.lastrowid == 42
    cur.execute("UPDATE t SET v = 'y' WHERE id = 42")
    # Sticky — did not get zeroed by the UPDATE wire response.
    assert cur.lastrowid == 42
    cur.execute("DELETE FROM t WHERE id = 42")
    assert cur.lastrowid == 42


def test_is_insert_or_replace_prefix_detection() -> None:
    """Low-level helper pin: detect INSERT / INSERT OR REPLACE /
    INSERT OR IGNORE / REPLACE; reject UPDATE / DELETE / DDL / WITH."""
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
