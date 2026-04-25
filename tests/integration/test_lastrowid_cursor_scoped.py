"""``lastrowid`` is cursor-scoped, not connection-scoped.

stdlib ``sqlite3.Cursor.lastrowid`` is connection-scoped: every
cursor on the connection sees the same ``last_insert_rowid()`` from
the underlying SQLite handle. The dqlite dbapi deliberately diverges
— each cursor's ``lastrowid`` reflects only INSERTs run on THAT
cursor, leaving sibling cursors at None until they do their own
INSERT.

Both behaviours are PEP 249 compliant; the dqlite contract is
documented but unpinned. A future refactor to "match stdlib"
semantics would silently change user-visible behaviour for any
caller that holds two cursors per connection inside a transaction.

Pin the cursor-scoped invariant so the deliberate design choice is
loud against drift.
"""

from __future__ import annotations

import pytest

import dqlitedbapi


@pytest.mark.integration
def test_sync_lastrowid_is_cursor_scoped_not_connection_scoped(
    cluster_address: str,
) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur1 = conn.cursor()
        cur2 = conn.cursor()
        cur1.execute("DROP TABLE IF EXISTS test_lastrowid_scoped")
        cur1.execute("CREATE TABLE test_lastrowid_scoped (id INTEGER PRIMARY KEY, x TEXT)")
        conn.commit()

        # Sibling cursor with no prior INSERT must see lastrowid=None.
        assert cur2.lastrowid is None

        cur1.execute("BEGIN")
        cur1.execute("INSERT INTO test_lastrowid_scoped (x) VALUES ('a')")
        first_rowid = cur1.lastrowid
        assert first_rowid is not None
        # cur2 (no INSERT yet) still sees None.
        assert cur2.lastrowid is None

        cur2.execute("INSERT INTO test_lastrowid_scoped (x) VALUES ('b')")
        second_rowid = cur2.lastrowid
        assert second_rowid is not None
        assert second_rowid != first_rowid
        # cur1's lastrowid is unchanged by cur2's INSERT — cursor-scoped.
        assert cur1.lastrowid == first_rowid

        conn.commit()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_lastrowid_is_cursor_scoped_not_connection_scoped(
    cluster_address: str,
) -> None:
    from dqlitedbapi.aio import aconnect

    conn = await aconnect(cluster_address)
    try:
        cur1 = conn.cursor()
        cur2 = conn.cursor()
        await cur1.execute("DROP TABLE IF EXISTS test_async_lastrowid_scoped")
        await cur1.execute(
            "CREATE TABLE test_async_lastrowid_scoped (id INTEGER PRIMARY KEY, x TEXT)"
        )
        await conn.commit()

        assert cur2.lastrowid is None

        await cur1.execute("BEGIN")
        await cur1.execute("INSERT INTO test_async_lastrowid_scoped (x) VALUES ('a')")
        first_rowid = cur1.lastrowid
        assert first_rowid is not None
        assert cur2.lastrowid is None

        await cur2.execute("INSERT INTO test_async_lastrowid_scoped (x) VALUES ('b')")
        second_rowid = cur2.lastrowid
        assert second_rowid is not None
        assert second_rowid != first_rowid
        assert cur1.lastrowid == first_rowid

        await conn.commit()
    finally:
        await conn.close()
