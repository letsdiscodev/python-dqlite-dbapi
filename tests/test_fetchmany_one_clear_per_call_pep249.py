"""``fetchmany`` clears ``cur.messages`` once at the call boundary, not per inner row."""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_fetchmany_clears_messages_only_once() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    try:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS fm_pin")
        cur.execute("CREATE TABLE fm_pin (n INTEGER)")
        cur.executemany("INSERT INTO fm_pin VALUES (?)", [(i,) for i in range(5)])
        cur.execute("SELECT n FROM fm_pin ORDER BY n")
        rows = cur.fetchmany(2)
        assert len(rows) == 2
        cur.messages.append(("Warning", "sentinel"))  # type: ignore[arg-type]
        more = cur.fetchmany(2)
        assert len(more) == 2
        # The sentinel is wiped by the prelude clear (correct); the real pin is structural below.
    finally:
        conn.close()


def test_sync_fetchmany_loop_body_uses_unlocked_helper() -> None:
    """``fetchmany``'s loop calls ``_next_row_unlocked``, not ``fetchone``."""
    import ast
    import inspect
    import textwrap

    from dqlitedbapi import cursor as sync_cur_mod

    src = textwrap.dedent(inspect.getsource(sync_cur_mod.Cursor.fetchmany))
    tree = ast.parse(src)
    found_unlocked_call = False
    for node in ast.walk(tree):
        if isinstance(node, ast.For):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute):
                    if sub.func.attr == "_next_row_unlocked":
                        found_unlocked_call = True
                    assert sub.func.attr != "fetchone", (
                        "Cursor.fetchmany must not call self.fetchone() in "
                        "the loop — that re-clears messages and re-runs "
                        "guards per-row. Use _next_row_unlocked instead."
                    )
    assert found_unlocked_call, "Cursor.fetchmany loop must call self._next_row_unlocked()."


def test_async_fetchmany_loop_body_uses_unlocked_helper() -> None:
    """Structural pin (async sibling)."""
    import ast
    import inspect
    import textwrap

    from dqlitedbapi.aio import cursor as aio_cur_mod

    src = textwrap.dedent(inspect.getsource(aio_cur_mod.AsyncCursor.fetchmany))
    tree = ast.parse(src)
    found_unlocked_call = False
    for node in ast.walk(tree):
        if isinstance(node, ast.For):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute):
                    if sub.func.attr == "_next_row_unlocked":
                        found_unlocked_call = True
                    assert sub.func.attr != "fetchone", (
                        "AsyncCursor.fetchmany must not call self.fetchone() "
                        "in the loop — that re-clears messages and re-runs "
                        "guards per-row. Use _next_row_unlocked instead."
                    )
    assert found_unlocked_call, "AsyncCursor.fetchmany loop must call self._next_row_unlocked()."


async def test_async_fetchmany_returns_correct_rows() -> None:
    """Regression guard: the refactor must not break basic delivery."""
    conn = AsyncConnection("localhost:9001")
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS afm_pin")
        await cur.execute("CREATE TABLE afm_pin (n INTEGER)")
        await cur.executemany("INSERT INTO afm_pin VALUES (?)", [(i,) for i in range(7)])
        await cur.execute("SELECT n FROM afm_pin ORDER BY n")
        first = await cur.fetchmany(3)
        assert [r[0] for r in first] == [0, 1, 2]
        rest = await cur.fetchall()
        assert [r[0] for r in rest] == [3, 4, 5, 6]
    finally:
        await conn.close()
