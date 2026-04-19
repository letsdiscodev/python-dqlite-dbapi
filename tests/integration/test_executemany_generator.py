"""executemany must accept any iterable — including generators — per PEP 249.

The outer argument's type annotation is ``Iterable[Sequence[Any]]``; the
runtime loop uses only the iteration protocol. Regression guard so a
future re-narrowing to ``Sequence[Sequence[Any]]`` trips a test.
"""

from collections.abc import Iterator

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect


def _row_generator(items: list[str]) -> Iterator[tuple[str]]:
    for x in items:
        yield (x,)


@pytest.mark.integration
def test_sync_executemany_accepts_generator(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS emany_gen")
        cur.execute("CREATE TABLE emany_gen (id INTEGER PRIMARY KEY, x TEXT)")
        cur.executemany("INSERT INTO emany_gen (x) VALUES (?)", _row_generator(["a", "b", "c"]))
        assert cur.rowcount == 3
        cur.execute("SELECT x FROM emany_gen ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == ["a", "b", "c"]
        conn.rollback()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_executemany_accepts_generator(cluster_address: str) -> None:
    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS emany_gen_async")
        await cur.execute("CREATE TABLE emany_gen_async (id INTEGER PRIMARY KEY, x TEXT)")
        await cur.executemany(
            "INSERT INTO emany_gen_async (x) VALUES (?)",
            _row_generator(["x", "y", "z", "w"]),
        )
        assert cur.rowcount == 4
        await cur.execute("SELECT x FROM emany_gen_async ORDER BY id")
        rows = await cur.fetchall()
        assert [r[0] for r in rows] == ["x", "y", "z", "w"]
        await conn.rollback()
    finally:
        await conn.close()
