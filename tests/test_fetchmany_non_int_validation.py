"""``Cursor.fetchmany(size)`` rejects non-int/bool with a ``dbapi.Error`` subclass (PEP 249 §7)."""

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


def _sync_cursor() -> Cursor:
    conn = Connection("localhost:19001", timeout=2.0)
    cur = Cursor(conn)
    # Prime an active result set so the type-check fires before _check_result_set.
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = []
    cur._row_index = 0
    return cur


def _async_cursor() -> AsyncCursor:
    conn = AsyncConnection("localhost:19001")
    cur = AsyncCursor(conn)
    cur._description = (("a", None, None, None, None, None, None),)
    cur._rows = []
    cur._row_index = 0
    return cur


@pytest.mark.parametrize(
    "bad_size,bad_type",
    [
        (1.5, "float"),
        ("3", "str"),
        (True, "bool"),
        (False, "bool"),
    ],
)
def test_fetchmany_rejects_non_int_sync(bad_size: object, bad_type: str) -> None:
    cur = _sync_cursor()
    with pytest.raises(ProgrammingError, match=bad_type):
        cur.fetchmany(bad_size)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "bad_size,bad_type",
    [
        (1.5, "float"),
        ("3", "str"),
        (True, "bool"),
        (False, "bool"),
    ],
)
async def test_fetchmany_rejects_non_int_async(bad_size: object, bad_type: str) -> None:
    cur = _async_cursor()
    with pytest.raises(ProgrammingError, match=bad_type):
        await cur.fetchmany(bad_size)  # type: ignore[arg-type]


def test_fetchmany_none_uses_arraysize_sync() -> None:
    """None routes to ``self._arraysize``; validation applies only to non-None non-int."""
    cur = _sync_cursor()
    cur.arraysize = 5
    rows = cur.fetchmany()
    assert rows == []
