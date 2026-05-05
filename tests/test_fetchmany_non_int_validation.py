"""``Cursor.fetchmany(size)`` validates ``size`` is int or None.

Pre-fix non-int / bool slipped past the value check and produced
either a silent truncation (``range(1.5)`` → 1 iteration) or a
bare ``TypeError`` outside ``dbapi.Error``. PEP 249 §7 requires
cursor methods to raise ``dbapi.Error`` subclasses.

Bool is rejected explicitly because ``True`` silently coerces to
1 (caller-bug trap, not a useful affordance) — same shape as the
``arraysize.setter`` rejection.
"""

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import ProgrammingError


def _sync_cursor() -> Cursor:
    conn = Connection("localhost:19001", timeout=2.0)
    cur = Cursor(conn)
    # Pretend a result set is active so the type-check fires before
    # _check_result_set; here we pre-populate the internal state.
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


@pytest.mark.asyncio
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
    """None still routes to ``self._arraysize`` — the validation only
    applies to non-None non-int values."""
    cur = _sync_cursor()
    cur.arraysize = 5
    rows = cur.fetchmany()  # None → arraysize
    assert rows == []
