"""Pin: ``Cursor.rownumber`` / ``AsyncCursor.rownumber`` return
``None`` on a closed cursor (not ``Error``), matching the bypass
discipline of sibling read-only accessors ``description`` /
``rowcount``.

The PEP 249 §6.1.2 strict reading would require an ``Error`` raise on
closed-cursor access. The dbapi diverges deliberately because
``close()`` scrubs ``_description`` to ``None`` and the property
short-circuits at the same branch as a never-executed cursor.

This test pins the deliberate behaviour so a future
"raise on closed" refactor cannot land silently; the closed-cursor
ambiguity is documented in the property's docstring.
"""

from unittest.mock import MagicMock

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def _bare_sync_cursor() -> Cursor:
    conn = MagicMock()
    conn._closed = False
    conn._check_thread = lambda: None
    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._connection = conn
    cur._rows = []
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    return cur


def _bare_async_cursor() -> AsyncCursor:
    conn = MagicMock()
    conn._closed = False
    conn._check_loop_only = lambda: None
    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._connection = conn
    cur._executing_task = None
    cur._rows = []
    cur._description = None
    cur._rowcount = -1
    cur._lastrowid = None
    cur._row_index = 0
    cur._arraysize = 1
    cur.messages = []
    return cur


def test_sync_rownumber_returns_none_on_closed_cursor() -> None:
    cur = _bare_sync_cursor()
    cur.close()
    assert cur._closed is True
    assert cur.rownumber is None


@pytest.mark.asyncio
async def test_async_rownumber_returns_none_on_closed_cursor() -> None:
    cur = _bare_async_cursor()
    cur.close()
    assert cur._closed is True
    assert cur.rownumber is None


def test_rownumber_docstring_directs_callers_to_cur_closed() -> None:
    """The closed-state alias is deliberate but operator-confusing;
    the docstring must direct callers to the public ``cur.closed``
    gate before consulting ``rownumber`` if the distinction between
    "closed" and "no result set" matters."""
    doc = Cursor.rownumber.__doc__ or ""
    assert "cur.closed" in doc, (
        "rownumber docstring must reference cur.closed as the explicit gate for "
        "distinguishing 'closed cursor' from 'no result set'"
    )
    assert "forward-compat" in doc.lower(), (
        "rownumber docstring must warn that a future major version may switch to "
        "raising on closed; callers should use cur.closed today"
    )
