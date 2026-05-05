"""Pin: ``Connection.cursor()`` rejects unknown kwargs (notably
stdlib's ``factory=`` Cursor-subclass hook) with
``NotSupportedError`` rather than letting Python raise a bare
``TypeError`` outside the ``dbapi.Error`` hierarchy.

Symmetric with the connect()-time ``**unknown_kwargs`` rejection
introduced by ISSUE-Q4/Q5/Q6 — covers the cursor-creation surface
that ISSUE-Q6 originally framed.
"""

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.exceptions import NotSupportedError


def test_sync_cursor_rejects_factory_kwarg() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        with pytest.raises(NotSupportedError, match="factory"):
            conn.cursor(factory=object)
    finally:
        conn.close()


def test_sync_cursor_rejects_arbitrary_unknown_kwarg() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        with pytest.raises(NotSupportedError):
            conn.cursor(unknown=True)
    finally:
        conn.close()


def test_sync_cursor_no_kwargs_still_works() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    try:
        cur = conn.cursor()
        assert cur is not None
        cur.close()
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_async_cursor_rejects_factory_kwarg() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(NotSupportedError, match="factory"):
        aconn.cursor(factory=object)


@pytest.mark.asyncio
async def test_async_cursor_no_kwargs_still_works() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    cur = aconn.cursor()
    assert cur is not None
