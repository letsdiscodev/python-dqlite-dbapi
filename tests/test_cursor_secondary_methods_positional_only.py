"""``Cursor`` secondary methods (setinputsizes / setoutputsize / callproc / scroll) are
positional-only on both sync and async, matching stdlib ``sqlite3`` (kwargs -> TypeError).
"""

from __future__ import annotations

import inspect

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor

_METHOD_NAMES = ["setinputsizes", "setoutputsize", "callproc", "scroll"]


@pytest.mark.parametrize("method_name", _METHOD_NAMES)
def test_sync_secondary_method_uses_positional_only_marker(method_name: str) -> None:
    sig = inspect.signature(getattr(Cursor, method_name))
    # ``self`` is POSITIONAL_OR_KEYWORD via descriptor binding; the rest must be POSITIONAL_ONLY.
    rest = [p for name, p in sig.parameters.items() if name != "self"]
    assert all(p.kind is inspect.Parameter.POSITIONAL_ONLY for p in rest), (
        f"Cursor.{method_name} parameters must be POSITIONAL_ONLY for stdlib "
        f"sqlite3 parity. Got: {[(p.name, p.kind.description) for p in rest]}"
    )


@pytest.mark.parametrize("method_name", _METHOD_NAMES)
def test_async_secondary_method_uses_positional_only_marker(method_name: str) -> None:
    sig = inspect.signature(getattr(AsyncCursor, method_name))
    rest = [p for name, p in sig.parameters.items() if name != "self"]
    assert all(p.kind is inspect.Parameter.POSITIONAL_ONLY for p in rest), (
        f"AsyncCursor.{method_name} parameters must be POSITIONAL_ONLY"
    )


def test_setoutputsize_kwarg_raises_type_error() -> None:
    from unittest.mock import MagicMock

    from dqlitedbapi import Connection

    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._connection = MagicMock(spec=Connection)
    with pytest.raises(TypeError, match="positional-only"):
        cur.setoutputsize(10, column=0)  # type: ignore[call-arg]


def test_setinputsizes_kwarg_raises_type_error() -> None:
    from unittest.mock import MagicMock

    from dqlitedbapi import Connection

    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._connection = MagicMock(spec=Connection)
    with pytest.raises(TypeError, match="positional-only"):
        cur.setinputsizes(sizes=[])  # type: ignore[call-arg]


async def test_async_setoutputsize_kwarg_raises_type_error() -> None:
    """Async kwarg form raises TypeError; guards against a ``**kwargs`` regression that
    would defeat the POSITIONAL_ONLY inspection pin on the async side only."""
    from unittest.mock import MagicMock

    from dqlitedbapi.aio import AsyncConnection
    from dqlitedbapi.aio.cursor import AsyncCursor

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._connection = MagicMock(spec=AsyncConnection)
    cur.messages = []
    with pytest.raises(TypeError, match="positional-only"):
        cur.setoutputsize(10, column=0)  # type: ignore[call-arg]


async def test_async_setinputsizes_kwarg_raises_type_error() -> None:
    from unittest.mock import MagicMock

    from dqlitedbapi.aio import AsyncConnection
    from dqlitedbapi.aio.cursor import AsyncCursor

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._connection = MagicMock(spec=AsyncConnection)
    cur.messages = []
    with pytest.raises(TypeError, match="positional-only"):
        cur.setinputsizes(sizes=[])  # type: ignore[call-arg]


async def test_async_scroll_kwarg_raises_type_error() -> None:
    from unittest.mock import MagicMock

    from dqlitedbapi.aio import AsyncConnection
    from dqlitedbapi.aio.cursor import AsyncCursor

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._connection = MagicMock(spec=AsyncConnection)
    cur.messages = []
    with pytest.raises(TypeError, match="positional-only"):
        cur.scroll(0, mode="relative")  # type: ignore[call-arg]


async def test_async_callproc_kwarg_raises_type_error() -> None:
    from unittest.mock import MagicMock

    from dqlitedbapi.aio import AsyncConnection
    from dqlitedbapi.aio.cursor import AsyncCursor

    cur = AsyncCursor.__new__(AsyncCursor)
    cur._closed = False
    cur._connection = MagicMock(spec=AsyncConnection)
    cur.messages = []
    with pytest.raises(TypeError, match="positional-only"):
        cur.callproc("proc", parameters=())  # type: ignore[call-arg]
