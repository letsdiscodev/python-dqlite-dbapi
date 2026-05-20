"""Pin: ``Cursor.setinputsizes`` / ``setoutputsize`` / ``callproc`` /
``scroll`` accept their arguments positional-only, matching stdlib
``sqlite3`` cursor's signature discipline.

The primary cursor methods ``execute`` / ``executemany`` /
``executescript`` already carry the ``/`` marker. Stdlib `sqlite3`
makes every secondary cursor method positional-only too — a
``setoutputsize(size=10)`` raises ``TypeError`` on stdlib. The dqlite
driver previously omitted ``/`` on the four secondary methods,
silently accepting kwargs (including misspelled ones) instead of
fail-fast TypeErrors.

Apply to both sync and async cursors.
"""

from __future__ import annotations

import inspect

import pytest

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor

_METHOD_NAMES = ["setinputsizes", "setoutputsize", "callproc", "scroll"]


@pytest.mark.parametrize("method_name", _METHOD_NAMES)
def test_sync_secondary_method_uses_positional_only_marker(method_name: str) -> None:
    """Every parameter on the sync sibling must be ``POSITIONAL_ONLY``
    (matching the ``execute`` / ``executemany`` / ``executescript``
    discipline and stdlib `sqlite3`)."""
    sig = inspect.signature(getattr(Cursor, method_name))
    # ``self`` is POSITIONAL_OR_KEYWORD via descriptor binding; the
    # rest of the parameters must be POSITIONAL_ONLY.
    rest = [p for name, p in sig.parameters.items() if name != "self"]
    assert all(p.kind is inspect.Parameter.POSITIONAL_ONLY for p in rest), (
        f"Cursor.{method_name} parameters must be POSITIONAL_ONLY for stdlib "
        f"sqlite3 parity. Got: {[(p.name, p.kind.description) for p in rest]}"
    )


@pytest.mark.parametrize("method_name", _METHOD_NAMES)
def test_async_secondary_method_uses_positional_only_marker(method_name: str) -> None:
    """Same discipline on the async sibling."""
    sig = inspect.signature(getattr(AsyncCursor, method_name))
    rest = [p for name, p in sig.parameters.items() if name != "self"]
    assert all(p.kind is inspect.Parameter.POSITIONAL_ONLY for p in rest), (
        f"AsyncCursor.{method_name} parameters must be POSITIONAL_ONLY"
    )


def test_setoutputsize_kwarg_raises_type_error() -> None:
    """End-to-end: a kwarg form raises TypeError, matching stdlib."""
    from unittest.mock import MagicMock

    from dqlitedbapi import Connection

    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._connection = MagicMock(spec=Connection)
    with pytest.raises(TypeError, match="positional-only"):
        cur.setoutputsize(10, column=0)  # type: ignore[misc]


def test_setinputsizes_kwarg_raises_type_error() -> None:
    """``setinputsizes(sizes=...)`` must TypeError."""
    from unittest.mock import MagicMock

    from dqlitedbapi import Connection

    cur = Cursor.__new__(Cursor)
    cur._closed = False
    cur._connection = MagicMock(spec=Connection)
    with pytest.raises(TypeError, match="positional-only"):
        cur.setinputsizes(sizes=[])  # type: ignore[misc]
