"""Pin ``row_factory``'s attribute and property-getter annotation to
the shared ``RowFactory`` alias.

The setter already validates ``callable(value) or value is None`` at
runtime, so the public contract is ``RowFactory | None`` (where
``RowFactory`` is the ``Callable[..., Any]`` alias in
``dqlitedbapi.types``). The storage attribute and the property getter
previously used the looser ``Any`` shape, which propagated through the
row-transform call sites and defeated mypy's ability to catch internal
mis-assignments.

Regression guard: if anyone re-widens to ``Any`` for ergonomics, this
test surfaces the contract drift.
"""

from __future__ import annotations

from typing import get_type_hints

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import Connection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.types import RowFactory


def _assert_row_factory_or_none(annotation: object, label: str) -> None:
    expected = RowFactory | None
    assert annotation == expected, (
        f"{label} must be ``RowFactory | None`` (alias of "
        f"``Callable[..., Any]``); got {annotation!r}"
    )


def test_sync_connection_row_factory_getter_annotation() -> None:
    hints = get_type_hints(Connection.row_factory.fget)  # type: ignore[attr-defined]
    _assert_row_factory_or_none(hints["return"], "Connection.row_factory")


def test_sync_cursor_row_factory_getter_annotation() -> None:
    hints = get_type_hints(Cursor.row_factory.fget)  # type: ignore[attr-defined]
    _assert_row_factory_or_none(hints["return"], "Cursor.row_factory")


def test_async_connection_row_factory_getter_annotation() -> None:
    hints = get_type_hints(AsyncConnection.row_factory.fget)  # type: ignore[attr-defined]
    _assert_row_factory_or_none(hints["return"], "AsyncConnection.row_factory")


def test_async_cursor_row_factory_getter_annotation() -> None:
    hints = get_type_hints(AsyncCursor.row_factory.fget)  # type: ignore[attr-defined]
    _assert_row_factory_or_none(hints["return"], "AsyncCursor.row_factory")


def test_row_factory_alias_resolves_to_callable() -> None:
    """The alias itself must resolve to a callable shape (``Callable[..., Any]``).
    Pins the underlying signature: if a future refactor swaps the alias
    for a tighter ``Callable[[Cursor, tuple], Any]`` shape, callers
    that relied on the looser arity would break — this test surfaces
    the change at a single site.
    """
    # PEP 695 ``type X = ...`` produces a ``TypeAliasType``; resolve
    # the underlying expression via ``__value__``.
    underlying = RowFactory.__value__
    assert "Callable" in str(underlying), (
        f"RowFactory alias must resolve to a Callable shape; got {underlying!r}"
    )
