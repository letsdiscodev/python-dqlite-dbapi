"""Pin ``row_factory``'s getter annotation to ``RowFactory | None`` so a
re-widening to ``Any`` (which defeats mypy on the call sites) is caught."""

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
    """The alias must resolve to a ``Callable`` shape."""
    # PEP 695 ``type X = ...`` yields a TypeAliasType; unwrap via __value__.
    underlying = RowFactory.__value__
    assert "Callable" in str(underlying), (
        f"RowFactory alias must resolve to a Callable shape; got {underlying!r}"
    )
