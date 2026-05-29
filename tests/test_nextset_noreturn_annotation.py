"""Pin: ``nextset`` is annotated ``-> NoReturn`` (body always raises)."""

from __future__ import annotations

import inspect
import typing

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def test_sync_cursor_nextset_return_annotation_is_noreturn() -> None:
    sig = inspect.signature(Cursor.nextset)
    assert sig.return_annotation is typing.NoReturn, (
        "Cursor.nextset always raises; annotate as NoReturn so a "
        "future refactor that returns None is caught by mypy."
    )


def test_async_cursor_nextset_return_annotation_is_noreturn() -> None:
    sig = inspect.signature(AsyncCursor.nextset)
    assert sig.return_annotation is typing.NoReturn


def test_nextset_body_raises_not_supported_error_unconditionally() -> None:
    """Source pin: the body raises NotSupportedError regardless of annotation."""
    src = inspect.getsource(Cursor.nextset)
    assert "raise NotSupportedError" in src
    src = inspect.getsource(AsyncCursor.nextset)
    assert "raise NotSupportedError" in src
