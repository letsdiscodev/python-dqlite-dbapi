"""``callproc`` is annotated ``-> NoReturn``: the body always raises
``NotSupportedError`` (no stored-procedure concept in dqlite/SQLite)."""

from __future__ import annotations

import inspect
import typing

from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.cursor import Cursor


def test_sync_cursor_callproc_return_annotation_is_noreturn() -> None:
    sig = inspect.signature(Cursor.callproc)
    assert sig.return_annotation is typing.NoReturn, (
        "Cursor.callproc always raises; annotate as NoReturn so a "
        "future refactor that returns None is caught by mypy."
    )


def test_async_cursor_callproc_return_annotation_is_noreturn() -> None:
    sig = inspect.signature(AsyncCursor.callproc)
    assert sig.return_annotation is typing.NoReturn


def test_callproc_body_raises_not_supported_error_unconditionally() -> None:
    """Source pin: body raises NotSupportedError (gated by thread/closed
    checks first, so inspect source rather than driving a call)."""
    src = inspect.getsource(Cursor.callproc)
    assert "raise NotSupportedError" in src
    src = inspect.getsource(AsyncCursor.callproc)
    assert "raise NotSupportedError" in src
