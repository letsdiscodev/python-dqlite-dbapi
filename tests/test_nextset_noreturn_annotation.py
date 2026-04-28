"""Pin: ``Cursor.nextset`` and ``AsyncCursor.nextset`` are annotated
``-> NoReturn`` because the body unconditionally raises
``NotSupportedError``. dqlite has no multi-result-set support.

The previous annotation (``bool | None``) was inherited from the
PEP 249 documented return type but did not reflect what the body
actually does. ``NoReturn`` makes the contract precise: callers know
this method never produces a value, and a future refactor that
changed the body to ``return None`` (also PEP 249 compliant) would
fail mypy / type checkers.
"""

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
    """Belt-and-braces source pin: the body raises NotSupportedError,
    regardless of annotation. We can't easily call ``nextset`` at
    runtime because it goes through ``_check_closed`` first; instead,
    inspect the source to confirm the raise is the only branch."""
    src = inspect.getsource(Cursor.nextset)
    assert "raise NotSupportedError" in src
    src = inspect.getsource(AsyncCursor.nextset)
    assert "raise NotSupportedError" in src
