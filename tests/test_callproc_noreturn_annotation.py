"""Pin: ``Cursor.callproc`` and ``AsyncCursor.callproc`` are annotated
``-> NoReturn`` because the body unconditionally raises
``NotSupportedError``. dqlite (and SQLite) have no stored-procedure
concept.

Symmetric with the same fix applied to ``nextset`` (see
``test_nextset_noreturn_annotation.py``). ``Sequence[Any] | None``
is the PEP 249 documented return type but does not reflect what the
body actually does. ``NoReturn`` makes the contract precise: callers
know this method never produces a value, and a future refactor that
changed the body to ``return None`` would fail mypy / type checkers.
"""

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
    """Belt-and-braces source pin: the body raises NotSupportedError,
    regardless of annotation. ``callproc`` is gated by
    ``_check_thread`` / ``_check_closed`` first, so we inspect the
    source rather than driving a runtime call."""
    src = inspect.getsource(Cursor.callproc)
    assert "raise NotSupportedError" in src
    src = inspect.getsource(AsyncCursor.callproc)
    assert "raise NotSupportedError" in src
