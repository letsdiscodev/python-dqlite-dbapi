"""Pin: ``AsyncConnection.backup`` is plain ``def``, not ``async
def`` — so a forgotten ``await`` raises ``NotSupportedError`` on
the call line rather than producing a discarded coroutine that
warns "coroutine was never awaited" at GC.

ISSUE-Sym4 applied this discipline to ``executescript`` but
deferred ``backup``; this is the symmetric pin.
"""

import inspect

import pytest

import dqlitedbapi.aio
from dqlitedbapi.exceptions import NotSupportedError


def test_async_backup_is_not_async_def() -> None:
    """Inspect the method directly: ``backup`` must be a plain
    function, not a coroutine function."""
    method = dqlitedbapi.aio.AsyncConnection.backup
    assert not inspect.iscoroutinefunction(method), (
        "AsyncConnection.backup is async def — a forgotten `await` "
        "would silently produce a discarded coroutine. Match the "
        "ISSUE-Sym4 discipline applied to executescript."
    )


def test_async_backup_raises_immediately_without_await() -> None:
    """Calling without await should raise NotSupportedError on
    the call line — not silently produce a coroutine."""
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(NotSupportedError):
        aconn.backup(None)
