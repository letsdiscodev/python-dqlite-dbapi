"""``AsyncConnection.backup`` is plain ``def``, not ``async def``, so a forgotten
``await`` raises ``NotSupportedError`` on the call line instead of a discarded coroutine."""

import inspect

import pytest

import dqlitedbapi.aio
from dqlitedbapi.exceptions import NotSupportedError


def test_async_backup_is_not_async_def() -> None:
    method = dqlitedbapi.aio.AsyncConnection.backup
    assert not inspect.iscoroutinefunction(method), (
        "AsyncConnection.backup is async def — a forgotten `await` "
        "would silently produce a discarded coroutine. Match the "
        "plain-def discipline applied to executescript."
    )


def test_async_backup_raises_immediately_without_await() -> None:
    aconn = dqlitedbapi.aio.AsyncConnection("localhost:9001")
    with pytest.raises(NotSupportedError):
        aconn.backup(None)
