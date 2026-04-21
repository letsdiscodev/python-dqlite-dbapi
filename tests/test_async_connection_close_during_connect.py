"""Concurrent close() during first-use _build_and_connect must not leak.

``AsyncConnection.close()`` early-returns on ``_async_conn is None``
without acquiring any lock. ``_ensure_connection`` is the only place
that assigns ``self._async_conn``; it performs the assignment after
an ``await _build_and_connect(...)`` suspend. If ``close()`` lands in
that window, it flips ``_closed=True`` and returns — and when the
first-use task resumes, it installs a live connection into an object
whose caller has already released it. The underlying
``DqliteConnection`` (and its socket + reader task + server-side
session) leaks until GC.

The fix is to re-check ``self._closed`` under ``connect_lock`` after
the build completes; on close-in-flight, close the freshly built
connection and raise ``InterfaceError``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import InterfaceError


class TestCloseDuringEnsureConnection:
    async def test_close_during_build_closes_fresh_connection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """If close() runs while _build_and_connect is suspended, the
        freshly built underlying connection must be closed, not leaked
        into a closed wrapper.
        """
        built_connection = AsyncMock()
        built_connection.close = AsyncMock()
        build_has_started = asyncio.Event()
        may_build_finish = asyncio.Event()

        async def _fake_build(*args: object, **kwargs: object) -> AsyncMock:
            build_has_started.set()
            await may_build_finish.wait()
            return built_connection

        monkeypatch.setattr(
            "dqlitedbapi.aio.connection._build_and_connect",
            _fake_build,
        )

        conn = AsyncConnection("localhost:19001")

        async def open_it() -> None:
            with pytest.raises(InterfaceError, match="closed"):
                await conn._ensure_connection()

        open_task = asyncio.create_task(open_it())
        await build_has_started.wait()

        # Call close() while _build_and_connect is suspended.
        await conn.close()
        assert conn._closed is True

        # Now let the build finish.
        may_build_finish.set()
        await open_task

        # The freshly built connection must have been closed.
        built_connection.close.assert_awaited()
        # It must NOT have been installed into _async_conn.
        assert conn._async_conn is None
