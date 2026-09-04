"""Concurrent close() during first-use _build_and_connect must not leak."""

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
        """Close during a suspended build must close the fresh connection, not leak it."""
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

        may_build_finish.set()
        await open_task

        built_connection.close.assert_awaited()
        assert conn._async_conn is None
