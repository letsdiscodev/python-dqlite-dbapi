"""close() acquires _op_lock so in-flight operations drain before teardown."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


async def test_close_waits_for_in_flight_execute() -> None:
    conn = AsyncConnection("localhost:19001", database="x")

    order: list[str] = []

    async def slow_execute(_sql: str, _params: object) -> tuple[int, int]:
        order.append("execute:start")
        await asyncio.sleep(0.05)
        order.append("execute:end")
        return (0, 0)

    async def fake_query_raw_typed(_sql: str, _params: object) -> tuple[list, list, list, list]:  # type: ignore[type-arg]
        return ([], [], [], [])

    with (
        patch(
            "dqlitedbapi.connection._resolve_leader",
            new=AsyncMock(side_effect=lambda address, *, timeout, **_kw: address),
        ),
        patch("dqlitedbapi.connection.DqliteConnection") as MockDqliteConn,
    ):
        mock_instance = AsyncMock()
        mock_instance.connect = AsyncMock()
        mock_instance.execute = slow_execute
        mock_instance.query_raw_typed = fake_query_raw_typed

        async def fake_close() -> None:
            order.append("close:start")
            order.append("close:end")

        mock_instance.close = fake_close
        MockDqliteConn.return_value = mock_instance

        await conn.connect()

        async def run_execute() -> None:
            cursor = conn.cursor()
            await cursor.execute("INSERT INTO t VALUES (1)")

        async def run_close() -> None:
            await asyncio.sleep(0.01)  # let execute start first
            await conn.close()

        await asyncio.gather(run_execute(), run_close())

    assert order == [
        "execute:start",
        "execute:end",
        "close:start",
        "close:end",
    ], f"actual order: {order}"


class TestCommitRollbackCloseRace:
    """close() winning op_lock first makes parked commit/rollback raise
    InterfaceError, not deref _async_conn=None."""

    async def test_commit_parked_on_lock_sees_close_first(self) -> None:
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from dqlitedbapi.aio.connection import AsyncConnection
        from dqlitedbapi.exceptions import InterfaceError

        conn = AsyncConnection("localhost:19001", database="x")

        # Prime locks from the running loop so close()/commit() take the lock path.
        conn._ensure_locks()
        inner = MagicMock()
        inner.close = AsyncMock()
        inner.execute = AsyncMock()
        conn._async_conn = inner

        assert conn._op_lock is not None
        close_release = asyncio.Event()

        real_close = inner.close

        async def slow_close(*args: object, **kwargs: object) -> None:
            await close_release.wait()
            await real_close(*args, **kwargs)

        inner.close = slow_close

        close_task = asyncio.create_task(conn.close())
        # Yield so close() acquires op_lock before commit() parks on it.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        commit_task = asyncio.create_task(conn.commit())
        await asyncio.sleep(0)
        close_release.set()

        await close_task
        with pytest.raises(InterfaceError, match="Connection is closed"):
            await commit_task
