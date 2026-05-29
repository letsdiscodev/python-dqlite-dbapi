"""Pin: ``_ensure_connection`` shields the post-build ``built.close()`` against outer cancel.

An unshielded close interrupted by a cancel cascade (e.g. ``engine.dispose()``) leaks the
freshly-built transport's socket and reader Task.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
from typing import Any, cast
from unittest.mock import patch

import dqlitedbapi.aio
from dqlitedbapi.exceptions import InterfaceError


def _bare_async_connection() -> Any:
    """Build an ``AsyncConnection`` without ``__init__`` so no real address/loop is needed."""
    aconn = cast(Any, dqlitedbapi.aio.AsyncConnection.__new__(dqlitedbapi.aio.AsyncConnection))
    aconn._closed = False
    aconn._closed_flag = [False]
    aconn._connected_flag = [False]
    aconn._async_conn = None
    aconn._creator_pid = os.getpid()
    aconn._creator_thread = threading.get_ident()
    aconn._loop_ref = None
    aconn._connect_lock = None
    aconn._op_lock = None
    aconn._address = "h:9001"
    aconn._database = "default"
    aconn._timeout = 1.0
    aconn._max_total_rows = None
    aconn._max_continuation_frames = None
    aconn._trust_server_heartbeat = False
    aconn._close_timeout = 0.5
    aconn.messages = []
    return aconn


class _SyntheticBuilt:
    """Stand-in for the built ``DqliteConnection``; ``close()`` waits on a test-gated Event."""

    def __init__(self, gate: asyncio.Event) -> None:
        self._gate = gate
        self.close_completed = False

    async def close(self) -> None:
        await self._gate.wait()
        self.close_completed = True


async def test_post_yield_close_completes_under_outer_cancel() -> None:
    aconn = _bare_async_connection()

    gate = asyncio.Event()
    synthetic = _SyntheticBuilt(gate)

    async def _fake_build_and_connect(*args: Any, **kwargs: Any) -> Any:
        # Flip closed so _ensure_connection takes its post-build branch.
        aconn._closed = True
        return synthetic

    async def _runner() -> None:
        with (
            patch(
                "dqlitedbapi.aio.connection._build_and_connect",
                new=_fake_build_and_connect,
            ),
            contextlib.suppress(InterfaceError, asyncio.CancelledError),
        ):
            await aconn._ensure_connection()

    runner_task = asyncio.create_task(_runner())

    # Wait until the close is suspended on the gate, then cancel.
    while not (synthetic._gate is gate and not synthetic.close_completed):
        await asyncio.sleep(0.001)
        if runner_task.done():
            break

    await asyncio.sleep(0.01)
    runner_task.cancel()
    gate.set()
    with contextlib.suppress(asyncio.CancelledError, InterfaceError):
        await runner_task

    assert synthetic.close_completed, (
        "AsyncConnection._ensure_connection's post-build close must be "
        "shielded against outer cancellation; without the shield, the "
        "close body is interrupted and the freshly-built transport leaks. "
        "Mirrors the pool's asyncio.shield(conn.close()) discipline."
    )


async def test_outer_cancel_during_post_yield_close_propagates_as_cancellederror() -> None:
    """Outer cancel during the shielded close must propagate as CancelledError, not
    InterfaceError: the post-close suppress MUST NOT catch CancelledError."""
    aconn = _bare_async_connection()
    gate = asyncio.Event()
    synthetic = _SyntheticBuilt(gate)

    async def _fake_build_and_connect(*args: Any, **kwargs: Any) -> Any:
        aconn._closed = True
        return synthetic

    captured: list[BaseException] = []

    async def _runner() -> None:
        with patch(
            "dqlitedbapi.aio.connection._build_and_connect",
            new=_fake_build_and_connect,
        ):
            try:
                await aconn._ensure_connection()
            except BaseException as e:
                captured.append(e)
                raise

    runner_task = asyncio.create_task(_runner())
    while not (synthetic._gate is gate and not synthetic.close_completed):
        await asyncio.sleep(0.001)
        if runner_task.done():
            break
    await asyncio.sleep(0.01)
    runner_task.cancel()
    gate.set()
    with contextlib.suppress(asyncio.CancelledError):
        await runner_task

    assert len(captured) == 1, f"expected exactly one captured exception, got {captured}"
    assert isinstance(captured[0], asyncio.CancelledError), (
        f"outer cancel during shielded close must propagate as CancelledError, "
        f"not {type(captured[0]).__name__}: {captured[0]}. The post-close "
        f"suppress MUST NOT include asyncio.CancelledError."
    )
    assert synthetic.close_completed
