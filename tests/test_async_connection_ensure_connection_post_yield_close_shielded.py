"""Pin: ``AsyncConnection._ensure_connection`` shields the post-build
``built.close()`` against outer cancellation, so a cancel cascade
during ``engine.dispose()`` cannot leak the freshly-built transport.

The unshielded shape was:

    if self._closed:
        with contextlib.suppress(Exception):
            await built.close()       # not shielded
        raise InterfaceError(...)

A ``CancelledError`` delivered to ``_ensure_connection`` while
``built.close()`` is suspended on its ``wait_closed()`` checkpoint
propagates out, leaving ``built``'s socket and reader Task
unreferenced. The fix wraps the close in ``asyncio.shield(...)``
so the inner coroutine completes even if the outer awaiter is
cancelled — mirrors the pool's already-applied discipline.

The test installs a synthetic ``built`` whose ``close()`` awaits an
``asyncio.Event`` the test sets only AFTER firing the outer cancel.
That guarantees the cancel necessarily lands during the suspended
close. With the unshielded shape, ``built.close`` is interrupted
and ``_close_completed`` is False. With the shield, the close runs
to completion regardless of the cancel.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import threading
from typing import Any, cast
from unittest.mock import patch

import pytest

import dqlitedbapi.aio
from dqlitedbapi.exceptions import InterfaceError


def _bare_async_connection() -> Any:
    """Build an ``AsyncConnection`` without invoking ``__init__`` so
    no real address / loop binding is needed. Mirrors the in-package
    ``_bare_async_conn`` pattern used by other tests."""
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
    """Stand-in for the ``DqliteConnection`` that ``_build_and_connect``
    returns. ``close()`` waits on an Event the test controls."""

    def __init__(self, gate: asyncio.Event) -> None:
        self._gate = gate
        self.close_completed = False

    async def close(self) -> None:
        # Suspend until the test gates us through.
        await self._gate.wait()
        self.close_completed = True


@pytest.mark.asyncio
async def test_post_yield_close_completes_under_outer_cancel() -> None:
    aconn = _bare_async_connection()

    gate = asyncio.Event()
    synthetic = _SyntheticBuilt(gate)

    async def _fake_build_and_connect(*args: Any, **kwargs: Any) -> Any:
        # Flip the closed flag concurrently so _ensure_connection's
        # post-build branch fires.
        aconn._closed = True
        return synthetic

    # Fire the outer cancel partway through `built.close()` (i.e.,
    # while it's awaiting the gate). Then set the gate so the close
    # body runs to completion under the shield.
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
        # Bail out if the runner finishes first (would mean the close
        # path didn't even reach the await — wrong test setup).
        if runner_task.done():
            break

    # Give the close one event-loop tick to actually be suspended on
    # the gate.
    await asyncio.sleep(0.01)
    runner_task.cancel()
    # Now release the gate. Under the shield, the close body completes
    # despite the cancel. Without the shield, the close await is
    # interrupted and the body never sets close_completed.
    gate.set()
    with contextlib.suppress(asyncio.CancelledError, InterfaceError):
        await runner_task

    assert synthetic.close_completed, (
        "AsyncConnection._ensure_connection's post-build close must be "
        "shielded against outer cancellation; without the shield, the "
        "close body is interrupted and the freshly-built transport leaks. "
        "Mirrors the pool's asyncio.shield(conn.close()) discipline."
    )


@pytest.mark.asyncio
async def test_outer_cancel_during_post_yield_close_propagates_as_cancellederror() -> None:
    """The outer cancel that lands during the shielded close must
    propagate as ``CancelledError`` to the caller — not be swallowed
    and reborn as ``InterfaceError``.

    With ``asyncio.shield``, the inner close runs in the background
    while the outer awaiter re-raises ``CancelledError``. The post-
    close branch's ``with contextlib.suppress(...)`` MUST NOT include
    ``CancelledError`` in its catch set; otherwise the cancel is
    silently swallowed and the next line ``raise InterfaceError(...)``
    runs, breaking the cancellation contract for callers using
    ``task.cancel()`` / ``asyncio.timeout(...)`` / TaskGroup siblings.
    """
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

    # The runner's except arm must have captured CancelledError, NOT
    # InterfaceError. Without this assertion the test passes vacuously
    # because the close completes in the background regardless.
    assert len(captured) == 1, f"expected exactly one captured exception, got {captured}"
    assert isinstance(captured[0], asyncio.CancelledError), (
        f"outer cancel during shielded close must propagate as CancelledError, "
        f"not {type(captured[0]).__name__}: {captured[0]}. The post-close "
        f"suppress MUST NOT include asyncio.CancelledError."
    )
    # And the close still completed in the background (shield contract).
    assert synthetic.close_completed
