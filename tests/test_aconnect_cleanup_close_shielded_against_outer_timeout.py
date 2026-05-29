"""aconnect's cleanup-close shields against a fresh outer CancelledError during conn.close().

`except Exception` does not catch CancelledError, so an unshielded cancel during the close
would supplant the original connect failure and demote it to __context__. The close is wrapped
in suppress(CancelledError) + shield so it completes and the original exception is re-raised.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.aio import aconnect
from dqlitedbapi.aio.connection import AsyncConnection


async def test_cancel_during_cleanup_close_does_not_supplant_original() -> None:
    """A cancel during cleanup-close keeps the original OSError active; the close still runs."""

    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    async def _slow_close(self: AsyncConnection) -> None:
        close_started.set()
        # The test arranges for an outer cancel to land while suspended here.
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            close_finished.set()
            raise
        close_finished.set()

    async def _drive() -> None:
        with (
            patch.object(
                AsyncConnection,
                "connect",
                new=AsyncMock(side_effect=OSError("connect-failed")),
            ),
            patch.object(AsyncConnection, "close", new=_slow_close),
        ):
            await aconnect("localhost:9001", database="test", timeout=5.0)

    task = asyncio.create_task(_drive())
    await close_started.wait()
    task.cancel()

    with pytest.raises(BaseException) as exc_info:
        await task

    # On the buggy path the CancelledError supplants the OSError and demotes it to __context__.
    raised = exc_info.value
    if isinstance(raised, OSError):
        assert "connect-failed" in str(raised)
    else:
        pytest.fail(
            f"expected OSError('connect-failed') to be the active exception "
            f"after cancel-during-cleanup, got {type(raised).__name__}: {raised!r} "
            f"(context={raised.__context__!r})"
        )
