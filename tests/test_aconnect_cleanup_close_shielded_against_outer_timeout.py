"""Pin: ``dqlitedbapi.aio.aconnect`` cleanup-close must shield against
a fresh outer ``CancelledError`` arriving during ``conn.close()`` so
the original connect-time failure is the active exception surfaced to
the caller, not a CancelledError from the cleanup site.

Pre-fix the cleanup arm was:

    try:
        await conn.close()
    except Exception:
        logger.debug(...)
    raise

``except Exception`` does NOT catch ``CancelledError`` (3.8+), so a
fresh outer cancel landing during ``await conn.close()`` raises a new
``CancelledError`` that fully supplants the original connect-time
exception — the bare ``raise`` is never reached and the original
error is demoted to ``__context__``.

Mirrors the sibling ``dqliteclient.connect`` fix (round-3 commit
``1ba9371``): wrap the close in
``contextlib.suppress(asyncio.CancelledError) + asyncio.shield(...)``
so the close runs to completion in the background and the original
exception's bare ``raise`` is reached.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from dqlitedbapi.aio import aconnect
from dqlitedbapi.aio.connection import AsyncConnection


async def test_cancel_during_cleanup_close_does_not_supplant_original() -> None:
    """When a fresh CancelledError lands during the cleanup-close, the
    original connect-time exception (here OSError) must remain the
    active exception surfaced to the caller; the close still runs."""

    close_started = asyncio.Event()
    close_finished = asyncio.Event()

    async def _slow_close(self: AsyncConnection) -> None:
        close_started.set()
        # Long sleep — the test arranges for an outer cancel to land
        # while we are suspended here. The shield must keep us alive
        # to completion regardless.
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            # Without the shield, the cleanup-close would be cancelled
            # mid-flight here. With the shield the inner task keeps
            # running until the sleep is replaced by a direct set().
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
    # Wait until the cleanup-close is running, then cancel.
    await close_started.wait()
    task.cancel()

    with pytest.raises(BaseException) as exc_info:
        await task

    # The original OSError must be the active exception. On the buggy
    # code path the CancelledError supplants it and the OSError is
    # demoted to ``__context__``.
    raised = exc_info.value
    if isinstance(raised, OSError):
        assert "connect-failed" in str(raised)
    else:
        pytest.fail(
            f"expected OSError('connect-failed') to be the active exception "
            f"after cancel-during-cleanup, got {type(raised).__name__}: {raised!r} "
            f"(context={raised.__context__!r})"
        )
