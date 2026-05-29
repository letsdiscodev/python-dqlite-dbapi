"""commit/rollback run the loop-binding check BEFORE clearing self.messages, so a
cross-loop call cannot mutate a foreign loop's state before being rejected
(mirrors the sync sibling's up-front _check_thread())."""

from __future__ import annotations

import asyncio
import threading

from dqlitedbapi import ProgrammingError
from dqlitedbapi.aio.connection import AsyncConnection


def _invoke_on_other_loop(conn: AsyncConnection, coro_factory) -> BaseException | None:  # noqa: ANN001
    captured: list[BaseException] = []

    def _runner() -> None:
        async def _invoke() -> None:
            try:
                await coro_factory(conn)
            except BaseException as e:
                captured.append(e)

        asyncio.run(_invoke())

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    return captured[0] if captured else None


async def test_commit_rejects_cross_loop_before_clearing_messages() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()  # bind to this loop

    sentinel: tuple[type[Exception], Exception] = (RuntimeError, RuntimeError("sentinel"))
    conn.messages.append(sentinel)

    err = _invoke_on_other_loop(conn, lambda c: c.commit())

    assert isinstance(err, ProgrammingError), (
        f"cross-loop commit() must raise ProgrammingError; got {err!r}"
    )
    # Sentinel surviving proves the clear did not run before the loop check.
    assert sentinel in conn.messages, (
        f"loop-binding check must fire BEFORE the messages-clear; "
        f"sentinel was scrubbed by the cross-loop caller. messages={conn.messages!r}"
    )


async def test_rollback_rejects_cross_loop_before_clearing_messages() -> None:
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()

    sentinel: tuple[type[Exception], Exception] = (
        RuntimeError,
        RuntimeError("sentinel-rollback"),
    )
    conn.messages.append(sentinel)

    err = _invoke_on_other_loop(conn, lambda c: c.rollback())

    assert isinstance(err, ProgrammingError), (
        f"cross-loop rollback() must raise ProgrammingError; got {err!r}"
    )
    assert sentinel in conn.messages, (
        f"loop-binding check must fire BEFORE the messages-clear; "
        f"sentinel was scrubbed by the cross-loop caller. messages={conn.messages!r}"
    )
