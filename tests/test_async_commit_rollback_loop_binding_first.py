"""Pin: ``AsyncConnection.commit`` / ``rollback`` invoke the
loop-binding check BEFORE clearing ``self.messages``.

A stray cross-loop ``await aconn.commit()`` previously:
1. Cleared the OTHER loop's ``messages`` list (visible mutation
   from a foreign loop).
2. Compared ``_transaction_owner`` (a Task object owned by loop A)
   with ``current_task()`` from loop B — different Task identities,
   so the in-transaction-body guard evaluated False spuriously.
3. Only at ``_ensure_locks()`` did the loop-binding diagnostic
   finally fire.

The sync sibling ``Connection.commit()`` calls ``_check_thread()``
up front; this pin restores the same discipline on the async side.
"""

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
    """Cross-loop ``commit()`` must raise ``ProgrammingError`` AND
    leave ``self.messages`` untouched — proving the loop-binding
    check runs before the PEP 249 §6.1.1 messages-clear.
    """
    conn = AsyncConnection("127.0.0.1:9001")
    conn._ensure_locks()  # bind to this loop

    sentinel: tuple[type[Exception], Exception] = (RuntimeError, RuntimeError("sentinel"))
    conn.messages.append(sentinel)

    err = _invoke_on_other_loop(conn, lambda c: c.commit())

    assert isinstance(err, ProgrammingError), (
        f"cross-loop commit() must raise ProgrammingError; got {err!r}"
    )
    # The sentinel survives — proves clear was NOT executed by the
    # foreign loop. Pre-fix, ``del self.messages[:]`` ran before the
    # check fired, so this assertion would have failed.
    assert sentinel in conn.messages, (
        f"loop-binding check must fire BEFORE the messages-clear; "
        f"sentinel was scrubbed by the cross-loop caller. messages={conn.messages!r}"
    )


async def test_rollback_rejects_cross_loop_before_clearing_messages() -> None:
    """Symmetric pin for ``rollback()``."""
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
