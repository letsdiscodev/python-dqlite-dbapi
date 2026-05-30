"""Server-BUSY retry mechanism — stdlib ``sqlite3`` parity.

dqlite's VFS authorizer denies ``PRAGMA busy_timeout``, so this module
reimplements SQLite's retry curve and BUSY classifier in-driver for the
sync and async surfaces to share.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any, Final

from dqlitedbapi.exceptions import OperationalError as _OperationalError
from dqlitewire import SQLITE_BUSY as _SQLITE_BUSY

logger = logging.getLogger(__name__)


def _resolve_busy_timeout_seconds(connection: object) -> float:
    """Read ``connection._busy_timeout``; non-numeric (e.g. MagicMock) -> 0.0 (no retry)."""
    value = getattr(connection, "_busy_timeout", 0.0)
    # ``bool`` is ``int`` to ``isinstance`` but must be rejected explicitly.
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return 0.0
    return float(value)


# SQLite's busy-callback curve (``main.c::sqliteDefaultBusyCallback``), canonical values.
# ``delays[i]`` = ms to sleep before retry ``i+1``; ``totals[i]`` = cumulative sleep before
# attempt ``i`` (``totals[i+1] == totals[i] + delays[i]``). The repeated 25s are upstream-intended.
_BUSY_DELAYS_MS: Final[tuple[int, ...]] = (1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100)
_BUSY_TOTALS_MS: Final[tuple[int, ...]] = (0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228)


def next_busy_delay_ms(count: int, busy_timeout_ms: int) -> int | None:
    """Sleep (ms) before retry ``count+1``, or ``None`` when the budget is exhausted.

    Mirrors SQLite's ``sqliteDefaultBusyCallback``; ``busy_timeout_ms <= 0`` means no wait.
    """
    if busy_timeout_ms <= 0:
        return None
    n_curve = len(_BUSY_DELAYS_MS)
    if count < n_curve:
        delay = _BUSY_DELAYS_MS[count]
        prior = _BUSY_TOTALS_MS[count]
    else:
        delay = _BUSY_DELAYS_MS[-1]
        prior = _BUSY_TOTALS_MS[-1] + delay * (count - (n_curve - 1))
    if prior + delay > busy_timeout_ms:
        delay = busy_timeout_ms - prior
        if delay <= 0:
            return None
    return delay


def is_busy_exception(exc: BaseException) -> bool:
    """True for ``OperationalError`` with ``code == SQLITE_BUSY`` (engine-BUSY and RAFT-BUSY).

    Gates on code, not message: ``DqliteConnectionError`` (checkpoint-BUSY) and other
    ``OperationalError`` codes (leader/transport faults) are deliberately excluded.
    """
    if not isinstance(exc, _OperationalError):
        return False
    return getattr(exc, "code", None) == _SQLITE_BUSY


def retry_sync_on_busy[T](
    busy_timeout: float,
    run_sync: Callable[[Coroutine[Any, Any, T]], T],
    coro_factory: Callable[[], Coroutine[Any, Any, T]],
) -> T:
    """Module-level sync retry loop.

    ``coro_factory`` must return a fresh coroutine per attempt (coroutines are single-use).
    Under ``check_same_thread=False`` a retried statement sees sibling writes between attempts
    (``_op_lock`` is released during the sleep); wrap in ``conn.transaction()`` for a snapshot.
    """
    # round (not int): ``N / 1000.0`` is not exactly representable, so the retry
    # budget matches the PRAGMA busy_timeout getter's echo instead of landing at N-1.
    busy_timeout_ms = round(busy_timeout * 1000)
    if busy_timeout_ms <= 0:
        return run_sync(coro_factory())
    count = 0
    while True:
        try:
            return run_sync(coro_factory())
        except _OperationalError as exc:
            if not is_busy_exception(exc):
                raise
            delay_ms = next_busy_delay_ms(count, busy_timeout_ms)
            if delay_ms is None:
                logger.warning(
                    "busy_timeout budget %.3fs exhausted after %d retries; raising original BUSY",
                    busy_timeout,
                    count,
                )
                raise
            logger.debug(
                "BUSY retry attempt %d: sleeping %dms (budget %.3fs)",
                count,
                delay_ms,
                busy_timeout,
            )
            time.sleep(delay_ms / 1000.0)
            count += 1


async def retry_async_on_busy[T](
    busy_timeout: float,
    coro_factory: Callable[[], Awaitable[T]],
) -> T:
    """Module-level async retry loop, for the executemany path (MagicMock cursors lack
    ``Connection._await_with_busy_retry``).

    Whether the sleep holds any lock depends on the factory: ``execute`` acquires ``op_lock``
    per attempt (released during sleep), but ``executemany`` holds it across the whole loop to
    preserve cancel-atomicity, accepting sibling-task starvation across retries.
    """
    # round (not int): ``N / 1000.0`` is not exactly representable, so the retry
    # budget matches the PRAGMA busy_timeout getter's echo instead of landing at N-1.
    busy_timeout_ms = round(busy_timeout * 1000)
    if busy_timeout_ms <= 0:
        return await coro_factory()
    count = 0
    while True:
        try:
            return await coro_factory()
        except _OperationalError as exc:
            if not is_busy_exception(exc):
                raise
            delay_ms = next_busy_delay_ms(count, busy_timeout_ms)
            if delay_ms is None:
                logger.warning(
                    "busy_timeout budget %.3fs exhausted after %d retries; raising original BUSY",
                    busy_timeout,
                    count,
                )
                raise
            logger.debug(
                "BUSY retry attempt %d: sleeping %dms (budget %.3fs)",
                count,
                delay_ms,
                busy_timeout,
            )
            await asyncio.sleep(delay_ms / 1000.0)
            count += 1
