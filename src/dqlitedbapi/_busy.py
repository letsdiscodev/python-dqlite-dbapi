"""Retry on ``SQLITE_BUSY`` following SQLite's default busy-handler curve.

dqlite rejects ``PRAGMA busy_timeout`` on the wire, so the driver owns the budget.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Final

from dqlitedbapi.exceptions import OperationalError
from dqlitewire import SQLITE_BUSY

logger = logging.getLogger(__name__)

# sqlite3.c ``sqliteDefaultBusyCallback``: sleep before retry i, and cumulative sleep.
_DELAYS_MS: Final[tuple[int, ...]] = (1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100)
_TOTALS_MS: Final[tuple[int, ...]] = (0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228)


def next_delay_ms(attempt: int, budget_ms: int) -> int | None:
    """Milliseconds to sleep before retry ``attempt + 1``, or ``None`` when the budget is spent."""
    if budget_ms <= 0:
        return None
    if attempt < len(_DELAYS_MS):
        delay, prior = _DELAYS_MS[attempt], _TOTALS_MS[attempt]
    else:
        delay = _DELAYS_MS[-1]
        prior = _TOTALS_MS[-1] + delay * (attempt - (len(_DELAYS_MS) - 1))
    if prior + delay > budget_ms:
        delay = budget_ms - prior
    return delay if delay > 0 else None


def is_busy(exc: BaseException) -> bool:
    return isinstance(exc, OperationalError) and exc.code == SQLITE_BUSY


async def retry_on_busy[T](busy_timeout: float, attempt: Callable[[], Awaitable[T]]) -> T:
    """Run ``attempt()`` until it succeeds, fails with a non-BUSY error, or the budget is spent."""
    budget_ms = round(busy_timeout * 1000)
    count = 0
    while True:
        try:
            return await attempt()
        except OperationalError as exc:
            if not is_busy(exc):
                raise
            delay = next_delay_ms(count, budget_ms)
            if delay is None:
                if budget_ms > 0:
                    logger.warning(
                        "busy_timeout budget %.3fs exhausted after %d retries", busy_timeout, count
                    )
                raise
            await asyncio.sleep(delay / 1000)
            count += 1
