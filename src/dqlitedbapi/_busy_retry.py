"""Server-BUSY retry mechanism — stdlib ``sqlite3`` parity.

Stdlib ``sqlite3.connect(timeout=N)`` defaults to a 5-second
``busy_timeout`` at the SQLite C library level. When a write hits
contention the C library transparently sleeps and retries via a
deterministic curve (``main.c::sqliteDefaultBusyCallback``). dqlite's
server-side VFS authorizer denies ``PRAGMA busy_timeout`` (see
``done/dbapi-pragma-deny-list-no-regression-pin.md``), so callers
cannot tune the retry behaviour through the canonical SQLite escape
hatch. Without an in-driver retry, application code that worked
against stdlib ``sqlite3`` silently regresses on dqlite — two
concurrent writers contending for the dqlite write lock produce
``OperationalError: database is locked to <addr>`` surfacing as HTTP
500s.

This module hosts the SQLite-equivalent retry curve and the
``OperationalError`` ↔ BUSY classifier so the sync and async surfaces
can share both. The actual retry-loop helpers live on the
``Connection`` / ``AsyncConnection`` classes (they need access to the
per-connection ``busy_timeout`` and the run-sync / run-async
bridges).

Design notes:

- The curve is SQLite's own (``delays`` / ``totals`` arrays from
  ``main.c``). Battle-tested. No jitter — BUSY is local contention,
  not distributed-systems flapping; jitter would only smear the
  retry burst across siblings without changing the convergence shape.

- The classifier gates on ``code == SQLITE_BUSY`` not on a substring
  scan. The user-supplied brief referenced
  ``dqliteclient/connection.py:_RAFT_BUSY_MESSAGE_FRAGMENTS`` but
  that tuple is narrow (``("checkpoint in progress",)``) and is the
  classifier for the SEPARATE "checkpoint-BUSY rewrap as
  ``DqliteConnectionError``" path — NOT the RAFT-BUSY-"is locked"
  family this retry targets. Code-based gating catches both engine-
  BUSY and RAFT-BUSY shapes per the existing comment at
  ``dqliteclient/connection.py:2683-2698``.
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
    """Read ``connection._busy_timeout`` with a MagicMock-safe
    fallback. Test fixtures construct ``Cursor`` instances with
    ``MagicMock`` connections whose attributes auto-spawn — without
    this guard the retry curve's ``int(busy_timeout * 1000)`` would
    crash on a non-numeric MagicMock. ``isinstance`` check on the
    return value falls through to ``0.0`` (no retry — preserves the
    pre-feature test semantics)."""
    value = getattr(connection, "_busy_timeout", 0.0)
    # ``bool`` is ``int`` to ``isinstance`` but must be rejected
    # explicitly; production code rejects bool at validation. A
    # MagicMock attribute is not a real number — fall through.
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        return 0.0
    return float(value)


# SQLite's busy-callback curve (``main.c::sqliteDefaultBusyCallback``).
# ``delays[i]`` is the milliseconds to sleep before retry attempt
# ``i+1``; ``totals[i]`` is the cumulative sleep BEFORE attempt
# ``i`` (so ``totals[i+1] == totals[i] + delays[i]``). After
# ``len(delays)`` attempts the curve flattens at the last
# ``delays`` value (100 ms) — see ``next_busy_delay_ms`` below.
#
# Pinned to the canonical SQLite values (verified internally
# consistent via the totals[i+1] = totals[i] + delays[i] check
# in ``tests/test_busy_retry_curve.py``). The constant 25 appears
# three times in a row at positions 6, 7, 8 — this is intentional
# in the upstream curve, not an editing mistake.
_BUSY_DELAYS_MS: Final[tuple[int, ...]] = (1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100)
_BUSY_TOTALS_MS: Final[tuple[int, ...]] = (0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228)


def next_busy_delay_ms(count: int, busy_timeout_ms: int) -> int | None:
    """Return the sleep duration (ms) before retry attempt ``count+1``,
    or ``None`` when the cumulative budget is exhausted.

    Mirrors SQLite's ``sqliteDefaultBusyCallback`` exactly:

    - For ``count < len(_BUSY_DELAYS_MS)``, return the next entry from
      the curve.
    - For ``count >= len(_BUSY_DELAYS_MS)``, return the flat tail
      value (100 ms).
    - If the resulting ``prior + delay`` would overrun
      ``busy_timeout_ms``, clamp ``delay`` to fit; if the clamp would
      produce ``delay <= 0``, return ``None`` (caller raises the
      original BUSY).

    ``busy_timeout_ms <= 0`` short-circuits ``None`` regardless of
    ``count`` — stdlib parity for ``timeout=0`` meaning "no wait."
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
    """Classify an exception as a retryable BUSY.

    The retry mechanism gates on ``OperationalError`` whose ``code`` is
    ``SQLITE_BUSY (5)``. This covers both engine-BUSY (the SQLite-side
    BUSY where the user can retry the failing statement and continue
    the same tx) AND RAFT-BUSY translated by the server's gateway
    (the in-flight write was not accepted; the server-side tx state
    is the same as engine-BUSY from the client's perspective).

    The two shapes are indistinguishable at the Python layer per the
    existing comment at ``dqliteclient/connection.py:2683-2698``
    ("users must retry explicitly"). This retry IS that explicit
    retry, applied uniformly across both BUSY origins.

    Specifically NOT retried:

    - ``DqliteConnectionError`` (the checkpoint-BUSY rewrap at
      ``dqliteclient/connection.py:2717``) — bypasses this classifier
      because it's not an ``OperationalError`` subclass. The
      checkpoint-BUSY case already triggers SA pool invalidation by
      design (the connection's tx state was reset; retry on the same
      connection is wrong).
    - Other ``OperationalError`` codes (LEADER_ERROR_CODES,
      INTERFACE_ERROR_CODES, etc.) — leader-flip and transport
      faults have their own retry semantics elsewhere; this gate
      narrows to BUSY only.
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

    Caller provides ``run_sync`` (the function that drives the
    coroutine on the connection's daemon loop — typically
    ``connection._run_sync``) and ``coro_factory`` (a callable that
    returns a fresh coroutine each time the retry needs to re-submit).
    Coroutines are single-use so the factory shape is mandatory.

    Same retry shape as the inline-on-connection variants: SQLite-curve
    sleeps, BUSY-only classification (``OperationalError(code=
    SQLITE_BUSY)``), propagate non-BUSY exceptions unchanged.
    ``KeyboardInterrupt`` / ``SystemExit`` during ``time.sleep``
    propagate (those are ``BaseException``, not caught by ``except
    OperationalError``).

    ``busy_timeout <= 0`` short-circuits to a single ``run_sync``
    call with no retry (stdlib parity for ``timeout=0``).

    **Cross-thread caveat under** ``check_same_thread=False``:

    The retry loop releases the connection's ``_op_lock`` between
    attempts. ``run_sync`` acquires the lock at the START of each
    attempt and releases it before returning; the ``time.sleep``
    that follows runs OUTSIDE the lock so the sleep doesn't starve
    sibling threads. By design — holding ``_op_lock`` across a
    100ms sleep would defeat cross-thread sharing.

    On a Connection shared across threads via
    ``check_same_thread=False``, a BUSY-retried statement may
    observe interleaved writes from sibling threads between retry
    attempts. Example:

        Thread A: cur.execute("UPDATE t SET v=v+1 WHERE k=?") -> BUSY
        Thread A: sleeps for SQLite-curve delay (op_lock released)
        Thread B: cur.execute("UPDATE t SET v=v+1 WHERE k=?") -> succeeds
        Thread A: retries -> sees B's update; may succeed against
                  a different row state than the original attempt.

    This is the correct semantic for retry-on-contention (each
    retry sees current state, the same as it would on a fresh
    connection). Callers who need to retry against an atomic
    snapshot must wrap in an explicit transaction:

        with conn.transaction():
            cur.execute("UPDATE t SET v=v+1 WHERE k=?", (k,))

    The transaction holds the wire lock across the entire body, so
    sibling threads cannot interleave statements between the retry
    attempts inside that body.
    """
    busy_timeout_ms = int(busy_timeout * 1000)
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
    """Module-level async retry loop. Used by the per-iteration
    executemany path that cannot route through
    ``Connection._await_with_busy_retry`` because test fixtures
    construct ``Cursor`` instances with a ``MagicMock`` connection
    that does not have that method.

    Same retry shape as the connection-method variants: SQLite-curve
    sleeps, BUSY-only classification, propagate non-BUSY unchanged.
    ``CancelledError`` propagates through ``await asyncio.sleep``
    because the ``except OperationalError`` arm does not catch
    ``BaseException``.

    ``busy_timeout <= 0`` short-circuits to a single awaitable call
    with no retry (stdlib parity).

    **Cross-task caveat**:

    The helper itself does NOT acquire or release any wire-level
    lock — it simply awaits ``coro_factory()``, classifies BUSY,
    sleeps, and retries. Whether the ``await asyncio.sleep`` runs
    inside or outside any caller-held lock is determined by the
    factory's shape.

    The async ``execute`` call site supplies a factory that
    acquires ``op_lock`` per attempt (``async with op_lock: ...
    await self._execute_unlocked(...)``), so the sleep between
    attempts runs OUTSIDE the lock and sibling tasks on the same
    ``AsyncConnection`` (``await conn.commit()`` / ``rollback()``
    / ``close()``) can acquire ``op_lock`` in the gap. Mirrors the
    sync sibling's ``run_sync``-per-attempt discipline.

    The ``executemany`` call site is the documented exception: it
    holds an outer ``async with op_lock:`` across the WHOLE
    iteration loop (preserving the cancel-atomicity invariant —
    a concurrent task cannot slip arbitrary statements between
    iterations). The per-iteration BUSY backoff therefore DOES
    keep the lock held; sibling-task starvation across executemany
    BUSY retries is the accepted trade-off for batch atomicity.

    On an AsyncConnection shared across asyncio tasks on the same
    loop, a BUSY-retried statement may observe interleaved writes
    from sibling tasks between retry attempts. Same shape as the
    sync sibling: each retry sees current state. Callers who need
    to retry against an atomic snapshot must wrap in an explicit
    ``async with conn.transaction():`` block.
    """
    busy_timeout_ms = int(busy_timeout * 1000)
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
