"""PEP 249 Connection implementation for dqlite."""

import asyncio
import concurrent.futures
import contextlib
import logging
import math
import threading
import warnings
import weakref
from collections.abc import Coroutine
from types import TracebackType
from typing import Any

import dqliteclient.exceptions as _client_exc
from dqliteclient import DqliteConnection
from dqliteclient.connection import _parse_address as _client_parse_address
from dqliteclient.protocol import _validate_positive_int_or_none
from dqlitedbapi import exceptions as _exc
from dqlitedbapi.cursor import Cursor, _call_client
from dqlitedbapi.exceptions import InterfaceError, OperationalError, ProgrammingError
from dqlitewire import (
    DEFAULT_MAX_CONTINUATION_FRAMES as _DEFAULT_MAX_CONTINUATION_FRAMES,
)
from dqlitewire import (
    DEFAULT_MAX_TOTAL_ROWS as _DEFAULT_MAX_TOTAL_ROWS,
)
from dqlitewire.constants import primary_sqlite_code

__all__ = ["Connection"]

logger = logging.getLogger(__name__)

# SQLite result code for "you tried to COMMIT/ROLLBACK but there's no
# transaction active." The dqlite C server's gateway path
# (``dqlite-upstream/src/gateway.c``) propagates the SQLite engine's
# ``sqlite3_errcode``, which for stray COMMIT/ROLLBACK is
# ``SQLITE_ERROR`` (1) only — ``SQLITE_MISUSE`` (21) is used for an
# unrelated VFS file-control path (``vfs.c::vfsFileControlPersistWal``)
# but never for transaction-state misuse on the wire. Pinned by the
# integration test ``test_no_transaction_error_wording.py``. We
# deliberately do NOT include 21 in the whitelist so a real misuse
# error always surfaces. Check the numeric code first so a malicious
# or impostor server cannot silence unrelated errors just by crafting
# a message string that contains the magic substring. The substring
# remains as a secondary filter because SQLite has many uses of code=1.
_NO_TX_CODES = frozenset({1})
# Substrings that mark a benign "no transaction was active" reply.
# Mirror the client-layer ``_is_no_tx_rollback_error`` recogniser so a
# wording drift in the server (or in the embedded SQLite version) that
# drops one of these clauses does not produce a silent layer
# divergence — the client suppressing while the dbapi raises.
_NO_TX_SUBSTRINGS = ("no transaction is active", "cannot rollback")

# Bound (in seconds) for joining the background event-loop thread on
# teardown. Worst-case scenario: a coroutine queued on the loop is
# mid-await on a wire call when shutdown is requested. The loop is
# asked to stop; the in-flight task resolves to CancelledError; the
# thread then joins. 5 seconds covers the slow-network worst case
# while preventing process hang on close. Both the finalizer
# (``_cleanup_loop_thread``) and ``Connection.close()`` use this
# bound — keep them in step via the constant.
_LOOP_THREAD_JOIN_TIMEOUT_SECONDS = 5.0


def _validate_timeout(timeout: float) -> None:
    """Raise ProgrammingError if ``timeout`` is not a positive finite number.

    Reused by ``dqlitedbapi.connect``, ``dqlitedbapi.aio.connect`` (the
    sync-returning pun), and ``dqlitedbapi.aio.aconnect``. The exact
    error phrasing is verbatim because several existing tests match on
    the prefix ``"timeout must be a positive finite number"``.

    ``bool`` is rejected up front: ``isinstance(True, float)`` is False
    but ``isinstance(True, int)`` is True and ``math.isfinite(True)``
    returns True, so a caller passing ``timeout=True`` would silently
    get a 1-second budget. Match the sibling validator
    ``_validate_positive_int_or_none`` in the client layer.
    """
    if isinstance(timeout, bool):
        raise ProgrammingError(f"timeout must be a positive finite number, got {timeout!r} (bool)")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ProgrammingError(f"timeout must be a positive finite number, got {timeout}")


def _validate_close_timeout(close_timeout: float) -> None:
    """Raise ProgrammingError if ``close_timeout`` is not a positive finite number.

    The client layer raises ``ValueError`` for the same predicate; the
    dbapi layer wraps with ``ProgrammingError`` so PEP 249 error
    classification holds at the dbapi boundary (parallel to
    ``_validate_timeout``). Rejects ``bool`` for the same reason as the
    sibling ``_validate_timeout``.
    """
    if isinstance(close_timeout, bool):
        raise ProgrammingError(
            f"close_timeout must be a positive finite number, got {close_timeout!r} (bool)"
        )
    if not math.isfinite(close_timeout) or close_timeout <= 0:
        raise ProgrammingError(
            f"close_timeout must be a positive finite number, got {close_timeout}"
        )


async def _build_and_connect(
    address: str,
    *,
    database: str,
    timeout: float,
    max_total_rows: int | None,
    max_continuation_frames: int | None,
    trust_server_heartbeat: bool,
    close_timeout: float,
) -> DqliteConnection:
    """Build a DqliteConnection with the given governors and connect it.

    Wraps the construct-then-connect sequence that both the sync and
    async Connection flavours execute under their respective locks. The
    ``OperationalError`` message phrasing ("Failed to connect: ...") is
    intentionally verbatim so test assertions that match on the prefix
    continue to pass.
    """
    conn = DqliteConnection(
        address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
    )
    try:
        await conn.connect()
    except _client_exc.OperationalError as e:
        # Preserve the server-supplied code so sqlalchemy-dqlite's
        # is_disconnect classifier can recognise leader-change codes
        # (SQLITE_IOERR_NOT_LEADER / _LEADERSHIP_LOST) on the connect
        # path via the code-based branch, matching the query path.
        # Plumb raw_message so callers that want the un-truncated
        # server text don't have to walk __cause__.
        raise OperationalError(
            f"Failed to connect: {e.message}",
            code=e.code,
            raw_message=f"Failed to connect: {e.raw_message}",
        ) from e
    except _client_exc.ClusterPolicyError as e:
        # Deterministic configuration mismatch. Route through
        # ``InterfaceError`` with a distinguishing ``"Cluster policy
        # rejection;"`` prefix so callers can branch on the message
        # without importing client-layer types. SA's ``is_disconnect``
        # narrows ``InterfaceError`` matching to "connection is
        # closed" / "cursor is closed", so the pool does NOT enter a
        # retry loop against the permanent policy rejection — matches
        # the ``_call_client`` query-path wrap.
        raise InterfaceError(f"Cluster policy rejection; {e}") from e
    except Exception as e:
        raise OperationalError(f"Failed to connect: {e}") from e
    return conn


def _is_no_transaction_error(exc: Exception) -> bool:
    """True if ``exc`` is a genuine "no active transaction" server reply.

    Gates the silent swallow on the SQLite result code in addition to
    the English wording. A disk-full / constraint / IO error whose
    message happens to include the magic substring will not be
    swallowed.

    A ``code`` of ``None`` (the dbapi wraps DqliteConnectionError /
    ClusterError / ProtocolError / DataError with ``code=None``) must
    NOT match: those classes are precisely the errors we want to
    surface, never silently swallow. The integration test
    ``test_no_transaction_error_wording.py`` proves the server emits
    code=1 for the genuine reply, so the whitelist is exhaustive on
    its own — the message-text fallback is only valid alongside a
    real SQLite code.
    """
    code = getattr(exc, "code", None)
    if code is None:
        return False
    # Mask to the SQLite primary result code (low byte of the extended
    # code); mirrors ``_classify_operational`` in cursor.py. Without the
    # mask, any extended variant of SQLITE_ERROR / SQLITE_MISUSE whose
    # low byte is 1 or 21 would slip past the whitelist and be surfaced.
    if primary_sqlite_code(code) not in _NO_TX_CODES:
        return False
    lowered = str(exc).lower()
    return any(s in lowered for s in _NO_TX_SUBSTRINGS)


def _cleanup_loop_thread(
    loop: asyncio.AbstractEventLoop,
    thread: threading.Thread,
    closed_flag: list[bool],
    address: str,
) -> None:
    """Stop the background event loop and join its thread.

    Called from a ``weakref.finalize`` so it must not reference the
    ``Connection`` instance. ``closed_flag`` is a 1-element list that
    the Connection mutates when ``close()`` is called — we use that
    rather than a direct reference to self to decide whether to emit
    a ``ResourceWarning``.
    """
    # Wrap the entire body in try/finally so the loop/thread teardown
    # ALWAYS runs, regardless of whether the warning emission raises.
    # Under ``pytest -W error::ResourceWarning`` the
    # ``warnings.warn(..., ResourceWarning, ...)`` call below converts
    # to a raised ``ResourceWarning`` (subclass of ``Warning`` /
    # ``Exception``, NOT ``RuntimeError``). Without the finally, the
    # raise propagated out of the finalizer past the narrow
    # ``contextlib.suppress(RuntimeError)``, the cleanup steps below
    # never ran, and the daemon event-loop thread lingered with an
    # open socket — ironically *amplifying* the leak the warning was
    # supposed to surface.
    try:
        if closed_flag[0] is False:
            # User never called close() → leak warning (matches stdlib
            # sqlite3). The narrow ``RuntimeError`` suppression here is
            # for the specific interpreter-shutdown race where the
            # warnings module's own finalization is mid-teardown; any
            # other exception (including ResourceWarning being
            # converted to a raise under -W error) is allowed to
            # propagate through the surrounding finally so the
            # finalizer's reporter (sys.unraisablehook) still surfaces
            # it while the cleanup completes.
            with contextlib.suppress(RuntimeError):
                warnings.warn(
                    f"Connection(address={address!r}) was garbage-collected "
                    f"without close(); cleaning up event-loop thread. Call "
                    f"Connection.close() explicitly to avoid this warning.",
                    ResourceWarning,
                    stacklevel=2,
                )
    finally:
        # Narrow suppression to the specific exceptions loop/thread
        # teardown can legitimately raise during finalization. Wider
        # ``except Exception: pass`` would hide programmer bugs like a
        # missing attribute reference introduced during a refactor.
        try:
            if not loop.is_closed():
                loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:  # pragma: no cover - race: loop closed mid-call
            # Loop was closed between is_closed() and the threadsafe
            # call.
            pass
        with contextlib.suppress(RuntimeError):
            thread.join(timeout=_LOOP_THREAD_JOIN_TIMEOUT_SECONDS)
        try:
            if not loop.is_closed():
                loop.close()
        except RuntimeError:
            # Raised if the loop was somehow restarted mid-finalization.
            pass


class Connection:
    """PEP 249 compliant database connection.

    Transactions: each statement auto-commits at the server unless
    wrapped in an explicit ``BEGIN`` — this differs from PEP 249 §6's
    implicit-transaction model and from stdlib ``sqlite3``. See the
    README's "Transactions" section.

    The autocommit-by-default model also applies to ``executemany``:
    without a surrounding ``BEGIN`` / ``COMMIT``, a mid-batch cancel
    leaves the iterations that already completed persisted. See
    ``Cursor.executemany`` / ``AsyncCursor.executemany`` for the
    cancellation-atomicity contract.
    """

    # PEP 249 optional extension ("Attributes from Module Exceptions"):
    # expose the module-level exception classes as class attributes so
    # cross-driver generic code can write ``except conn.Error:`` without
    # importing the driver module. Stdlib ``sqlite3.Connection`` and
    # every mainstream driver (psycopg2, asyncpg, aiosqlite) do the same.
    # Class attrs (not instance attrs) to keep ``type(conn).Error``
    # identity.
    Error = _exc.Error
    Warning = _exc.Warning
    InterfaceError = _exc.InterfaceError
    DatabaseError = _exc.DatabaseError
    DataError = _exc.DataError
    OperationalError = _exc.OperationalError
    IntegrityError = _exc.IntegrityError
    InternalError = _exc.InternalError
    ProgrammingError = _exc.ProgrammingError
    NotSupportedError = _exc.NotSupportedError

    def __init__(
        self,
        address: str,
        *,
        database: str = "default",
        timeout: float = 10.0,
        max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
        max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
        trust_server_heartbeat: bool = False,
        close_timeout: float = 0.5,
    ) -> None:
        """Initialize connection (does not connect yet).

        Args:
            address: Node address in "host:port" format
            database: Database name to open
            timeout: Connection timeout in seconds (must be positive
                and finite; validated here so direct ``Connection(...)``
                calls don't silently accept bad values that later
                produce hangs or stranger downstream errors)
            max_total_rows: Cumulative row cap across continuation
                frames for a single query. Forwarded to the underlying
                :class:`DqliteConnection`. ``None`` disables the cap.
            max_continuation_frames: Per-query continuation-frame cap.
                Bounds Python-side decode work a hostile server can
                inflict by drip-feeding 1-row frames. Forwarded to the
                underlying :class:`DqliteConnection`.
            trust_server_heartbeat: When True, widen the per-read
                deadline to the server-advertised heartbeat (subject to
                a 300 s hard cap). Default False so the configured
                ``timeout`` is authoritative.
            close_timeout: Budget (seconds) for the transport-drain
                during ``close()``. Forwarded to the underlying
                :class:`DqliteConnection`. The default (0.5 s) is
                sized for LAN; callers with higher-latency links or
                strict shutdown SLAs can override.
        """
        _validate_timeout(timeout)
        _validate_close_timeout(close_timeout)
        # Eager address parse so a typoed DSN surfaces as
        # ``InterfaceError`` at the operator's config-load site rather
        # than at first-use — the sibling ``DqliteConnection``
        # already parses here; mirror that contract at the dbapi
        # layer. Map the client's ``ValueError`` / ``TypeError`` to
        # PEP 249's ``InterfaceError`` ("problems with the database
        # interface rather than the database itself").
        if not isinstance(address, str):
            raise InterfaceError(
                f"address must be a 'host:port' string, got {type(address).__name__}"
            )
        try:
            _client_parse_address(address)
        except ValueError as e:
            raise InterfaceError(f"Invalid address: {e}") from e
        self._address = address
        self._database = database
        self._timeout = timeout
        self._max_total_rows = _validate_positive_int_or_none(max_total_rows, "max_total_rows")
        self._max_continuation_frames = _validate_positive_int_or_none(
            max_continuation_frames, "max_continuation_frames"
        )
        self._trust_server_heartbeat = trust_server_heartbeat
        self._close_timeout = close_timeout
        self._async_conn: DqliteConnection | None = None
        self._closed = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._loop_lock = threading.Lock()
        self._op_lock = threading.Lock()
        self._connect_lock: asyncio.Lock | None = None
        self._creator_thread = threading.get_ident()
        # PEP 249 optional extension. No driver path currently appends
        # here; callers can rely on the attribute existing.
        self.messages: list[tuple[type[Exception], Exception | str]] = []
        # 1-element list (mutable, captured by the finalizer) that
        # close() flips to True. Using a list avoids the finalizer
        # closing over ``self`` and preventing GC.
        self._closed_flag: list[bool] = [False]
        self._finalizer: weakref.finalize[Any, Any] | None = None
        # Track outstanding cursors weakly so Connection.close() can
        # scrub their state (stdlib sqlite3 cascades; buffered fetches
        # on a cursor whose Connection was externally closed used to
        # silently succeed against stale in-memory rows).
        self._cursors: weakref.WeakSet[Cursor] = weakref.WeakSet()

    def _check_thread(self) -> None:
        """Raise ProgrammingError if called from a different thread than the creator."""
        current = threading.get_ident()
        if current != self._creator_thread:
            raise ProgrammingError(
                f"Connection objects created in a thread can only be used in that "
                f"same thread. The object was created in thread id "
                f"{self._creator_thread} and this is thread id {current}."
            )

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Ensure a dedicated event loop is running in a background thread.

        This allows sync methods to work even when called from within
        an already-running async context (e.g. uvicorn).

        Registers a ``weakref.finalize`` the first time the loop is
        created so a Connection that's garbage-collected without an
        explicit ``close()`` still cleans up its thread. (GC'd connections
        used to leak daemon threads forever.)
        """
        if self._loop is not None and not self._loop.is_closed():
            return self._loop
        with self._loop_lock:
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
                self._thread.start()
                # Finalizer can't close over self — it'd keep the
                # Connection alive. Capture primitives only. The
                # closed-flag list is mutated by close() so the
                # finalizer knows whether to emit a leak warning.
                self._finalizer = weakref.finalize(
                    self,
                    _cleanup_loop_thread,
                    self._loop,
                    self._thread,
                    self._closed_flag,
                    self._address,
                )
        return self._loop

    def _run_sync[T](self, coro: Coroutine[Any, Any, T]) -> T:
        """Run an async coroutine from sync code.

        Submits the coroutine to the dedicated background event loop
        and blocks until the result is available. The operation lock
        ensures only one operation runs at a time, preventing wire
        protocol corruption from concurrent access. The coroutine's
        return type ``T`` is preserved so callers retain inferred
        result types (mirrors the sibling generic in
        ``DqliteConnection._run_protocol``).

        On sync-side timeout we cancel the future AND invalidate the
        underlying connection. The coroutine may have already written
        partial bytes to the socket before observing the cancel;
        invalidation poisons the wire stream so the next operation
        reconnects instead of reusing a torn protocol state.

        Lock acquisition is bounded by ``self._timeout`` so a same-
        thread re-entry from a signal handler (e.g. SIGTERM handler
        calling ``close()`` while ``execute()`` is mid-await) raises a
        clean ``InterfaceError`` instead of deadlocking on the
        non-reentrant ``threading.Lock``. Cross-thread waiters honour
        the same bound — long-running ops cannot trap a sibling
        thread's call indefinitely.
        """
        # ``threading.Lock.acquire(timeout=...)`` is interruptible by
        # SIGINT on CPython — a ``KeyboardInterrupt`` (or ``SystemExit``)
        # raised by the signal handler escapes ``acquire`` BEFORE the
        # ``try`` block below is entered, so the in-block KI cleanup
        # arm is skipped. If a prior in-flight call is still running on
        # the loop thread, it owns ``_in_use=True`` and the connection
        # is wedged for the life of the dbapi instance. Schedule a
        # defensive ``_invalidate`` so the next call reconnects with a
        # clean slate. Gate on ``_async_conn._in_use`` so a KI raised
        # during a quiet acquire (no prior op) does not invalidate
        # gratuitously.
        try:
            acquired = self._op_lock.acquire(timeout=self._timeout)
        except (KeyboardInterrupt, SystemExit):
            # The coroutine was never scheduled on the loop, so close
            # it explicitly to suppress "coroutine was never awaited"
            # ResourceWarnings (and free its frame).
            coro.close()
            if self._async_conn is not None and self._loop is not None and self._async_conn._in_use:
                with contextlib.suppress(RuntimeError):
                    self._loop.call_soon_threadsafe(
                        self._async_conn._invalidate,
                        InterfaceError("operation interrupted during op-lock acquire"),
                    )
            raise
        # Release the lock from a single finally that covers the
        # window between ``acquired = ...`` returning True and the
        # inner ``try:`` body — a KI/SystemExit raised by a signal
        # handler in that gap would otherwise leak the lock
        # permanently (subsequent ``_run_sync`` calls deadlock until
        # ``acquire(timeout=...)`` fires).
        try:
            if not acquired:
                coro.close()
                raise InterfaceError(
                    "another operation is in progress on this connection "
                    f"(could not acquire operation lock within {self._timeout}s — "
                    "may indicate re-entry from a signal handler or concurrent "
                    "use from another thread)"
                )
            loop = self._ensure_loop()
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            try:
                # Future.result() provides a happens-before memory barrier,
                # ensuring all writes by the event loop thread are visible here.
                return future.result(timeout=self._timeout)
            except TimeoutError as e:
                # Race check BEFORE calling ``cancel()`` /
                # ``_invalidate``: the coroutine may have completed
                # successfully between ``result(timeout=...)`` raising
                # TimeoutError and our cancel attempt landing. In that
                # case the operation actually persisted — raising
                # OperationalError now would cause the caller's retry
                # logic to re-run the op and, for non-idempotent
                # statements, duplicate the write. Honour the
                # successful completion instead.
                recovered_error: BaseException | None = None
                if (
                    future.done() and not future.cancelled()
                ):  # pragma: no cover - race: future completes mid-timeout
                    try:
                        return future.result(timeout=0)
                    except BaseException as recovered:
                        # Coroutine completed with an exception of its
                        # own (e.g. SQLITE_BUSY, leader flip mid-flight).
                        # Capture for chaining: the legacy "on sync-
                        # timeout, you get OperationalError" contract is
                        # preserved (we still raise OperationalError
                        # below), but the recovered exception is attached
                        # via __cause__ so the user can see the actual
                        # failure instead of an opaque "timed out"
                        # diagnostic.
                        recovered_error = recovered
                future.cancel()
                # Poison the underlying connection. The coroutine may have
                # half-written a request; the wire is in unknown state.
                # Fire-and-forget on the loop thread (don't await).
                if self._async_conn is not None:
                    # RuntimeError if the loop is already shutting down.
                    with contextlib.suppress(
                        RuntimeError
                    ):  # pragma: no cover - race: loop closing mid-schedule
                        loop.call_soon_threadsafe(
                            self._async_conn._invalidate,
                            OperationalError(f"sync timeout after {self._timeout}s"),
                        )
                # Wait a bounded time for the cancelled coroutine to
                # unwind. Without this, the next sync call can race the
                # still-running prior coroutine — both want the
                # underlying DqliteConnection's ``_in_use`` flag, and the
                # new op sees ``already in use`` even though from the
                # caller's perspective the previous operation already
                # raised. The 1s cap is enough for normal cancellation
                # to land; ``_invalidate`` above is the safety net for
                # a genuinely stuck coroutine.
                try:
                    future.result(timeout=1.0)
                except (
                    concurrent.futures.CancelledError,
                    concurrent.futures.TimeoutError,
                ):
                    pass
                except Exception:
                    # Unexpected: the cancelled coroutine terminated
                    # with something other than CancelledError /
                    # TimeoutError (e.g. a programming bug in a
                    # cleanup path). Outer OperationalError still
                    # surfaces for the caller; DEBUG-log the root
                    # cause so operators can see it instead of having
                    # it silently absorbed.
                    logger.debug(
                        "sync timeout: unexpected error during bounded cancel-wait",
                        exc_info=True,
                    )
                if recovered_error is not None:
                    # Surface the recovered exception via __cause__
                    # while preserving the OperationalError("timed out")
                    # type contract. SA's is_disconnect cause-walk and
                    # any caller printing exc_info will see the real
                    # cause instead of the timeout placeholder.
                    raise OperationalError(
                        f"Operation timed out after {self._timeout} seconds "
                        f"(coroutine completed with error: {recovered_error!r})"
                    ) from recovered_error
                raise OperationalError(f"Operation timed out after {self._timeout} seconds") from e
            except (KeyboardInterrupt, SystemExit):
                # KeyboardInterrupt / SystemExit raised inside the
                # caller's thread while it was blocked on Future.result.
                # The coroutine is still running on the background loop
                # thread, owns ``DqliteConnection._in_use=True``, and
                # without intervention every subsequent sync call would
                # fail with "another operation is in progress" — the
                # connection is wedged for life.
                #
                # Mirror the timeout cleanup: cancel the future, schedule
                # an _invalidate on the loop thread (so the wire state
                # is poisoned and the next call reconnects), then bound-
                # wait for the coroutine to unwind. Re-raise the original
                # KI/SystemExit (no ``from``) so the signal propagates
                # to the caller's frame as Python expects.
                #
                # Narrowed to ``KeyboardInterrupt | SystemExit`` (not
                # bare ``BaseException``) because ``Future.result`` on
                # a coroutine that raises a normal ``Exception``
                # subclass (every PEP 249 error inherits from
                # ``Exception``) re-raises that exception on the
                # calling thread — those must propagate to the caller
                # via the standard exception path, NOT trigger
                # invalidation.
                future.cancel()
                if self._async_conn is not None:
                    with contextlib.suppress(RuntimeError):
                        loop.call_soon_threadsafe(
                            self._async_conn._invalidate,
                            InterfaceError("operation interrupted"),
                        )
                with contextlib.suppress(BaseException):
                    future.result(timeout=1.0)
                raise
        finally:
            # Only release if we actually acquired. ``acquired`` is
            # always defined here because the surrounding ``try`` was
            # entered after the acquire — even if the acquire raised
            # KI, that path raised before reaching this try and the
            # finally does not run.
            if acquired:
                self._op_lock.release()

    async def _get_async_connection(self) -> DqliteConnection:
        """Get or create the underlying async connection."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            return self._async_conn

        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()

        async with self._connect_lock:
            if self._async_conn is not None:  # pragma: no cover - race: peer built conn mid-lock
                return self._async_conn

            self._async_conn = await _build_and_connect(
                self._address,
                database=self._database,
                timeout=self._timeout,
                max_total_rows=self._max_total_rows,
                max_continuation_frames=self._max_continuation_frames,
                trust_server_heartbeat=self._trust_server_heartbeat,
                close_timeout=self._close_timeout,
            )

        return self._async_conn

    def connect(self) -> None:
        """Eagerly establish the TCP session.

        Optional — the connection is lazy and the first cursor() or
        execute() will connect automatically. Call this to fail-fast
        when the cluster is unreachable, without allocating a cursor.
        Mirrors :meth:`AsyncConnection.connect`.
        """
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        # _get_async_connection is a coroutine; route through _run_sync
        # so we share the same loop-in-thread the cursor path uses.
        self._run_sync(self._get_async_connection())

    def close(self) -> None:
        """Close the connection."""
        # PEP 249 §6.1: close() must be idempotent ("further attempts
        # at .close() have no effect"). Check the closed flag BEFORE
        # the thread guard so a re-close from a finalizer / atexit /
        # ThreadPoolExecutor cleanup running on a non-creator thread
        # is a no-op rather than raising ProgrammingError. The first
        # close still must run on the creator thread (it tears down
        # the loop thread and primitives that are GIL-but-not-thread-
        # safe), so the thread check stays — just AFTER the closed
        # short-circuit. Mirrors the cursor-side resolution.
        if self._closed:
            return
        self._check_thread()
        self._closed = True
        # Flip the flag the finalizer reads so it knows this was an
        # explicit close (no ResourceWarning).
        self._closed_flag[0] = True
        # Cascade to tracked cursors so buffered fetches on them
        # stop silently answering from stale in-memory rows. stdlib
        # sqlite3.Connection.close() does the same. Writes go
        # directly to the cursor's private attributes so we bypass
        # the Cursor.close() path (which would re-dispatch through
        # Cursor.messages). Wrap in try/finally so a KI/SystemExit
        # landing mid-loop does not leave ``self._cursors`` populated
        # with stale references — see async sibling for rationale.
        try:
            for cur in list(self._cursors):
                cur._closed = True
                cur._rows = []
                cur._description = None
                cur._rowcount = -1
                cur._lastrowid = None
                # Mirror Cursor.close()'s consistent "no operation
                # performed" surface — the row index must be reset
                # alongside the buffer or a future rownumber accessor
                # change could expose stale post-close state.
                cur._row_index = 0
        finally:
            self._cursors.clear()
        # Detach the finalizer — it's about to do nothing useful, and
        # keeping it registered would double-stop the loop.
        if self._finalizer is not None:
            self._finalizer.detach()
            self._finalizer = None
        try:
            if self._loop is not None and not self._loop.is_closed():
                with contextlib.suppress(Exception):
                    self._run_sync(self._close_async())
        finally:
            with self._loop_lock:
                if self._loop is not None and not self._loop.is_closed():
                    self._loop.call_soon_threadsafe(self._loop.stop)
                    if self._thread is not None:
                        self._thread.join(timeout=_LOOP_THREAD_JOIN_TIMEOUT_SECONDS)
                    self._loop.close()
                    self._loop = None
                    self._thread = None
                # Drop the asyncio.Lock bound to the loop we just closed;
                # the lazy-create branch in _get_async_connection rebuilds it
                # against the next loop so the primitive never outlives its
                # owning event loop.
                self._connect_lock = None

    async def _close_async(self) -> None:
        """Async implementation of close -- runs on event loop thread."""
        if self._async_conn is not None:
            try:
                await self._async_conn.close()
            finally:
                self._async_conn = None

    @property
    def in_transaction(self) -> bool:
        """Whether the connection currently has an open transaction.

        Mirrors stdlib ``sqlite3.Connection.in_transaction``. Callers
        use this in shutdown paths to decide whether to commit or
        rollback. Never-connected or closed connections return False
        — by definition they cannot hold an open transaction. Delegates
        to the underlying client-layer :class:`DqliteConnection`.
        """
        self._check_thread()
        # Snapshot the reference once so a concurrent close() that nulls
        # ``_async_conn`` cannot land between the None-check and the
        # attribute read. ``bool(...)`` keeps the mock-adapter safety
        # from the stdlib-parity introduction.
        conn = self._async_conn
        if conn is None or self._closed:
            return False
        return bool(conn.in_transaction)

    def commit(self) -> None:
        """Commit any pending transaction.

        If the connection has never been used, this is a silent no-op
        (matches stdlib ``sqlite3`` and the existing "no spurious
        connect" contract). If the server reports "no transaction is
        active," that too is swallowed — and on this driver "no
        transaction is active" is the *common* case, because every
        statement auto-commits at the server unless an explicit
        ``BEGIN`` was issued (see class docstring / README
        "Transactions"). stdlib ``sqlite3.commit()`` silently succeeds
        in the same case, and callers should not have to tell the
        difference between an empty transaction and a successfully
        committed one.

        Operational caveat: on a leader flip mid-transaction, COMMIT
        can raise ``OperationalError`` with a code in
        ``dqlitewire.LEADER_ERROR_CODES``. The write MAY or MAY NOT
        have been persisted — Raft may already have replicated the
        commit log entry before the flip, or the flip may have
        occurred before the entry was appended. Callers cannot tell
        from the exception alone. Use idempotent DML
        (``INSERT OR REPLACE``, UPDATE on a unique key) or an
        out-of-band state-check before retrying to avoid duplicate
        writes. The same caveat applies to ``__exit__``'s clean-exit
        commit.
        """
        del self.messages[:]
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._async_conn is None:
            return
        # Local short-circuit when no transaction is active. Mirrors
        # stdlib ``sqlite3.Connection.commit`` which uses
        # ``sqlite3_get_autocommit`` to skip the wire round-trip.
        # ``_has_untracked_savepoint`` covers the case where the
        # tracker missed the BEGIN (quoted SAVEPOINT autobegin,
        # multi-statement batch the splitter could not see) — must
        # still issue a real COMMIT so the server-side autobegun tx
        # is closed.
        if not self._async_conn.in_transaction and not getattr(
            self._async_conn, "_has_untracked_savepoint", False
        ):
            return
        self._run_sync(self._commit_async())

    async def _commit_async(self) -> None:
        """Async implementation of commit."""
        if self._async_conn is None:
            raise InterfaceError("Connection is closed")
        try:
            # Route through ``_call_client`` so client-layer errors
            # (including ``DqliteConnectionError`` for an externally
            # invalidated connection) surface as PEP 249 ``Error``
            # subclasses, not raw client exceptions.
            await _call_client(self._async_conn.execute("COMMIT"))
        except OperationalError as e:
            if not _is_no_transaction_error(e):
                raise

    def rollback(self) -> None:
        """Roll back any pending transaction.

        Same silent-success contract as :meth:`commit` for "no active
        transaction" and for never-used connections. As with
        :meth:`commit`, "no active transaction" is the *common* case
        on this driver — see the class docstring for the autocommit-
        by-default contract.
        """
        del self.messages[:]
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._async_conn is None:
            return
        # See commit() — same local short-circuit applies. Saves a
        # wire round-trip on the autocommit-by-default common case.
        if not self._async_conn.in_transaction and not getattr(
            self._async_conn, "_has_untracked_savepoint", False
        ):
            return
        self._run_sync(self._rollback_async())

    async def _rollback_async(self) -> None:
        """Async implementation of rollback."""
        if self._async_conn is None:
            raise InterfaceError("Connection is closed")
        try:
            # See ``_commit_async``: route through ``_call_client`` so
            # client-layer failures surface as PEP 249 ``Error``
            # subclasses.
            await _call_client(self._async_conn.execute("ROLLBACK"))
        except OperationalError as e:
            if not _is_no_transaction_error(e):
                raise

    def cursor(self) -> Cursor:
        """Return a new Cursor object."""
        del self.messages[:]
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        cur = Cursor(self)
        self._cursors.add(cur)
        return cur

    @property
    def address(self) -> str:
        """Node address this connection was opened against.

        Read-only. Exposed so diagnostic layers (SQLAlchemy adapter,
        pool metrics, structured logs) can label events with the peer
        address without reaching into the private ``_address`` field.
        """
        return self._address

    def __repr__(self) -> str:
        state = "closed" if self._closed else ("connected" if self._async_conn else "unused")
        return f"<Connection address={self._address!r} database={self._database!r} {state}>"

    def __enter__(self) -> "Connection":
        # Eager connect to match ``AsyncConnection.__aenter__`` — both
        # context managers should fail at the ``with`` line when the
        # cluster is unreachable, not inside the body's first operation.
        try:
            self.connect()
        except BaseException:
            # Python does not call ``__exit__`` when ``__enter__`` raises,
            # so clean up partial state ourselves. ``close()`` is
            # idempotent and tolerates the never-connected case.
            with contextlib.suppress(Exception):
                self.close()
            raise
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # If no query has ever run, there's no transaction to commit or
        # roll back — nothing to do; the connection remains reusable,
        # matching stdlib sqlite3 / psycopg semantics.
        if self._async_conn is None:
            return
        if exc_type is None:
            # Clean exit: commit. Let exceptions propagate; silent
            # data loss is worse than a noisy failure.
            self.commit()
        else:
            # Body already raised; attempt rollback but don't mask
            # the original exception. Narrow except so programming
            # bugs still surface; DEBUG-log the rollback failure so
            # operators can tell silent-swallow from silent-success
            # — matching the async __aexit__ pattern.
            try:
                self.rollback()
            except (KeyboardInterrupt, SystemExit):
                # Signal interrupted the rollback (no asyncio.CancelledError
                # in sync context). Log the breadcrumb and re-raise so
                # the signal supersedes the body exception, matching
                # the async sibling and the client transaction()
                # ctxmgr's discipline.
                logger.debug(
                    "Connection.__exit__ (address=%s, id=%s): "
                    "rollback interrupted by signal after body raised",
                    self._address,
                    id(self),
                    exc_info=True,
                )
                raise
            except Exception:
                logger.debug(
                    "Connection.__exit__ (address=%s, id=%s): "
                    "rollback failed; propagating original body exception",
                    self._address,
                    id(self),
                    exc_info=True,
                )
        # Do NOT close — matches stdlib sqlite3.Connection.__exit__ and
        # psycopg. Callers who want eager close use ``conn.close()``
        # explicitly or go through a pool.
