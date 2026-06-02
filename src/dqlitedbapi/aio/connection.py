"""Async connection implementation for dqlite."""

import asyncio
import contextlib
import logging
import os
import time
import warnings
import weakref
from collections.abc import AsyncIterator, Iterable, Sequence
from types import TracebackType
from typing import Any, NoReturn, Self

from dqliteclient import (
    DEFAULT_CLOSE_TIMEOUT_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    DialFunc,
    DqliteConnection,
    get_current_pid,
)
from dqliteclient import parse_address as _client_parse_address
from dqlitedbapi import exceptions as _exc
from dqlitedbapi._constants import _is_int_not_bool
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import (
    _STDLIB_IMPLICIT_TX_VALUES,
    _SYNC_PHASES_MULTIPLIER,
    MAX_CONTINUATION_FRAMES_UPPER_BOUND,
    _build_and_connect,
    _is_no_transaction_error,
    _validate_close_timeout,
    _validate_timeout,
    _wrap_positive_int,
)
from dqlitedbapi.cursor import _call_client, _validate_executemany_seq_shape
from dqlitedbapi.exceptions import (
    AMBIGUOUS_COMMIT_CODES,
    AmbiguousCommitError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from dqlitedbapi.types import RowFactory
from dqlitewire import (
    DEFAULT_MAX_CONTINUATION_FRAMES as _DEFAULT_MAX_CONTINUATION_FRAMES,
)
from dqlitewire import (
    DEFAULT_MAX_TOTAL_ROWS as _DEFAULT_MAX_TOTAL_ROWS,
)
from dqlitewire import (
    sanitize_for_log,
)

__all__ = ["AsyncConnection"]

logger = logging.getLogger(__name__)


def _async_unclosed_warning(
    closed_flag: list[bool],
    connected_flag: list[bool],
    address: str,
    creator_pid: int,
    *,
    # Capture module globals as kwarg defaults: Py_FinalizeEx nulls them mid-finalize,
    # so a late callback would raise TypeError out of warnings/contextlib/sanitize_for_log.
    # get_current_pid is INTENTIONALLY NOT captured — fork-pid tests patch the module name;
    # the runtime deref below is try-wrapped to stay shutdown-safe.
    _warnings: Any = warnings,
    _contextlib: Any = contextlib,
    _sanitize_for_log: Any = sanitize_for_log,
) -> None:
    """Emit ResourceWarning when an AsyncConnection is GC'd without ``await close()``.

    Gate: skip on forked child (snapshots belong to the parent), on closed_flag (close or
    force_close_transport ran), and when never-connected (connected_flag False — else common
    ``conn = AsyncConnection(...); del conn`` flows warn spuriously).
    """
    # Shutdown-phase teardown can null the captured module refs; bail so the
    # unraisable-hook traceback doesn't fire from inside weakref._exitfunc.
    if _warnings is None or _contextlib is None or _sanitize_for_log is None:
        return
    # Read get_current_pid from module globals so the test fixture's patch is observed;
    # broad except so the Py_FinalizeEx phase-3 nulling surfaces as a silent no-op.
    try:
        if get_current_pid() != creator_pid:
            return
    except Exception:
        return
    if closed_flag[0] or not connected_flag[0]:
        return
    with _contextlib.suppress(RuntimeError):
        _warnings.warn(
            f"AsyncConnection(address={_sanitize_for_log(str(address))!r}) was "
            f"garbage-collected without await close(). Call "
            f"``await aconn.close()`` explicitly to avoid this warning "
            f"and to release the underlying socket promptly.",
            ResourceWarning,
            stacklevel=2,
        )


def _loop_affinity_exc_class(
    bound: asyncio.AbstractEventLoop | None,
) -> type[Exception]:
    """Pick the loop-affinity exception class.

    Closed/GC'd loop -> InterfaceError so cross-driver retry middleware reconnects;
    live-but-different loop -> ProgrammingError (a genuine programmer mistake).
    """
    if bound is None or bound.is_closed():
        return InterfaceError
    return ProgrammingError


def _format_loop_affinity_message(
    bound: asyncio.AbstractEventLoop | None,
    current: asyncio.AbstractEventLoop | None,
    site: str,
) -> str:
    """Build the loop-affinity message naming both loop identities so operators can tell a
    GC'd bound loop (replace the connection) from two concurrent loops (route the call)."""
    if bound is None:
        bound_descr = "garbage-collected (loop was closed and GC'd)"
    elif bound.is_closed():
        bound_descr = f"id=0x{id(bound):x} (closed)"
    else:
        bound_descr = f"id=0x{id(bound):x}"
    current_descr = f"id=0x{id(current):x}" if current is not None else "no running loop"
    return (
        f"AsyncConnection {site} called from a different event loop; "
        f"AsyncConnection instances are loop-bound and cannot be "
        f"reused across asyncio.run() invocations. "
        f"Originally bound loop: {bound_descr}; current loop: {current_descr}."
    )


def _cascade_cursors_closed(cursors: list[Any]) -> None:
    """Mark every cursor closed and scrub its result-set state.

    Extracted so force_close_transport can route through call_soon_threadsafe on a
    foreign-thread/live-loop combo (else siblings see a partially-cascaded snapshot).
    Direct attribute writes only (AsyncCursor.close is async); ``_closed = True`` is the
    load-bearing write that _check_closed gates on first.
    """
    for cur in cursors:
        cur._closed = True
        cur._rows = []
        cur._description = None
        cur._rowcount = -1
        cur._lastrowid = None
        cur._row_index = 0
        # Null the single-flight slot like AsyncCursor.close(), else __aenter__'s
        # _executing_task check fires "already executing" on a cascade-closed cursor.
        cur._executing_task = None
        with contextlib.suppress(AttributeError):
            del cur.messages[:]
        with contextlib.suppress(TypeError):
            cur._connection = weakref.proxy(cur._connection)


class AsyncConnection:
    """Async database connection, loop-bound.

    Binds to the first asyncio event loop on which any method runs.
    Subsequent calls from a different loop raise ``ProgrammingError``;
    instances are NOT reusable across ``asyncio.run()`` invocations or
    across threads with their own loops.

    Safe for concurrent tasks on the SAME loop: the internal
    ``_op_lock`` serialises in-flight operations so commit/execute/
    rollback cannot interleave.

    Transactions: each statement auto-commits at the server unless
    wrapped in an explicit ``BEGIN`` — this differs from PEP 249 §6's
    implicit-transaction model and from stdlib ``sqlite3``. See the
    README's "Transactions" section.
    """

    # PEP 249 optional extension: exception classes as attributes so cross-driver
    # code can write ``except aconn.Error:`` without importing the driver module.
    Error = _exc.Error
    Warning = _exc.Warning  # noqa: A003, N815 - PEP 249 §7 mandated class attr name
    InterfaceError = _exc.InterfaceError
    DatabaseError = _exc.DatabaseError
    DataError = _exc.DataError
    OperationalError = _exc.OperationalError
    IntegrityError = _exc.IntegrityError
    InternalError = _exc.InternalError
    ProgrammingError = _exc.ProgrammingError
    NotSupportedError = _exc.NotSupportedError
    AmbiguousCommitError = _exc.AmbiguousCommitError

    def __init__(
        self,
        address: str,
        *,
        database: str = "default",
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
        max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
        max_message_size: int | None = None,
        trust_server_heartbeat: bool = False,
        close_timeout: float = DEFAULT_CLOSE_TIMEOUT_SECONDS,
        dial_timeout: float | None = None,
        attempt_timeout: float | None = None,
        dial_func: DialFunc | None = None,
        busy_timeout: float = 5.0,
        session_mode: str | None = None,
    ) -> None:
        """Initialize connection (does not connect yet).

        ``timeout`` is per-RPC-phase, so one call can take up to roughly N × timeout
        end-to-end; wrap callers in ``asyncio.timeout(...)`` for a wall-clock deadline.
        """
        import math as _math

        _validate_timeout(timeout)
        _validate_close_timeout(close_timeout)
        if dial_timeout is not None:
            _validate_timeout(dial_timeout)
        if attempt_timeout is not None:
            _validate_timeout(attempt_timeout)
        if isinstance(busy_timeout, bool) or not isinstance(busy_timeout, (int, float)):
            raise TypeError(
                f"busy_timeout must be a number (seconds); got {type(busy_timeout).__name__}"
            )
        if not _math.isfinite(busy_timeout) or busy_timeout < 0:
            raise ValueError(
                f"busy_timeout must be a non-negative finite number; got {busy_timeout}"
            )
        # Eager address parse so a typoed DSN surfaces at construction, not first-use.
        if not isinstance(address, str):
            raise InterfaceError(
                f"address must be a 'host:port' string, got {type(address).__name__}"
            )
        if not isinstance(database, str):
            raise InterfaceError(f"database must be a str, got {type(database).__name__}")
        if not database:
            raise InterfaceError("database must be a non-empty string")
        if database != database.strip():
            raise InterfaceError(
                f"database must not have leading or trailing whitespace (got {database!r})"
            )
        try:
            _client_parse_address(address)
        except ValueError as e:
            raise InterfaceError(f"Invalid address: {e}") from e
        self._address = address
        self._database = database
        self._timeout = timeout
        self._max_total_rows = _wrap_positive_int(max_total_rows, "max_total_rows")
        self._max_continuation_frames = _wrap_positive_int(
            max_continuation_frames,
            "max_continuation_frames",
            upper=MAX_CONTINUATION_FRAMES_UPPER_BOUND,
        )
        # None falls back to the wire-layer default; the wire layer revalidates.
        self._max_message_size = max_message_size
        self._trust_server_heartbeat = trust_server_heartbeat
        self._close_timeout = close_timeout
        self._dial_timeout = dial_timeout
        self._attempt_timeout = attempt_timeout
        self._dial_func = dial_func
        # Shared with the PRAGMA setter; either tunes the other.
        self._busy_timeout: float = float(busy_timeout)
        # None consults env DQLITE_SESSION_MODE. ``_dqlite_session_mode`` is the live value
        # (cursor BEGIN-rewrite / SA characteristic mutate it); ``_default`` is what the SA
        # characteristic's reset_characteristic restores on pool checkin.
        from dqlitedbapi._pragma_intercept import (
            session_mode_default_from_env,
            validate_session_mode,
        )

        if session_mode is None:
            _resolved_session_mode = session_mode_default_from_env()
        else:
            _resolved_session_mode = validate_session_mode(session_mode)
        self._dqlite_session_mode: str = _resolved_session_mode
        self._dqlite_session_mode_default: str = _resolved_session_mode
        self._async_conn: DqliteConnection | None = None
        self._closed = False
        # Task owning the conn.transaction() ctxmgr body; commit()/rollback() use it to
        # reject stray transaction-control calls that would silently exit the transaction.
        self._transaction_owner: asyncio.Task[Any] | None = None
        # None means "return plain tuples". New cursors inherit this default.
        self._row_factory: RowFactory | None = None
        # Fork-after-init is unsupported: the inherited socket FD is shared with the parent
        # and asyncio primitives are bound to the parent's loop.
        self._creator_pid = os.getpid()
        # asyncio primitives MUST be created inside their loop; instantiate lazily so
        # construction can run outside a loop (SA builds these in sync glue).
        self._connect_lock: asyncio.Lock | None = None
        self._op_lock: asyncio.Lock | None = None
        # Weakref to the loop the locks bound to, so cross-loop use raises a clean
        # ProgrammingError rather than asyncio's "Future attached to a different loop".
        self._loop_ref: weakref.ref[asyncio.AbstractEventLoop] | None = None
        # Tuple value is the exception value per PEP 249 §13 (an instance, not a string).
        self.messages: list[tuple[type[Exception], Exception]] = []
        # Track cursors weakly so close() can scrub their state (else buffered fetches on an
        # externally-closed connection answer from stale in-memory rows).
        self._cursors: weakref.WeakSet[AsyncCursor] = weakref.WeakSet()
        # 1-element flag the finalizer reads. close() sets it True to skip the warning. No
        # async cleanup from the finalizer — the user owns the loop; the warning is the only
        # safe synchronous signal.
        self._closed_flag: list[bool] = [False]
        # Flipped True only after _ensure_connection builds the inner conn; the finalizer
        # skips the warning when False so a never-connected instance stays silent.
        self._connected_flag: list[bool] = [False]
        # close()/force_close_transport() detach() this so the finalizer and its captured
        # cells don't linger on the weakref global table after orderly close.
        self._finalizer: weakref.finalize[Any, Any] | None = weakref.finalize(
            self,
            _async_unclosed_warning,
            self._closed_flag,
            self._connected_flag,
            address,
            self._creator_pid,
        )

    def _ensure_locks(self) -> tuple[asyncio.Lock, asyncio.Lock]:
        """Lazy-create the asyncio locks on the running loop and pin the connection to it;
        a later call from a different loop raises ProgrammingError (rebinding the loop-bound
        StreamReader/Writer is unsafe)."""
        # A concurrent close() may have just nulled the locks; recreating them here would
        # bind fresh primitives to a dead connection and leak them past a second close()'s
        # early-return. Fail fast so no primitives are created.
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if get_current_pid() != self._creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {self._creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        loop = asyncio.get_running_loop()
        if self._connect_lock is None:
            self._loop_ref = weakref.ref(loop)
            self._connect_lock = asyncio.Lock()
            self._op_lock = asyncio.Lock()
        else:
            bound = self._loop_ref() if self._loop_ref is not None else None
            if bound is not loop:
                raise _loop_affinity_exc_class(bound)(
                    _format_loop_affinity_message(bound, loop, "was first used")
                )
        assert self._op_lock is not None  # created with _connect_lock above; narrows mypy
        return self._connect_lock, self._op_lock

    def _check_loop_binding(self) -> None:
        """Validate the current loop matches the bound loop WITHOUT binding on first use.

        For methods that don't take the locks (no-op/always-raise paths) but should still
        fail fast on cross-loop misuse without lazily binding the loop.
        """
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if get_current_pid() != self._creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {self._creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        self._check_loop_only()

    def _check_loop_only(self) -> None:
        """Validate the current loop matches the bound loop; do NOT check ``_closed`` and do
        NOT bind on first use.

        For AsyncCursor.__aiter__, where ``iter(cur) is cur`` must succeed even when closed
        (closed diagnostic deferred to first fetch) while cross-loop misuse still fails up
        front. Pid mismatch is checked before the loop (an inherited _loop_ref may resolve to
        the parent's loop, making the comparison meaningless) so retry middleware catching
        InterfaceError sees the right class.
        """
        creator_pid = getattr(self, "_creator_pid", None)
        if creator_pid is not None and get_current_pid() != creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        # getattr-safe: __new__/bare-instantiation test patterns can reach here uninitialised.
        loop_ref = getattr(self, "_loop_ref", None)
        if loop_ref is None:
            return  # not yet bound — don't bind from here
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Outside an async context; the cursor method no-ops/raises anyway.
            return
        bound = loop_ref()
        if bound is not loop:
            raise _loop_affinity_exc_class(bound)(
                _format_loop_affinity_message(bound, loop, "was first used")
            )

    async def _ensure_connection(self) -> DqliteConnection:
        """Ensure the underlying connection is established."""
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")

        # Fork guard on the FAST path too: the already-connected return below would otherwise
        # hand the parent's inner conn to a forked child, deferring the diagnostic past where
        # connect()/transaction() health-probes expect it.
        creator_pid = getattr(self, "_creator_pid", None)
        if creator_pid is not None and get_current_pid() != creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {creator_pid}, "
                f"current pid {get_current_pid()})"
            )

        # Cross-loop diagnostic on the fast path: connect() (eager fail-fast probe) does not
        # pre-check loop binding, so without this a cross-loop connect() returns silently.
        # Idempotent — safe for callers that already pre-check.
        self._check_loop_binding()

        if self._async_conn is not None:
            return self._async_conn

        connect_lock, _ = self._ensure_locks()
        async with connect_lock:
            if self._async_conn is not None:
                return self._async_conn

            built = await _build_and_connect(
                self._address,
                database=self._database,
                timeout=self._timeout,
                max_total_rows=self._max_total_rows,
                max_continuation_frames=self._max_continuation_frames,
                max_message_size=getattr(self, "_max_message_size", None),
                trust_server_heartbeat=self._trust_server_heartbeat,
                close_timeout=self._close_timeout,
                dial_timeout=getattr(self, "_dial_timeout", None),
                attempt_timeout=getattr(self, "_attempt_timeout", None),
                dial_func=getattr(self, "_dial_func", None),
                session_mode=getattr(self, "_dqlite_session_mode", "immediate"),
            )
            # A concurrent close() may have flipped _closed while we were suspended in
            # _build_and_connect; it observed _async_conn is None and returned, so publishing
            # ``built`` now would leak a socket nobody closes. Close it and signal instead.
            # Shield so an outer cancel can't interrupt the inner close and leak the transport.
            # CancelledError is NOT suppressed: shield runs the close in the background while
            # the outer await re-raises the cancel — swallowing it would break cooperative
            # cancellation; suppress only absorbs non-cancel close errors (stale-transport
            # OSError) so the InterfaceError below is the user-visible signal.
            if self._closed:
                # Explicit observer callback so the shield's implicit Task isn't orphaned.
                from dqliteclient.cluster import _observe_drain_exception

                inner_drain = asyncio.ensure_future(built.close())
                inner_drain.add_done_callback(_observe_drain_exception)
                with contextlib.suppress(Exception):
                    await asyncio.shield(inner_drain)
                raise InterfaceError(f"Connection is closed (id={id(self)})")
            self._async_conn = built
            # Arm the finalizer's "anything to clean up" gate.
            self._connected_flag[0] = True

        return self._async_conn

    async def connect(self) -> None:
        """Eagerly establish the TCP session.

        Optional — the first cursor()/execute() connects lazily. Await this to fail-fast
        when the cluster is unreachable without allocating a cursor.
        """
        del self.messages[:]  # every public method clears messages first
        await self._ensure_connection()

    async def close(self) -> None:
        """Close the connection.

        Serializes with any in-flight op via ``_op_lock`` so we never tear down the protocol
        mid-execute/mid-commit (which surfaces mysterious "connection closed" errors).
        """
        if self._closed:
            return
        del self.messages[:]  # PEP 249 §6.1.1
        # Fork-after-init: the socket FD is shared with the parent and op_lock is bound to
        # the parent's loop; driving async teardown would FIN the parent or hang. Flip to
        # closed and drop references quietly.
        if get_current_pid() != self._creator_pid:
            self._closed = True
            self._closed_flag[0] = True
            if self._finalizer is not None:
                self._finalizer.detach()
                self._finalizer = None
            # Walk the inner conn's fork-close path before dropping it: a parent-loop
            # _pending_drain Task GC'd in the child trips "Task was destroyed but it is
            # pending". _protocol/_db_id likewise reference unusable parent-loop pairs.
            if self._async_conn is not None:
                inner = self._async_conn
                pending = getattr(inner, "_pending_drain", None)
                if pending is not None:
                    with contextlib.suppress(Exception):
                        inner._pending_drain = None
                with contextlib.suppress(Exception):
                    inner._protocol = None
                    inner._db_id = None
            self._async_conn = None
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            self._cursors.clear()
            self._transaction_owner = None
            return
        # Set _closed first so a task waiting on the lock sees closed as soon as it acquires;
        # then drain the in-flight op (if any) under the lock.
        self._closed = True
        self._closed_flag[0] = True
        finalizer = getattr(self, "_finalizer", None)
        if finalizer is not None:
            finalizer.detach()
            self._finalizer = None

        def _cascade_cursors() -> None:
            """Run the cursor cascade with direct attribute writes (AsyncCursor.close is
            async); ``_closed = True`` is the load-bearing write that _check_closed gates."""
            try:
                for cur in list(self._cursors):
                    cur._closed = True
                    cur._rows = []
                    cur._description = None
                    cur._rowcount = -1
                    cur._lastrowid = None
                    cur._row_index = 0
                    # Null the single-flight slot like AsyncCursor.close() (see
                    # _cascade_cursors_closed) so __aenter__ doesn't report
                    # "already executing" on a cascade-closed cursor.
                    cur._executing_task = None
                    del cur.messages[:]
                    with contextlib.suppress(TypeError):
                        cur._connection = weakref.proxy(cur._connection)
            finally:
                self._cursors.clear()

        if self._async_conn is None:
            # Never-connected: no in-flight op to race; cascade outside the lock. Null the
            # lazy locks so a reuse on a different loop can't observe a dead-loop primitive.
            _cascade_cursors()
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            self._transaction_owner = None
            return
        # Use the already-bound op_lock directly (_ensure_locks now raises on _closed).
        assert self._op_lock is not None
        op_lock = self._op_lock
        op_lock_timed_out = False
        try:
            # Bound the acquire by _SYNC_PHASES_MULTIPLIER * timeout to match the sync
            # sibling's overall budget so the async surface doesn't false-time-out on benign
            # first-call-after-connect latency. timeout (NOT close_timeout) is the per-phase
            # budget; close_timeout is only the transport-drain window.
            phases_budget = _SYNC_PHASES_MULTIPLIER * self._timeout
            async with asyncio.timeout(phases_budget):
                async with op_lock:
                    # Cascade INSIDE the lock so a concurrent in-flight fetch (also under the
                    # lock) can't see partial state and raise "no results to fetch" instead
                    # of "Cursor is closed".
                    _cascade_cursors()
                    if self._async_conn is not None:
                        await self._async_conn.close()
                        self._async_conn = None
        except TimeoutError:
            # Sibling holds op_lock past the budget. Force-close synchronously so SIGTERM/
            # dispose don't hang; the sibling's pending read sees EOF and surfaces a transport
            # error. The user's close-racing-op contract violation thus surfaces as a
            # transport failure rather than a multi-minute hang.
            op_lock_timed_out = True
            # Synchronous, idempotent, never raises; nulls _async_conn itself.
            self.force_close_transport()
        finally:
            # A CancelledError during the op_lock acquire skips the underlying close, but
            # _closed=True makes a retry close() return immediately — leaking the socket.
            # Best-effort shielded close here, idempotent against the successful path.
            if self._async_conn is not None:
                # Explicit observer so the shield's implicit Task isn't orphaned and any
                # eventual raise doesn't surface "Task exception was never retrieved".
                from dqliteclient.cluster import _observe_drain_exception

                inner_drain = asyncio.ensure_future(self._async_conn.close())
                inner_drain.add_done_callback(_observe_drain_exception)
                try:
                    await asyncio.shield(inner_drain)
                except InterfaceError as exc:
                    # Underlying conn still in_use by a sibling task (cross-task close
                    # mid-op). The sibling's _run_protocol finally only resets _in_use and
                    # closes nothing, so force-close the writer to reap the transport instead
                    # of leaking it; the sibling's pending read sees EOF and invalidates.
                    logger.warning(
                        "AsyncConnection.close (id=%s): underlying "
                        "connection still in use by a sibling task; "
                        "force-closing the writer synchronously. The "
                        "sibling's in-flight operation will raise a "
                        "transport error. cause=%r",
                        id(self),
                        exc,
                    )
                    inner = self._async_conn
                    proto = getattr(inner, "_protocol", None)
                    if proto is not None:
                        writer = getattr(proto, "_writer", None)
                        if writer is not None:
                            with contextlib.suppress(Exception):
                                writer.close()
                    # Drain a _pending_drain scheduled by a sibling's _invalidate: the
                    # cross-task fallback bypassed _close_impl (which normally awaits it), so
                    # the inner conn — about to be nulled — would be the task's only
                    # reachability and GC prints "Task was destroyed but it is pending".
                    pending = getattr(inner, "_pending_drain", None)
                    if pending is not None and not pending.done():
                        # Narrow suppress: KI/SystemExit propagate; CancelledError re-delivers
                        # at the next await. Canonical shield+suppress idiom.
                        with contextlib.suppress(Exception, asyncio.CancelledError):
                            await asyncio.shield(pending)
                    # Fall through (no ``return``): a return inside finally silently discards a
                    # propagating cancel/KI/SE from the outer try, breaking TaskGroup parents'
                    # observation of a child cancel during close.
                except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    # Fresh signal after the shielded close: cancel the inner _pending_drain
                    # before nulling our reference (else GC of the unreachable inner emits
                    # "Task was destroyed but it is pending"). Already on a cancel arm — do
                    # NOT await; the next tick reaps it.
                    inner = self._async_conn
                    if inner is not None:
                        pending = getattr(inner, "_pending_drain", None)
                        if pending is not None and not pending.done():
                            pending.cancel()
                    # Force-close synchronously so the writer/FD is reaped: shield only blocks
                    # the FIRST cancel, and outer asyncio.timeout deadlines also land here, so
                    # the writer may still be open. Idempotent; nulls _async_conn itself.
                    self.force_close_transport()
                    self._async_conn = None
                    self._connect_lock = None
                    self._op_lock = None
                    self._loop_ref = None
                    self._transaction_owner = None
                    # Redundant finalizer detach (top-of-close already ran) so a future
                    # refactor reordering that detach can't leak the registry entry on cancel.
                    finalizer = getattr(self, "_finalizer", None)
                    if finalizer is not None:
                        finalizer.detach()
                        self._finalizer = None
                    raise
                except Exception:
                    logger.debug(
                        "AsyncConnection.close (id=%s): underlying close failed",
                        id(self),
                        exc_info=True,
                    )
                    # Symmetric with the cancel arm above: a non-cancel
                    # raise from the underlying ``close()`` mid-drain
                    # could leave the writer open. Force-close the
                    # transport synchronously so the FD is reaped.
                    # Idempotent and never raises.
                    if self._async_conn is not None:
                        self.force_close_transport()
                self._async_conn = None
            # Reset the locks *after* closing so any task that was
            # parked on ``op_lock`` observes the
            # "_closed -> raise InterfaceError" re-check before it
            # touches the now-None primitive.
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            # Clear the transaction-owner slot as a defensive backstop.
            # The set-inside-try discipline in ``transaction()`` already
            # prevents BaseException-window leaks under normal flow, but
            # an instance-reuse path that somehow inherited a stale
            # token (test fixtures, signal-window edge cases) recovers
            # cleanly after close(). Deferred until AFTER the underlying
            # close has completed so a concurrent task that is mid-
            # ``transaction()`` is not deprived of its slot ownership
            # by a foreign-task close() — the in-flight transaction
            # task retains the slot for as long as the connection was
            # operable; once the protocol is gone, no future
            # ``transaction()`` call can succeed and the slot value is
            # purely cosmetic.
            self._transaction_owner = None
        if op_lock_timed_out:
            # Surface the timed-out wait to the caller so SIGTERM/
            # dispose handlers can log the unclean shutdown. The
            # transport was force-closed inside the except arm; the
            # sibling task's in-flight RPC will observe a transport
            # error on next yield. ``raise`` here (outside the
            # finally) so the InterfaceError replaces — rather than
            # masks — any underlying close failure logged to debug.
            raise InterfaceError(
                f"close timed out after {self._timeout}s waiting for "
                "in-flight operation; transport force-closed"
            )

    async def aclose(self) -> None:
        """PEP 525 / ``contextlib.aclosing``-compatible alias for :meth:`close`."""
        await self.close()

    def _null_loop_bound_slots(self) -> None:
        """Null the lazy loop-bound primitives so a closed connection doesn't keep the
        (typically dead) loop and its asyncio.Lock objects reachable."""
        with contextlib.suppress(AttributeError):
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            self._transaction_owner = None

    def force_close_transport(self) -> None:
        """Synchronously tear down the underlying socket transport.

        Last-resort cleanup for finalize paths running outside any event loop (GC sweep,
        atexit, SA finalize outside a greenlet). Idempotent; never raises.

        NOT safe against a live loop from a non-loop thread: StreamWriter.close() is not
        thread-safe. Production callers hit the loop-already-dead path; writer-close and
        pending-drain cancel are loop-aware (call_soon_threadsafe on a foreign live loop,
        direct otherwise). On the owning loop with N open cursors the cascade is O(N) inline
        CPU — await close() instead for cooperative teardown.
        """
        with contextlib.suppress(AttributeError):  # tolerate __new__-built fixtures
            del self.messages[:]
        # Mark closed unconditionally so any path here counts as explicit cleanup (no
        # spurious GC warning) and a follow-up close() short-circuits. Without _closed=True,
        # terminate() reaped the writer but cursor() still succeeded against a dead transport.
        self._closed_flag[0] = True
        self._closed = True
        # Cascade closed state to cursors before the inner snapshot, else they keep populated
        # result-set state and strong _connection refs that defeat the weakref.proxy swap.
        # Loop-aware routing: a foreign-thread caller mutating cursor fields directly races
        # siblings mid-fetch — each write is GIL-atomic but the SEQUENCE is not, so a sibling
        # could read a partially-cascaded snapshot. Defer via call_soon_threadsafe then.
        cursors = getattr(self, "_cursors", None)
        if cursors is not None:
            bound_loop_ref = getattr(self, "_loop_ref", None)
            bound_loop = bound_loop_ref() if bound_loop_ref is not None else None
            running = None
            with contextlib.suppress(RuntimeError):
                running = asyncio.get_running_loop()
            cursors_snapshot = list(cursors)
            cursors.clear()
            # Pre-set cur._closed synchronously (single GIL-atomic write, _check_closed reads
            # only it) so a sibling racing fetchone() before the deferred cascade sees the
            # closed cursor immediately — else it could return rows from a dead transport.
            for cur in cursors_snapshot:
                with contextlib.suppress(AttributeError):
                    cur._closed = True
            if bound_loop is None or bound_loop.is_closed() or running is bound_loop:
                _cascade_cursors_closed(cursors_snapshot)
            else:
                bound_loop.call_soon_threadsafe(_cascade_cursors_closed, cursors_snapshot)
        # Detach the finalizer — symmetric with the sync sibling's
        # ``self._finalizer.detach()`` call paths in
        # ``force_close_transport``. Keeps the ``weakref`` global
        # table free of stale entries after the
        # transport-level force-close path.
        finalizer = getattr(self, "_finalizer", None)
        if finalizer is not None:
            finalizer.detach()
            self._finalizer = None
        inner = self._async_conn
        if inner is None:
            self._async_conn = None
            self._null_loop_bound_slots()
            return
        # Fork-after-init: writer.close() on the inherited FD would FIN the parent's
        # connection. Drop the local reference instead.
        if get_current_pid() != self._creator_pid:
            self._async_conn = None
            self._null_loop_bound_slots()
            return
        # Disarm the inner conn's ResourceWarning finalizer before reaping the transport:
        # its gate fails open once _async_conn is nulled, warning on the conn we're closing.
        # close() detaches it via _close_impl; this path doesn't route through close().
        inner_closed_flag = getattr(inner, "_closed_flag", None)
        if isinstance(inner_closed_flag, list) and inner_closed_flag:
            inner_closed_flag[0] = True
        inner_finalizer = getattr(inner, "_finalizer", None)
        if inner_finalizer is not None:
            with contextlib.suppress(Exception):
                inner_finalizer.detach()
            inner._finalizer = None
        # Close the writer if present; the cleanup tail below runs unconditionally so a
        # post-_invalidate state (cleared _protocol / None _writer) doesn't skip the null-out.
        proto = getattr(inner, "_protocol", None)
        writer = getattr(proto, "_writer", None) if proto is not None else None
        if writer is not None:
            # StreamWriter.close() is not thread-safe; defer to call_soon_threadsafe when the
            # loop is alive on a foreign thread, call directly on the owning thread or a dead
            # loop.
            bound_loop_ref = getattr(self, "_loop_ref", None)
            bound_loop = bound_loop_ref() if bound_loop_ref is not None else None
            running = None
            with contextlib.suppress(RuntimeError):
                running = asyncio.get_running_loop()
            try:
                if bound_loop is None or bound_loop.is_closed() or running is bound_loop:
                    writer.close()
                else:
                    bound_loop.call_soon_threadsafe(writer.close)
            except Exception:  # noqa: BLE001 - last-resort cleanup
                logger.debug(
                    "AsyncConnection.force_close_transport (id=%s): "
                    "writer.close() raised; ignoring",
                    id(self),
                    exc_info=True,
                )
        # Reap the inner's _pending_drain (we can't await it here). Task.cancel() is not
        # thread-safe, so defer via call_soon_threadsafe on a foreign live loop, cancel
        # directly on the owning thread or a dead loop. Bounded re-snapshot loop: a concurrent
        # _invalidate can create a fresh drain task between snapshot and null-out, orphaning it
        # ("Task was destroyed but it is pending" at GC) without re-snapshotting.
        resnapshot_cap = 3
        for _attempt in range(resnapshot_cap):
            pending = getattr(inner, "_pending_drain", None)
            with contextlib.suppress(Exception):
                inner._pending_drain = None
            if pending is None or pending.done():
                break
            with contextlib.suppress(Exception):
                try:
                    pending_loop = pending.get_loop()
                except Exception:
                    pending_loop = None
                running = None
                with contextlib.suppress(RuntimeError):
                    running = asyncio.get_running_loop()
                if pending_loop is None or pending_loop.is_closed() or running is pending_loop:
                    pending.cancel()
                else:
                    # Live loop, foreign thread: schedule via call_soon_threadsafe and observe
                    # the cancel via a done-callback, else asyncio logs "Task exception was
                    # never retrieved" at GC.
                    def _cancel_and_observe(target: asyncio.Task[Any]) -> None:
                        target.cancel()

                        def _observe(t: asyncio.Task[Any]) -> None:
                            # Narrow to Exception — KI/SystemExit propagate through callbacks.
                            if not t.cancelled():
                                with contextlib.suppress(Exception):
                                    t.exception()

                        target.add_done_callback(_observe)

                    pending_loop.call_soon_threadsafe(_cancel_and_observe, pending)
        else:
            # Cap exhausted: a racing _invalidate keeps creating fresh drain tasks. Final
            # defensive null-out + WARNING so operators see the pathological feedback loop.
            with contextlib.suppress(Exception):
                inner._pending_drain = None
            logger.warning(
                "AsyncConnection.force_close_transport: inner._pending_drain still "
                "set after %d re-snapshot iterations; cancelling residual task to "
                "avoid 'Task was destroyed but it is pending' at GC. This indicates "
                "a pathological _invalidate feedback loop on inner conn.",
                resnapshot_cap,
            )
        self._async_conn = None
        self._null_loop_bound_slots()

    @property
    def in_transaction(self) -> bool:
        """Whether the connection currently has an open transaction.

        Divergence: stdlib raises on a closed connection; this driver returns False (closed /
        never-connected can't hold a transaction) so it's safe in shutdown commit-vs-rollback
        paths. Raises on cross-loop read; closed short-circuits before the loop check so a
        foreign-loop reader of a closed connection gets False, not a loop error. A
        never-awaited connection returns False from any loop (binding only after first await).
        """
        # Snapshot once: a concurrent close() may null _async_conn between the None-check and
        # the attribute read.
        conn = self._async_conn
        if conn is None or self._closed:
            return False
        self._check_loop_only()
        return bool(conn.in_transaction)

    @property
    def autocommit(self) -> "bool | int":
        """``True`` — dqlite is autocommit-by-default (every statement commits unless an
        explicit ``BEGIN`` was issued). The SA adapter exposes ``False`` since it wraps in
        explicit BEGIN/COMMIT.

        Setter accepts ``True`` or stdlib's LEGACY_TRANSACTION_CONTROL (``-1``) — both no-op
        the wire layer but round-trip through the getter; anything else raises
        NotSupportedError. Raises InterfaceError on closed/post-fork.
        """
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        creator_pid = getattr(self, "_creator_pid", None)
        if creator_pid is not None and get_current_pid() != creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        return getattr(self, "_autocommit_value", True)

    @autocommit.setter
    def autocommit(self, value: object) -> None:
        with contextlib.suppress(AttributeError):  # PEP 249 §6.4; messages may not exist yet
            del self.messages[:]
        self._check_loop_binding()  # even the no-op accept must surface cross-loop misuse
        # Accept True or LEGACY_TRANSACTION_CONTROL (-1); exact-int gate matches stdlib's C
        # discipline (loose ``== -1`` would accept -1.0 / Decimal('-1') / custom __eq__).
        if value is True:
            self._autocommit_value: bool | int = value
            return
        if _is_int_not_bool(value) and value == -1:
            self._autocommit_value = -1
            return
        raise NotSupportedError(
            "dqlite operates in autocommit-by-default mode; the autocommit "
            "flag cannot be turned off at the dbapi level. Wrap your "
            "statements in explicit BEGIN/COMMIT (issued via cursor.execute) "
            "to control transaction boundaries instead."
        )

    @property
    def isolation_level(self) -> "str | None":
        """stdlib pre-3.12 ``isolation_level``-parity surface.

        Setter input round-trips through the getter (default None = autocommit sentinel).
        Raises InterfaceError on closed/post-fork.
        """
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        creator_pid = getattr(self, "_creator_pid", None)
        if creator_pid is not None and get_current_pid() != creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        return getattr(self, "_isolation_level_value", None)

    @isolation_level.setter
    def isolation_level(self, value: object) -> None:
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        self._check_loop_binding()
        # Store the caller's input so the getter round-trips.
        if value is None:
            self._isolation_level_value: str | None = value
            return
        if isinstance(value, str) and value.upper() in _STDLIB_IMPLICIT_TX_VALUES:
            # stdlib parity: isolation_level reads back uppercased.
            self._isolation_level_value = value.upper()
            return
        raise ProgrammingError(
            f"isolation_level must be None or one of "
            f"{sorted(_STDLIB_IMPLICIT_TX_VALUES)!r}; got {value!r}. "
            f"dqlite is fixed-mode autocommit at the wire layer; the "
            f"accepted values are stdlib pre-3.12 parity no-ops. Use "
            f"explicit BEGIN/COMMIT via cursor.execute to control "
            f"transaction boundaries."
        )

    async def commit(self) -> None:
        """Commit any pending transaction.

        No-op if never used or if the server reports "no transaction active" (the common case
        here — autocommit-by-default unless an explicit ``BEGIN`` was issued).

        Caveats: losing leadership after the COMMIT entry is submitted raises
        AmbiguousCommitError with the write in doubt (use idempotent DML before retry);
        a plain not-leader rejection is a clean failure and raises OperationalError. An
        outer asyncio.timeout cancels without re-classifying to TimeoutError, bypassing
        the phase-aware diagnostic — treat CancelledError as "invalidate the connection".
        """
        # Loop check BEFORE the messages-clear so a stray cross-loop commit() doesn't scribble
        # the bound loop's messages, and the owner-Task check below doesn't misfire cross-loop.
        self._check_loop_only()
        del self.messages[:]  # PEP 249 §6.1.1 (every invocation including the raise arm)
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if self._async_conn is None:
            return
        # Reject stray commit() from inside a conn.transaction() body: the ctxmgr owns
        # boundaries; a body commit silently ends the transaction (a data-correctness hazard).
        if (
            self._transaction_owner is not None
            and self._transaction_owner is asyncio.current_task()
        ):
            raise InterfaceError(
                "commit() cannot be issued inside conn.transaction(); "
                "the context manager owns transaction boundaries — "
                "exit the ``async with`` block first."
            )
        # Cancel-after-invalidate: a cancelled prior commit/rollback invalidates the inner
        # conn AND clears in_transaction; a naive retry would short-circuit on the False flag
        # and hide partial-commit ambiguity. Fast-path raise here; authoritative recheck under
        # op_lock catches a sibling _invalidate racing the acquire.
        if getattr(self._async_conn, "_protocol", "_sentinel") is None:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}); reconnect before "
                "retrying commit / rollback. The prior call may have "
                "reached the leader before cancel landed; server-side "
                "transaction state is ambiguous."
            )
        _, op_lock = self._ensure_locks()
        # Bound the acquire by _SYNC_PHASES_MULTIPLIER * timeout (same as close()) so commit()
        # doesn't hang indefinitely on a sibling parked in a slow reader.read() (especially
        # under trust_server_heartbeat) while still absorbing the multi-phase first-call budget.
        commit_budget = _SYNC_PHASES_MULTIPLIER * self._timeout
        entered_lock = False
        # True while the COMMIT round-trip is initiated but unconfirmed. An interrupt then
        # leaves partial wire state, so force-close: raw CancelledError doesn't trip SA's
        # is_disconnect, leaving an ambiguous-state slot in the pool.
        request_in_flight = False
        # Entry time so the TimeoutError arm can annotate elapsed-vs-budget when an outer
        # asyncio.timeout fires first and the message would otherwise name our budget.
        _start_monotonic = time.monotonic()
        try:
            async with asyncio.timeout(commit_budget):
                async with op_lock:
                    entered_lock = True
                    # Re-check under the lock so a concurrent close() or a sibling _invalidate
                    # racing the acquire can't slip through the in_transaction-False
                    # short-circuit and mask partial-commit ambiguity.
                    if (
                        self._closed
                        or self._async_conn is None
                        or getattr(self._async_conn, "_protocol", "_sentinel") is None
                    ):
                        if self._async_conn is not None and self._closed is False:
                            raise InterfaceError(
                                f"Connection invalidated (id={id(self)}); reconnect "
                                "before retrying commit / rollback. The prior call "
                                "may have reached the leader before cancel landed; "
                                "server-side transaction state is ambiguous."
                            )
                        raise InterfaceError(f"Connection is closed (id={id(self)})")
                    # Clear messages under the lock so the contract is atomic with the op.
                    del self.messages[:]
                    # Read in_transaction under the lock so a stale True doesn't route us into
                    # a wasted COMMIT round-trip; it already ORs the untracked-savepoint flag.
                    if not getattr(self._async_conn, "in_transaction", False):
                        return
                    try:
                        request_in_flight = True
                        # The C-level busy_timeout callback fires on COMMIT too; wrap with the
                        # SQLite-curve retry, bounded by op_lock and the surrounding timeout.
                        from dqlitedbapi._busy_retry import (
                            _resolve_busy_timeout_seconds,
                            retry_async_on_busy,
                        )

                        _inner = self._async_conn
                        await retry_async_on_busy(
                            _resolve_busy_timeout_seconds(self),
                            lambda: _call_client(_inner.execute("COMMIT")),
                        )
                        request_in_flight = False
                    except OperationalError as e:
                        # Wire round-trip completed; state is well-defined.
                        request_in_flight = False
                        if _is_no_transaction_error(e):
                            return
                        # Leadership lost after the COMMIT entry was submitted (in doubt):
                        # rewrap as AmbiguousCommitError so OperationalError-catching middleware
                        # still sees it while retry code can branch on the in-doubt shape.
                        # Retrying non-idempotent DML here risks duplicate writes. A plain
                        # not-leader rejection is a clean pre-apply failure -> stays Operational.
                        if e.code in AMBIGUOUS_COMMIT_CODES:
                            raise AmbiguousCommitError(
                                "ambiguous commit: leadership lost during "
                                "COMMIT; the write may or may not have "
                                f"been persisted. Original: {e}",
                                code=e.code,
                                raw_message=getattr(e, "raw_message", None),
                            ) from e
                        raise
        except TimeoutError as e:
            # entered_lock distinguishes phases: "op_lock acquire" (a sibling held the lock —
            # programmer bug) vs "COMMIT round-trip" (wire RTT stalled — network bug).
            # OperationalError so SA's is_disconnect invalidates the slot; state is ambiguous.
            if request_in_flight:
                self.force_close_transport()
            phase = "COMMIT round-trip" if entered_lock else "op_lock acquire"
            # elapsed << budget means an outer cancel/timeout cut us short and CPython
            # converted it to our local TimeoutError; annotate so operators can correlate.
            _elapsed = time.monotonic() - _start_monotonic
            cause_hint = ""
            if _elapsed < commit_budget * 0.95:
                cause_hint = (
                    f" (elapsed {_elapsed:.2f}s < budget {commit_budget}s; "
                    "outer scope or sibling cancel likely interrupted)"
                )
            raise OperationalError(
                f"commit {phase} timed out after {commit_budget}s "
                f"(id={id(self)}){cause_hint}; connection state ambiguous; reconnect.",
                code=None,
            ) from e
        except BaseException:
            # Outer-cancel/signal during the COMMIT round-trip: wire state is partial.
            # Force-close so SA's is_disconnect invalidates the slot (raw CancelledError
            # doesn't trip it, leaving the ambiguous-state conn in the pool).
            if request_in_flight:
                self.force_close_transport()
            raise

    async def rollback(self) -> None:
        """Roll back any pending transaction. Same no-op and outer-cancel caveats as
        :meth:`commit`."""
        self._check_loop_only()  # before messages-clear; see commit()
        del self.messages[:]  # PEP 249 §6.1.1
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if self._async_conn is None:
            return
        # Same conn.transaction() ctxmgr-owns-boundaries gate as commit().
        if (
            self._transaction_owner is not None
            and self._transaction_owner is asyncio.current_task()
        ):
            raise InterfaceError(
                "rollback() cannot be issued inside conn.transaction(); "
                "the context manager owns transaction boundaries — "
                "exit the ``async with`` block first."
            )
        # Same invalidated-inner detection as commit(); under-lock recheck below.
        if getattr(self._async_conn, "_protocol", "_sentinel") is None:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}); reconnect before "
                "retrying commit / rollback."
            )
        _, op_lock = self._ensure_locks()
        # Bound the acquire as in commit().
        rollback_budget = _SYNC_PHASES_MULTIPLIER * self._timeout
        entered_lock = False
        request_in_flight = False
        _start_monotonic = time.monotonic()
        try:
            async with asyncio.timeout(rollback_budget):
                async with op_lock:
                    entered_lock = True
                    # Re-check under the lock for the same race as commit().
                    if (
                        self._closed
                        or self._async_conn is None
                        or getattr(self._async_conn, "_protocol", "_sentinel") is None
                    ):
                        if self._async_conn is not None and self._closed is False:
                            raise InterfaceError(
                                f"Connection invalidated (id={id(self)}); reconnect "
                                "before retrying commit / rollback."
                            )
                        raise InterfaceError(f"Connection is closed (id={id(self)})")
                    del self.messages[:]
                    # Read in_transaction under the lock; see commit() (avoids wasted RTT).
                    if not getattr(self._async_conn, "in_transaction", False):
                        return
                    try:
                        request_in_flight = True
                        # Busy-curve retry as in commit(); retry_async_on_busy retries only on
                        # SQLITE_BUSY and propagates cancel/invalidate, so ROLLBACK can't
                        # re-issue.
                        from dqlitedbapi._busy_retry import (
                            _resolve_busy_timeout_seconds,
                            retry_async_on_busy,
                        )

                        _inner = self._async_conn
                        await retry_async_on_busy(
                            _resolve_busy_timeout_seconds(self),
                            lambda: _call_client(_inner.execute("ROLLBACK")),
                        )
                        request_in_flight = False
                    except OperationalError as e:
                        request_in_flight = False
                        if not _is_no_transaction_error(e):
                            raise
        except TimeoutError as e:
            # See commit() — phase-aware diagnostic so the operator
            # can distinguish lock contention (programmer bug) from a
            # stalled ROLLBACK RTT (network bug).
            if request_in_flight:
                self.force_close_transport()
            phase = "ROLLBACK round-trip" if entered_lock else "op_lock acquire"
            # See commit() for the elapsed-vs-budget rationale.
            _elapsed = time.monotonic() - _start_monotonic
            cause_hint = ""
            if _elapsed < rollback_budget * 0.95:
                cause_hint = (
                    f" (elapsed {_elapsed:.2f}s < budget {rollback_budget}s; "
                    "outer scope or sibling cancel likely interrupted)"
                )
            raise OperationalError(
                f"rollback {phase} timed out after {rollback_budget}s "
                f"(id={id(self)}){cause_hint}; connection state ambiguous; reconnect.",
                code=None,
            ) from e
        except BaseException:
            # Outer-cancel/signal during the ROLLBACK round-trip: force-close. See commit().
            if request_in_flight:
                self.force_close_transport()
            raise

    @contextlib.asynccontextmanager
    async def transaction(self) -> "AsyncIterator[None]":
        """Async context manager wrapping ``BEGIN`` / ``COMMIT`` / ``ROLLBACK``.

        Mirrors asyncpg / psycopg ``transaction()``; delegates to the client-layer
        DqliteConnection.transaction() whose cancellation-aware rollback is the source of
        truth. Raises InterfaceError if the connection is closed.
        """
        del self.messages[:]  # PEP 249 §6.4
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Explicit loop-binding check: the already-connected fast path skips _ensure_locks
        # (which binds for the sibling entry points), so without this a cross-loop caller
        # would get the underlying asyncio.Lock diagnostic, not the dbapi ProgrammingError.
        self._check_loop_binding()
        async_conn = await self._ensure_connection()
        # A foreign-thread force_close_transport may have set _closed between the fast-path
        # return and here; re-check so the owner slot isn't reserved against a dead conn (else
        # siblings see a misleading "Nested" rather than "closed"). Check _closed only —
        # fixtures patch _ensure_connection without updating _async_conn.
        if self._closed:
            raise InterfaceError(f"Connection closed during transaction setup (id={id(self)})")
        # Track the owning task so a stray commit()/rollback() from the same task inside the
        # body raises instead of silently ending the transaction.
        if self._transaction_owner is not None:
            raise InterfaceError(
                f"Nested conn.transaction() not supported (id={id(self)}); "
                "exit the outer block before opening a new one. "
                f"(owner task id={self._transaction_owner})"
            )
        token = asyncio.current_task()
        # Set the owner slot INSIDE the try so a BaseException at the bytecode boundary can't
        # leak the slot pinned to a dying task.
        try:
            self._transaction_owner = token
            async with async_conn.transaction():
                yield
        finally:
            # Only clear if we still own the slot.
            if self._transaction_owner is token:
                self._transaction_owner = None

    def cursor(self, **unknown_kwargs: object) -> AsyncCursor:
        """Return a new AsyncCursor object.

        Intentionally sync — SA calls cursor() from sync greenlet context. Loop binding is
        validated best-effort (no running loop is a valid SA-glue case). Fork-after-init is
        rejected so a forked child can't register a parent-pinned cursor.

        Porting note (aiosqlite): this is sync, so use ``cur = conn.cursor()`` not
        ``await conn.cursor()``.
        """
        del self.messages[:]
        # Affinity precedence: closed -> pid -> loop -> kwarg-shape, so a forked/cross-loop
        # caller with a bogus kwarg sees the affinity diagnostic, not "unknown kwarg".
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if get_current_pid() != self._creator_pid:
            raise InterfaceError(
                f"AsyncConnection used after fork; reconstruct from "
                f"configuration in the target process. "
                f"(created in pid {self._creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        if self._loop_ref is not None:
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running loop — SA greenlet glue calls cursor() from sync context.
                pass
            else:
                bound = self._loop_ref()
                if bound is not None and bound is not current_loop:
                    raise _loop_affinity_exc_class(bound)(
                        _format_loop_affinity_message(bound, current_loop, ".cursor()")
                    )
        if unknown_kwargs:
            raise NotSupportedError(
                f"dqlitedbapi cursor() rejects stdlib sqlite3 kwargs not "
                f"supported by this driver: {sorted(unknown_kwargs)}. "
                f"(stdlib's factory= is not honoured here — Cursor "
                f"subclassing is not supported.)"
            )
        cur = AsyncCursor(self)
        self._cursors.add(cur)
        # Re-check _closed after add: a concurrent close() snapshots list(self._cursors), so a
        # cursor added after the snapshot would skip the cascade and be returned usable on a
        # mid-close conn. Run the scrub and discard it so _cursors matches the clear()
        # postcondition.
        if self._closed:
            cur._closed = True
            cur._rows = []
            cur._description = None
            cur._rowcount = -1
            cur._lastrowid = None
            cur._row_index = 0
            del cur.messages[:]
            # weakref.proxy swap so a race-leaked cursor doesn't strong-pin the closed
            # conn's loop-bound state. suppress(TypeError) for test fakes typed object().
            with contextlib.suppress(TypeError):
                cur._connection = weakref.proxy(cur._connection)
            self._cursors.discard(cur)
        return cur

    @property
    def address(self) -> str:
        """Node address this connection was opened against (read-only)."""
        return self._address

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` ran OR the inner connection was invalidated
        (cancel-mid-execute, leader-flip, transport reset). The OR-in matches psycopg/asyncpg
        so reconnect heuristics work; use :attr:`invalidated` to distinguish the two."""
        return self._closed or self.invalidated

    @property
    def invalidated(self) -> bool:
        """``True`` if the inner connection was invalidated (cancel-mid-execute / leader-flip
        / transport reset) but :meth:`close` was not called. False if explicitly closed,
        never connected, or alive."""
        if self._closed:
            return False
        inner = self._async_conn
        if inner is None:
            return False
        return getattr(inner, "_protocol", "_sentinel") is None

    @property
    def row_factory(self) -> RowFactory | None:
        """stdlib ``row_factory`` parity. New cursors inherit this; override per-cursor."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        # Loop-binding check so a foreign-loop caller can't mutate _row_factory and silently
        # affect cursors on the legitimate loop (also raises InterfaceError on closed).
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        self._check_loop_binding()
        if value is not None and not callable(value):
            raise ProgrammingError(
                f"row_factory must be callable or None, got {type(value).__name__}"
            )
        self._row_factory = value

    @property
    def text_factory(self) -> type[str]:
        """stdlib ``text_factory``-parity stub."""
        return str

    @text_factory.setter
    def text_factory(self, value: object) -> None:
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        self._check_loop_binding()
        if value is str:
            return
        raise NotSupportedError(
            "dqlitedbapi does not support text_factory; TEXT cells are "
            "always returned as str (UTF-8 decoded at the wire layer)"
        )

    async def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
        /,
    ) -> AsyncCursor:
        """Convenience extension (NOT PEP 249 §10) — open a cursor, run ``execute``, return
        it. Mirrors stdlib/aiosqlite ``Connection.execute``.

        Porting note: aiosqlite's chained-CM idiom (``async with conn.execute(sql) as cur``)
        is NOT supported here (plain coroutine); use ``cur = await conn.execute(sql)``. Same
        for executemany.
        """
        del self.messages[:]  # eager-clear: the contract is "next method call", not cursor()
        cur = self.cursor()
        try:
            if parameters is None:
                await cur.execute(operation)
            else:
                await cur.execute(operation, parameters)
        except BaseException:
            # Widened suppress: a KI inside the sync cur.close() could escape suppress(Exception)
            # and replace the original BaseException about to be re-raised.
            with contextlib.suppress(Exception, asyncio.CancelledError):
                cur.close()
            raise
        return cur

    async def executemany(
        self,
        operation: str,
        seq_of_parameters: Iterable[Sequence[Any]],
        /,
    ) -> AsyncCursor:
        """Convenience extension (NOT PEP 249 §10) — open a cursor, run ``executemany``,
        return it. Mirrors stdlib/aiosqlite ``Connection.executemany``. Chained-CM idiom NOT
        supported; see :meth:`execute`.
        """
        del self.messages[:]
        # Route through cursor() so a closed connection raises before the shape check.
        cur = self.cursor()
        # Reject outer shapes that iterate over keys/chars/non-deterministic order; shared
        # helper keeps this and AsyncCursor.executemany on one contract (Mapping not rejected).
        try:
            _validate_executemany_seq_shape(seq_of_parameters)
        except ProgrammingError:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                cur.close()
            raise
        try:
            await cur.executemany(operation, seq_of_parameters)
        except BaseException:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                cur.close()
            raise
        return cur

    # PEP 249 §7 (TPC) / stdlib parity stubs so callers get NotSupportedError, not
    # AttributeError outside dbapi.Error. Plain ``def`` (not async) so a forgotten ``await``
    # raises on the call line instead of a GC-time "coroutine was never awaited" warning.
    def tpc_begin(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_prepare(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_commit(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_rollback(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_recover(self, *args: object, **kwargs: object) -> NoReturn:
        # NoReturn though PEP 249 §7 specs list[Xid]; feature-detect via hasattr + try/except.
        self._stub_unsupported("dqlite does not support two-phase commit")

    def xid(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def _stub_unsupported(self, msg: str) -> NoReturn:
        """Shared NotSupportedError stub: clear messages, check fork then closed, then raise.

        Pid check runs BEFORE closed so a forked child gets InterfaceError (caught by
        is_disconnect) rather than NotSupportedError (a DatabaseError subtype that isn't).
        """
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        creator_pid = getattr(self, "_creator_pid", None)
        if creator_pid is not None and get_current_pid() != creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        with contextlib.suppress(AttributeError):
            if self._closed:
                raise InterfaceError(f"Connection is closed (id={id(self)})")
        raise NotSupportedError(msg)

    def executescript(self, sql_script: str, /) -> NoReturn:
        """stdlib-parity stub: dqlite has no multi-statement-script primitive on the wire.
        Plain ``def`` so the raise fires on the call line, not a GC-time never-awaited warning.
        """
        self._stub_unsupported(
            "dqlite does not support stdlib sqlite3 executescript; "
            "split the script and execute each statement individually"
        )

    def interrupt(self) -> NoReturn:
        self._stub_unsupported(
            "dqlite does not surface interrupt() at the dbapi layer; "
            "use asyncio.timeout(...) or rely on the per-RPC timeout"
        )

    # stdlib parity stubs: VDBE-callback / db-status / db-config / serialize / blob-open
    # primitives are not wire-supportable.

    def set_authorizer(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not expose a per-prepare authorization callback")

    def set_progress_handler(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not expose a VDBE progress callback")

    def set_trace_callback(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not expose a per-statement trace callback")

    def total_changes(self, *args: object, **kwargs: object) -> NoReturn:
        """No total_changes counter on the wire. Exposed as a callable stub (not a property)
        so ``hasattr(aconn, "total_changes")`` stays True for feature-probe code — a
        property-getter would propagate NotSupportedError through hasattr."""
        self._stub_unsupported("dqlite-server does not surface a total_changes counter on the wire")

    def getlimit(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported(
            "dqlite-server does not expose sqlite3_db_status getlimit/setlimit on the wire"
        )

    def setlimit(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported(
            "dqlite-server does not expose sqlite3_db_status getlimit/setlimit on the wire"
        )

    def getconfig(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported(
            "dqlite-server does not expose sqlite3_db_config getconfig/setconfig on the wire"
        )

    def setconfig(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported(
            "dqlite-server does not expose sqlite3_db_config getconfig/setconfig on the wire"
        )

    def serialize(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported(
            "dqlite does not support sqlite3_serialize; conflicts with the distributed Raft model"
        )

    def deserialize(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported(
            "dqlite does not support sqlite3_deserialize; conflicts with the distributed Raft model"
        )

    def blobopen(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not expose sqlite3_blob_open on the wire")

    def enable_load_extension(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not support runtime extension loading")

    def load_extension(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not support runtime extension loading")

    def backup(self, *args: object, **kwargs: object) -> NoReturn:
        # Plain ``def`` so a forgotten ``await`` raises on the call line, not a GC-time warning.
        self._stub_unsupported(
            "dqlite does not support the stdlib sqlite3 online backup API; "
            "use the dqlite-server dump/restore mechanism instead"
        )

    def iterdump(self, *, filter: str | None = None, **kwargs: object) -> NoReturn:
        # Spell ``filter=`` explicitly so inspect.signature matches stdlib-3.13's (*, filter=None).
        del filter  # accepted for signature parity; dqlite has no dump surface
        self._stub_unsupported(
            "dqlite does not support stdlib sqlite3 iterdump; "
            "use the dqlite-server dump/restore mechanism instead"
        )

    def create_function(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not support user-defined SQL functions")

    def create_aggregate(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not support user-defined SQL aggregates")

    def create_collation(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not support user-defined SQL collations")

    def create_window_function(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not support user-defined SQL window functions")

    def __repr__(self) -> str:
        state = "closed" if self._closed else ("connected" if self._async_conn else "unused")
        # Sanitize before !r so attacker-influenced bidi/zero-width codepoints render as ?.
        safe_addr = sanitize_for_log(str(self._address))
        return f"<AsyncConnection address={safe_addr!r} database={self._database!r} {state}>"

    def __reduce__(self) -> NoReturn:
        # Loop-bound socket + asyncio locks don't survive pickling; raise a clear driver-level
        # TypeError instead of leaking the underlying unpickleable-member message.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — async "
            "driver connections own a loop-bound socket and asyncio "
            "primitives tied to a specific event loop; use a "
            "connection pool or recreate the connection in the "
            "consumer process instead"
        )

    async def __aenter__(self) -> Self:
        """Materialise the underlying connection and return self.

        Asymmetric lifecycle: __aexit__ commits/rolls back but does NOT close (matches stdlib
        sqlite3, diverges from aiosqlite/psycopg). Add an explicit ``await aconn.close()`` or
        hand ownership to a pool.
        """
        try:
            await self.connect()
        except BaseException:
            # __aexit__ isn't called when __aenter__ raises, so partial state (loop-bound
            # locks) would leak and a retry on another loop would hit a misleading "different
            # loop" error. close() is idempotent. shield lets the inner close finish its drain
            # when the connect-failure cancel is the same chain; suppress(CancelledError)
            # absorbs a FRESH outer cancel so the bare raise re-delivers the ORIGINAL error.
            # Explicit observer so the shield's implicit Task isn't orphaned.
            from dqliteclient.cluster import _observe_drain_exception

            inner_drain = asyncio.ensure_future(self.close())
            inner_drain.add_done_callback(_observe_drain_exception)
            try:
                with contextlib.suppress(asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    # Absorb KI/SE too: one delivered inside the shielded close would
                    # supplant the saved original connect-time exception.
                    await asyncio.shield(inner_drain)
            except Exception:
                logger.debug(
                    "AsyncConnection.__aenter__ (id=%s, address=%s): "
                    "exception during cleanup-close after failed connect",
                    id(self),
                    sanitize_for_log(str(self._address)),
                    exc_info=True,
                )
            # Defensive null-out: if a concurrent close flipped _closed, the cleanup close()
            # short-circuits at its top guard and skips the never-connected branch that nulls
            # these slots — leaving a reuse on another loop to hit a misleading "different
            # loop" error. Idempotent with close()'s own nulling.
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Commit on clean exit, rollback on exception, do NOT close (stdlib sqlite3 parity;
        diverges from aiosqlite/psycopg). Cancel during commit/rollback leaves server-side
        state possibly ambiguous but still propagates faithfully (with a DEBUG breadcrumb).
        """
        if self._closed or self._async_conn is None:
            # Never ran, OR a foreign thread closed mid-block via force_close_transport. The
            # _closed arm avoids the commit() closed-state guard supplanting the body outcome.
            return
        if exc_type is None:
            try:
                await self.commit()
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                # COMMIT may have reached the leader before the cancel: state ambiguous. Log a
                # breadcrumb, then force-close so the slot is reaped (SA's is_disconnect
                # doesn't match cancel/signal), then re-raise so the cancel propagates.
                logger.debug(
                    "AsyncConnection.__aexit__ (address=%s, id=%s): "
                    "commit interrupted by cancel/signal; "
                    "server-side commit state may be ambiguous",
                    sanitize_for_log(str(self._address)),
                    id(self),
                    exc_info=True,
                )
                self.force_close_transport()
                raise
        else:
            try:
                await self.rollback()
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                # Policy: structured-concurrency cancel-wins — re-raise the cancel even after a
                # body exception. PEP 343 leaves the body exception only on __context__, but
                # SA's is_disconnect walks __cause__ only, so the slot is NOT invalidated;
                # operators must consult __context__ or wrap in asyncio.timeout. Pin:
                # test_aexit_rollback_debug_log::test_aexit_rollback_cancelled_error_propagates.
                logger.debug(
                    "AsyncConnection.__aexit__ (address=%s, id=%s): "
                    "rollback interrupted by cancel/signal after body "
                    "raised %s; cancel re-raised per structured-"
                    "concurrency policy — body exception survives on "
                    "__context__ only, not __cause__",
                    sanitize_for_log(str(self._address)),
                    id(self),
                    exc_type.__name__,
                    exc_info=True,
                )
                raise
            except Exception:
                # Body already raised so we can't re-raise; log a breadcrumb. The transaction
                # may still be open and SA classifies against the body (not the rollback
                # failure), so force-close to reap the slot before the next checkout.
                logger.debug(
                    "AsyncConnection.__aexit__ (address=%s, id=%s): "
                    "rollback failed after body raised %s",
                    sanitize_for_log(str(self._address)),
                    id(self),
                    exc_type.__name__,
                    exc_info=True,
                )
                self.force_close_transport()
        # Do NOT close — stdlib sqlite3 parity (aiosqlite/psycopg close on exit). Callers who
        # want eager close call conn.close() or go through a pool.


# Expose AsyncCursor (and the aiosqlite-convention ``Cursor`` alias) for cross-driver
# isinstance checks. Assigned outside the class body to avoid shadowing the type name in
# method annotations. Not a factory hook — cursor() still instantiates AsyncCursor directly.
AsyncConnection.AsyncCursor = AsyncCursor  # type: ignore[attr-defined]
AsyncConnection.Cursor = AsyncCursor  # type: ignore[attr-defined]
