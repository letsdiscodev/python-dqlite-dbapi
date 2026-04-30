"""Async connection implementation for dqlite."""

import asyncio
import contextlib
import logging
import os
import warnings
import weakref
from types import TracebackType
from typing import NoReturn

from dqliteclient import DqliteConnection
from dqliteclient import connection as _client_conn_mod
from dqliteclient.connection import parse_address as _client_parse_address
from dqlitedbapi import exceptions as _exc
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import (
    _build_and_connect,
    _is_no_transaction_error,
    _validate_close_timeout,
    _validate_timeout,
    _wrap_positive_int,
)
from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import (
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from dqlitewire import (
    DEFAULT_MAX_CONTINUATION_FRAMES as _DEFAULT_MAX_CONTINUATION_FRAMES,
)
from dqlitewire import (
    DEFAULT_MAX_TOTAL_ROWS as _DEFAULT_MAX_TOTAL_ROWS,
)

__all__ = ["AsyncConnection"]

logger = logging.getLogger(__name__)


def _async_unclosed_warning(
    closed_flag: list[bool], connected_flag: list[bool], address: str
) -> None:
    """Emit a ResourceWarning when an ``AsyncConnection`` is GC'd
    without ``await close()``.

    Stdlib ``sqlite3`` raises ``ResourceWarning`` on
    ``Connection.__del__`` when the user forgot to close; the sync
    sibling matches that contract via ``_cleanup_loop_thread``. The
    async sibling has no daemon loop to reap (the user owns the
    loop), but the warning is still load-bearing for ops:
    operators forgetting ``await aconn.close()`` see only asyncio's
    own "Unclosed transport" warnings, which point at the StreamReader/
    StreamWriter rather than the dqlite layer they came from.

    Three-flag gate:

    - ``closed_flag[0]`` is True if ``close()`` ran or if the
      synchronous ``force_close_transport`` (terminate / SA outside-
      greenlet) ran.
    - ``connected_flag[0]`` is True only after ``_ensure_connection``
      successfully built the underlying ``DqliteConnection``. A
      never-connected instance has nothing to clean up — the warning
      would be a false positive.

    Without the connected-flag gate, common patterns like
    ``conn = AsyncConnection(...); del conn`` (early-error or
    test-fixture flow) would emit a misleading warning. Without
    ``force_close_transport`` setting ``closed_flag``, the SA
    ``terminate()`` path triggers the warning even though the
    transport was reaped.

    Suppression-narrow ``RuntimeError`` mirrors the sync sibling's
    interpreter-shutdown race protection.
    """
    if closed_flag[0] or not connected_flag[0]:
        return
    with contextlib.suppress(RuntimeError):
        warnings.warn(
            f"AsyncConnection(address={address!r}) was garbage-collected "
            f"without await close(). Call ``await aconn.close()`` "
            f"explicitly to avoid this warning and to release the "
            f"underlying socket promptly.",
            ResourceWarning,
            stacklevel=2,
        )


def _format_loop_affinity_message(
    bound: asyncio.AbstractEventLoop | None,
    current: asyncio.AbstractEventLoop | None,
    site: str,
) -> str:
    """Build the loop-affinity ProgrammingError message with both
    loop identities.

    Mirrors the sync ``Connection`` thread-affinity message which
    already names both thread ids — operators reading "different
    event loop" without identifiers cannot tell whether the bound
    loop has been garbage-collected (recovery: replace the
    connection) or whether two concurrent loops are running
    (recovery: route the call to the right loop).
    """
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

    # PEP 249 optional extension ("Attributes from Module Exceptions"):
    # parity with the sync ``Connection`` class so cross-driver code can
    # write ``except aconn.Error:`` without importing the driver module.
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
            timeout: Per-RPC-phase timeout in seconds (positive,
                finite). Each phase of an operation (send, read, any
                continuation drain) gets the full budget independently
                — a single call can take up to roughly N × ``timeout``
                end-to-end. Wrap callers in ``asyncio.timeout(...)``
                to enforce a wall-clock deadline.
            max_total_rows: Cumulative row cap across continuation
                frames. Forwarded to the underlying DqliteConnection;
                ``None`` disables the cap.
            max_continuation_frames: Per-query continuation-frame cap.
                Forwarded to the underlying DqliteConnection.
            trust_server_heartbeat: When True, let the server-advertised
                heartbeat widen the per-read deadline.
            close_timeout: Budget (seconds) for the transport-drain
                during ``close()``. Forwarded to the underlying
                DqliteConnection. Default 0.5 s is sized for LAN.
        """
        _validate_timeout(timeout)
        _validate_close_timeout(close_timeout)
        # Eager address parse, matching the sync Connection and the
        # underlying DqliteConnection. A typoed DSN surfaces at
        # construction, not at first-use.
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
        self._max_total_rows = _wrap_positive_int(max_total_rows, "max_total_rows")
        self._max_continuation_frames = _wrap_positive_int(
            max_continuation_frames, "max_continuation_frames"
        )
        self._trust_server_heartbeat = trust_server_heartbeat
        self._close_timeout = close_timeout
        self._async_conn: DqliteConnection | None = None
        self._closed = False
        # Fork-after-init is unsupported: the inherited TCP socket
        # is shared with the parent and writer.close() would FIN
        # the parent's connection, and asyncio primitives are bound
        # to the parent's loop. Symmetric with the sync sibling and
        # the client-layer guards.
        self._creator_pid = os.getpid()
        # asyncio primitives MUST be created inside the loop they will
        # run on. We instantiate lazily in _ensure_connection / the
        # op-serializing paths so constructors can safely run outside
        # a running loop (SQLAlchemy creates AsyncConnection in sync
        # glue code before any loop exists).
        self._connect_lock: asyncio.Lock | None = None
        self._op_lock: asyncio.Lock | None = None
        # Weak reference to the loop the locks were first bound to.
        # Captured at first ``_ensure_locks()`` so subsequent use from a
        # different event loop raises a clean ProgrammingError instead
        # of asyncio's internal "got Future attached to a different
        # loop" RuntimeError. Weakref avoids pinning a closed loop
        # alive once the caller has moved on.
        self._loop_ref: weakref.ref[asyncio.AbstractEventLoop] | None = None
        # PEP 249 optional extension; see Connection.messages.
        self.messages: list[tuple[type[Exception], Exception | str]] = []
        # Track outstanding cursors weakly so close() can scrub their
        # state (stdlib sqlite3 cascades). Buffered fetches on a
        # cursor whose AsyncConnection was externally closed used to
        # silently answer from stale in-memory rows.
        self._cursors: weakref.WeakSet[AsyncCursor] = weakref.WeakSet()
        # Mutable 1-element flag the finalizer reads. ``close()`` sets
        # it to True so the finalizer knows the user closed
        # explicitly and skips the ResourceWarning. We do NOT attempt
        # async cleanup from the finalizer — the user owns the event
        # loop, and emitting a warning is the only safe synchronous
        # signal available. Symmetric with the sync ``Connection``
        # finalizer's responsibility split (which does drive an
        # owned daemon thread; the async sibling has none).
        self._closed_flag: list[bool] = [False]
        # Companion flag flipped True only after ``_ensure_connection``
        # successfully builds the underlying ``DqliteConnection``. The
        # finalizer skips the ResourceWarning when False so a
        # never-connected instance (e.g. ``conn = AsyncConnection(...);
        # del conn``, common in early-error and test-fixture flows)
        # doesn't emit a misleading "GC'd without close" warning.
        self._connected_flag: list[bool] = [False]
        weakref.finalize(
            self,
            _async_unclosed_warning,
            self._closed_flag,
            self._connected_flag,
            address,
        )

    def _ensure_locks(self) -> tuple[asyncio.Lock, asyncio.Lock]:
        """Lazy-create the asyncio locks on the currently-running loop.

        Also pins the connection to that loop: subsequent calls from a
        different loop raise ``ProgrammingError`` up front. The
        underlying ``DqliteConnection`` protocol's StreamReader/Writer
        is also loop-bound, so transparently rebinding is not safe;
        fail fast with a clear message instead.
        """
        # A concurrent ``close()`` may have just nulled the lock
        # references; lazily recreating them here would bind fresh
        # primitives to the current loop on a connection that is
        # conceptually dead. The caller's next step is
        # ``_ensure_connection`` which would raise the same
        # ``InterfaceError`` anyway — but the fresh locks / loop_ref
        # survive the raise and a second ``close()`` early-returns
        # without re-nulling them, leaking three asyncio primitives
        # per race. Fail fast here so no primitives are created.
        if self._closed:
            raise InterfaceError("Connection is closed")
        # Fork-after-init: asyncio primitives are bound to the parent
        # loop and the inherited socket is shared. Reject up front
        # with the same diagnostic the sync sibling and the client
        # layer use, so a forked worker sees a clear "reconstruct in
        # the child" message instead of a confusing
        # "Connection bound to a different event loop" error.
        if _client_conn_mod._current_pid != self._creator_pid:
            raise InterfaceError(
                "Connection used after fork; reconstruct from configuration in the target process."
            )
        loop = asyncio.get_running_loop()
        if self._connect_lock is None:
            self._loop_ref = weakref.ref(loop)
            self._connect_lock = asyncio.Lock()
            self._op_lock = asyncio.Lock()
        else:
            bound = self._loop_ref() if self._loop_ref is not None else None
            if bound is not loop:
                raise ProgrammingError(_format_loop_affinity_message(bound, loop, "was first used"))
        # ``_op_lock`` is created together with ``_connect_lock`` above;
        # the assertion keeps mypy narrow without a runtime cost.
        assert self._op_lock is not None
        return self._connect_lock, self._op_lock

    def _check_loop_binding(self) -> None:
        """Validate the current loop matches the bound loop WITHOUT
        binding on first use. Sibling of ``_ensure_locks``; intended
        for cursor methods that don't need the locks (no-op /
        always-raise paths like ``callproc``, ``nextset``,
        ``scroll``, ``setinputsizes``, ``setoutputsize``) but should
        still fail fast on cross-loop misuse. Without this split,
        calling one of those methods on a fresh connection lazily
        binds the loop to the calling task's loop, and a later
        legitimate call from a different loop fails with a
        ProgrammingError pointing at a loop the user did not
        knowingly bind.

        Raises ProgrammingError if the connection has already bound
        to a different loop. No-op when not yet bound.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")
        if _client_conn_mod._current_pid != self._creator_pid:
            raise InterfaceError(
                "Connection used after fork; reconstruct from configuration in the target process."
            )
        if self._loop_ref is None:
            return  # not yet bound — don't bind from here
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Outside an async context; the cursor method will raise
            # NotSupportedError or no-op anyway. Don't manufacture a
            # different error.
            return
        bound = self._loop_ref()
        if bound is not loop:
            raise ProgrammingError(_format_loop_affinity_message(bound, loop, "was first used"))

    async def _ensure_connection(self) -> DqliteConnection:
        """Ensure the underlying connection is established."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            return self._async_conn

        connect_lock, _ = self._ensure_locks()
        async with connect_lock:
            # Double-check after acquiring lock
            if self._async_conn is not None:
                return self._async_conn

            built = await _build_and_connect(
                self._address,
                database=self._database,
                timeout=self._timeout,
                max_total_rows=self._max_total_rows,
                max_continuation_frames=self._max_continuation_frames,
                trust_server_heartbeat=self._trust_server_heartbeat,
                close_timeout=self._close_timeout,
            )
            # A concurrent close() may have flipped _closed while we were
            # suspended in _build_and_connect. close() observes
            # _async_conn is None at that point and early-returns, so if
            # we published ``built`` now the caller would hold a live
            # socket that nobody will close. Close the fresh connection
            # and signal the caller instead.
            if self._closed:
                with contextlib.suppress(Exception):
                    await built.close()
                raise InterfaceError("Connection is closed")
            self._async_conn = built
            # Flip the finalizer's "anything to clean up" gate. From
            # this point GC without close emits the ResourceWarning;
            # before this point a never-connected instance is silent.
            self._connected_flag[0] = True

        return self._async_conn

    async def connect(self) -> None:
        """Establish the connection."""
        await self._ensure_connection()

    async def close(self) -> None:
        """Close the connection.

        Serializes with any in-flight operation via ``_op_lock`` so we
        never tear down the underlying protocol while another task is
        mid-execute/mid-commit — that races would leave the caller
        with mysterious "connection closed" errors mid-query.
        """
        if self._closed:
            return
        # PEP 249 §6.1.1: Connection.messages should be cleared on
        # any standard Connection method invocation. Sync and async
        # commit/rollback/cursor paths already clear; align close()
        # so the contract is uniform across the four required
        # methods.
        del self.messages[:]
        # Fork-after-init: the inherited socket FD is shared with the
        # parent and the asyncio op_lock is bound to the parent's
        # loop. Driving the async teardown here would either send FIN
        # on the parent's connection (writer.close on the inherited
        # FD) or hang on a parent-loop primitive. Flip the local
        # state to closed and drop references quietly. Symmetric with
        # the sync ``Connection.close`` and ``DqliteConnection.close``
        # fork short-circuits.
        if _client_conn_mod._current_pid != self._creator_pid:
            self._closed = True
            self._closed_flag[0] = True
            self._async_conn = None
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            self._cursors.clear()
            return
        # Set _closed first so any task waiting on the lock sees the
        # closed state as soon as it acquires. Then drain the current
        # in-flight op (if any) under the lock.
        self._closed = True
        self._closed_flag[0] = True
        # Cascade to tracked cursors before the teardown drains the
        # wire so buffered fetches stop answering from stale rows.
        # Writes go directly to the cursor's private attributes —
        # ``AsyncCursor.close`` is ``async def`` so a ``cur.close()``
        # call from this synchronous loop would produce an un-awaited
        # coroutine. The direct-write path keeps the cascade
        # trivially synchronous and also defends against a future
        # change adding a lock acquire to ``AsyncCursor.close`` that
        # would otherwise deadlock against the ``_op_lock`` acquire
        # below.
        # Wrap in try/finally so a KI/SystemExit landing mid-loop does
        # not leave ``self._cursors`` populated with stale references.
        # Per-cursor ``_closed = True`` is the LOAD-BEARING write; even
        # if a later field-write is skipped on signal-arrival, the
        # cursor's own ``_check_closed()`` gates all reads — so a
        # cursor with ``_closed=True`` but stale ``_rows`` will reject
        # fetch attempts cleanly.
        try:
            for cur in list(self._cursors):
                cur._closed = True
                cur._rows = []
                cur._description = None
                cur._rowcount = -1
                cur._lastrowid = None
                # Mirror AsyncCursor.close()'s consistent "no operation
                # performed" surface — the row index must be reset
                # alongside the buffer.
                cur._row_index = 0
                # PEP 249 §6.4: clear messages on the cascade so a
                # post-cascade ``cur.messages`` access doesn't see
                # stale entries.
                del cur.messages[:]
        finally:
            self._cursors.clear()
        if self._async_conn is None:
            # Null the lazy locks so a subsequent fixture or
            # SQLAlchemy-glue reuse of the object in a different event
            # loop cannot observe a primitive bound to the dead loop.
            # Parity with the sync close() reset established for
            # connect_lock.
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            return
        # Use the already-bound op_lock directly; calling
        # ``_ensure_locks`` now raises because ``_closed`` is True.
        # If _async_conn is set, the locks were created on a prior
        # ``_ensure_locks`` call — otherwise the short-circuit above
        # at ``_async_conn is None`` returned.
        assert self._op_lock is not None
        op_lock = self._op_lock
        try:
            async with op_lock:
                if self._async_conn is not None:
                    await self._async_conn.close()
                    self._async_conn = None
        finally:
            # If a CancelledError lands during ``async with op_lock``'s
            # acquire — between the cursor cascade above and entering
            # the lock body — the underlying ``_async_conn.close()``
            # never runs, but ``_closed=True`` (set above) makes a
            # retry close() return immediately. The socket leaks
            # until process exit.
            #
            # Best-effort underlying close in the finally: shielded
            # against the cancel re-delivery, idempotent against the
            # successful path (the underlying close has its own
            # short-circuits via ``_pool_released`` / ``_in_use``).
            # Then the lock-cleanup runs unconditionally.
            if self._async_conn is not None:
                try:
                    await asyncio.shield(self._async_conn.close())
                except InterfaceError as exc:
                    # The underlying connection is still in_use by a
                    # sibling task (op_lock contract violation: cross-task
                    # close on a connection mid-operation). The sibling's
                    # ``_run_protocol`` finally only resets ``_in_use``;
                    # nothing in that path closes the underlying socket.
                    # Force-close the writer synchronously so the
                    # transport is reaped instead of leaked. The sibling
                    # task's pending read will see EOF and surface a
                    # transport error, then ``_run_protocol``'s
                    # CancelledError / DqliteConnectionError handler
                    # invalidates the conn. The user's contract violation
                    # surfaces as a transport failure on the in-flight
                    # operation — the lesser of two evils vs. a silent
                    # transport leak.
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
                    # Fall through (no ``return``) to the unconditional
                    # ``self._async_conn = None`` and lock-cleanup tail
                    # below. ``return`` inside a ``finally`` block
                    # silently discards any propagating CancelledError /
                    # KeyboardInterrupt / SystemExit from the outer
                    # ``try``, which breaks TaskGroup parents'
                    # observation of a child cancellation that landed
                    # during close.
                except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    # The shielded close should have absorbed cancel;
                    # if a fresh signal lands here, allow it to
                    # propagate after best-effort cleanup state.
                    self._async_conn = None
                    self._connect_lock = None
                    self._op_lock = None
                    self._loop_ref = None
                    raise
                except Exception:
                    logger.debug(
                        "AsyncConnection.close (id=%s): underlying close failed",
                        id(self),
                        exc_info=True,
                    )
                self._async_conn = None
            # Reset the locks *after* closing so any task that was
            # parked on ``op_lock`` observes the
            # "_closed -> raise InterfaceError" re-check before it
            # touches the now-None primitive.
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None

    def force_close_transport(self) -> None:
        """Synchronously tear down the underlying socket transport.

        Last-resort cleanup for finalize paths running outside any
        event loop (GC sweep with no greenlet, atexit handler with the
        loop already torn down). Walks the inner client-layer
        connection's protocol writer and calls
        ``writer.close()`` directly — the writer's ``close()`` is
        synchronous and safe to invoke without a running loop.

        Idempotent. Never raises. A missing inner connection / missing
        protocol / missing writer is silently absorbed (the connection
        was never opened, or the regular async ``close()`` already ran
        and nulled the references).

        Used by SQLAlchemy's async adapter when SA's finalize path
        executes outside a greenlet context — without this hook the
        adapter would have to walk private attributes of two
        underlying packages, which broke silently when the chain
        changed shape.

        Concurrent-safety: this method may be invoked while an async
        ``close()`` is in flight on the same connection (different
        greenlet, different thread, or finalize racing user code).
        Both will end up calling ``writer.close()``, which is itself
        idempotent — calling it twice is harmless. The hook does NOT
        wait for the in-flight async close to finish; if a rollback
        is mid-flight, it will be cancelled by the transport teardown.
        Callers that need to wait for the async path to drain should
        await ``close()`` from the original loop instead.
        """
        # Set the finalizer's closed_flag so a subsequent GC sweep
        # does not emit a misleading "GC'd without close()" warning
        # — the user (or SA's terminate()) explicitly cleaned up via
        # the synchronous force-close path. The flag is set
        # unconditionally (even on the no-op inner=None / fork-child
        # branches) so any path through this method counts as
        # explicit cleanup.
        self._closed_flag[0] = True
        # Also flip the public-facing closed flag so ``aconn.closed``
        # reflects reality and a subsequent ``await aconn.close()``
        # short-circuits via the closed-early-return at the top of
        # close(). Without this, terminate() reaped the writer but
        # left ``self._closed = False``; ``aconn.cursor()`` succeeded
        # against a dead transport and a follow-up close() drove the
        # full async teardown again on already-closed primitives.
        self._closed = True
        inner = self._async_conn
        if inner is None:
            self._async_conn = None
            return
        # Fork-after-init: ``writer.close()`` on the inherited socket
        # FD would send FIN on a connection the parent still holds
        # open. Quietly drop the local reference instead so child GC
        # has nothing left to act on. Symmetric with the
        # ``DqliteConnection.close`` and ``Connection.close`` fork
        # short-circuits; this synchronous force-close path was the
        # last gap left by cycle 20's async-only fork guards.
        if _client_conn_mod._current_pid != self._creator_pid:
            self._async_conn = None
            return
        proto = getattr(inner, "_protocol", None)
        if proto is None:
            return
        writer = getattr(proto, "_writer", None)
        if writer is None:
            return
        try:
            writer.close()
        except Exception:  # noqa: BLE001 - last-resort cleanup
            logger.debug(
                "AsyncConnection.force_close_transport (id=%s): writer.close() raised; ignoring",
                id(self),
                exc_info=True,
            )

    @property
    def in_transaction(self) -> bool:
        """Whether the connection currently has an open transaction.

        Mirrors stdlib ``sqlite3.Connection.in_transaction`` (and its
        sync sibling :attr:`dqlitedbapi.Connection.in_transaction`).
        Never-connected or closed connections return False.
        """
        # Snapshot the reference once: ``close()`` running concurrently
        # may null ``_async_conn`` between a None-check and an attribute
        # read. The local binding is immutable for the duration of the
        # property body, eliminating that window. ``bool(...)`` keeps the
        # mock-adapter safety from the stdlib-parity introduction.
        conn = self._async_conn
        if conn is None or self._closed:
            return False
        return bool(conn.in_transaction)

    @property
    def autocommit(self) -> bool:
        """``True`` — dqlite operates in autocommit-by-default mode.

        Mirrors the surface stdlib ``sqlite3`` added in Python 3.12 and
        the long-standing ``psycopg.Connection.autocommit`` accessor.
        Every statement commits at the server unless the caller issued
        an explicit ``BEGIN``.

        The bare async dbapi exposes ``True`` because the underlying
        wire protocol is genuinely autocommit-by-default. The
        SQLAlchemy adapter (``sqlalchemy-dqlite``) deliberately exposes
        ``False`` because SA wraps the connection with explicit
        BEGIN/COMMIT control — both are accurate for their respective
        layer.

        Setting to ``True`` is a no-op; setting to ``False`` raises
        ``NotSupportedError``.
        """
        return True

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        if value is True:
            return
        raise NotSupportedError(
            "dqlite operates in autocommit-by-default mode; the autocommit "
            "flag cannot be turned off at the dbapi level. Wrap your "
            "statements in explicit BEGIN/COMMIT (issued via cursor.execute) "
            "to control transaction boundaries instead."
        )

    async def commit(self) -> None:
        """Commit any pending transaction.

        Silent no-op if the connection has never been used (preserves
        the existing "no spurious connect" contract) or if the server
        reports "no transaction is active" (matches stdlib sqlite3).
        The latter is the *common* case on this driver — every
        statement auto-commits at the server unless an explicit
        ``BEGIN`` was issued (see class docstring / README
        "Transactions").

        Operational caveat: on a leader flip mid-transaction, COMMIT
        can raise ``OperationalError`` with a leader-change code. The
        write may or may not have been persisted — callers should use
        idempotent DML or out-of-band state-checks before retrying.
        Same caveat applies to ``__aexit__``'s clean-exit commit.
        """
        # PEP 249 §6.1.1: ``Connection.messages`` is cleared "prior to
        # executing the call" on every standard connection method —
        # including the closed-path raise below. Sync sibling clears
        # unconditionally as the very first statement; mirror that here
        # so the contract holds regardless of which branch we take.
        del self.messages[:]
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._async_conn is None:
            return
        _, op_lock = self._ensure_locks()
        async with op_lock:
            # Re-check under the lock: a concurrent close() may have
            # acquired op_lock before us, closed the connection, and
            # released. Without this second check we would dereference
            # ``self._async_conn.execute`` on ``None``.
            if self._closed or self._async_conn is None:
                raise InterfaceError("Connection is closed")
            # Clear ``messages`` under the lock so the PEP 249
            # contract "messages cleared by every method call" is
            # atomic with the operation. Clearing only pre-lock leaves
            # a window where a sibling task could append between
            # this clear and the COMMIT.
            del self.messages[:]
            # Read ``in_transaction`` under the lock so the value is
            # fresh against any sibling task that may have just
            # committed/rolled back. Reading outside the lock left a
            # window where a stale ``True`` would route us into a
            # redundant COMMIT round-trip whose "no transaction" error
            # is silenced below — correct, but a wasted RTT and a
            # structural race. ``in_transaction`` already ORs in the
            # untracked-savepoint flag at the client layer.
            if not getattr(self._async_conn, "in_transaction", False):
                return
            try:
                # Parity with ``Connection._commit_async``; ``_call_client``
                # maps raw client errors onto PEP 249 ``Error`` subclasses.
                await _call_client(self._async_conn.execute("COMMIT"))
            except OperationalError as e:
                if not _is_no_transaction_error(e):
                    raise

    async def rollback(self) -> None:
        """Roll back any pending transaction.

        Same no-op rules as :meth:`commit`, including the autocommit-
        by-default contract: "no active transaction" is the common
        case unless the caller issued an explicit ``BEGIN``.
        """
        # PEP 249 §6.1.1 messages-clear contract; see commit() above.
        del self.messages[:]
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._async_conn is None:
            return
        _, op_lock = self._ensure_locks()
        async with op_lock:
            # Re-check under the lock for the same race as commit().
            if self._closed or self._async_conn is None:
                raise InterfaceError("Connection is closed")
            # Clear ``messages`` under the lock; see ``commit`` rationale.
            del self.messages[:]
            # Read ``in_transaction`` under the lock; see commit() for
            # the rationale (avoid stale-True wasted ROLLBACK-RTT).
            if not getattr(self._async_conn, "in_transaction", False):
                return
            try:
                # Parity with ``Connection._rollback_async``; see ``commit``.
                await _call_client(self._async_conn.execute("ROLLBACK"))
            except OperationalError as e:
                if not _is_no_transaction_error(e):
                    raise

    def cursor(self) -> AsyncCursor:
        """Return a new AsyncCursor object.

        This is intentionally sync — SQLAlchemy calls cursor() from
        sync context within its greenlet-based async adapter. Loop
        binding is validated best-effort: if a different loop is
        running than the one this connection was first used on, raise
        ``ProgrammingError`` up front rather than letting the first
        await inside ``_ensure_locks`` surface the same error with a
        less specific diagnostic. No running loop (SA greenlet glue)
        is a valid case — skip the check.
        """
        del self.messages[:]
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._loop_ref is not None:
            try:
                current_loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running loop — SA greenlet glue calls cursor()
                # from sync context. Skip the check.
                pass
            else:
                bound = self._loop_ref()
                if bound is not None and bound is not current_loop:
                    raise ProgrammingError(
                        _format_loop_affinity_message(bound, current_loop, ".cursor()")
                    )
        cur = AsyncCursor(self)
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

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` has been called.

        Peer-driver parity (psycopg, asyncpg). PEP 249 does not
        require it; the underlying flag is already maintained.
        """
        return self._closed

    # PEP 249 §7 (TPC) and stdlib sqlite3 parity stubs. Without these
    # a caller hits AttributeError which escapes the dbapi.Error
    # hierarchy. Same shape as the sync sibling.

    async def tpc_begin(self, xid: object) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    async def tpc_prepare(self) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    async def tpc_commit(self, xid: object | None = None) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    async def tpc_rollback(self, xid: object | None = None) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    async def tpc_recover(self) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    def xid(self, format_id: int, global_transaction_id: str, branch_qualifier: str) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    def enable_load_extension(self, enabled: bool) -> NoReturn:
        raise NotSupportedError("dqlite-server does not support runtime extension loading")

    def load_extension(self, path: str, *, entrypoint: str | None = None) -> NoReturn:
        raise NotSupportedError("dqlite-server does not support runtime extension loading")

    async def backup(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError(
            "dqlite does not support the stdlib sqlite3 online backup API; "
            "use the dqlite-server dump/restore mechanism instead"
        )

    def iterdump(self) -> NoReturn:
        raise NotSupportedError(
            "dqlite does not support stdlib sqlite3 iterdump; "
            "use the dqlite-server dump/restore mechanism instead"
        )

    def create_function(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError("dqlite-server does not support user-defined SQL functions")

    def create_aggregate(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError("dqlite-server does not support user-defined SQL aggregates")

    def create_collation(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError("dqlite-server does not support user-defined SQL collations")

    def create_window_function(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError("dqlite-server does not support user-defined SQL window functions")

    def __repr__(self) -> str:
        state = "closed" if self._closed else ("connected" if self._async_conn else "unused")
        return f"<AsyncConnection address={self._address!r} database={self._database!r} {state}>"

    def __reduce__(self) -> NoReturn:
        # AsyncConnections own a loop-bound socket and asyncio Locks
        # tied to a specific event loop — neither survives pickling.
        # Surface a clear driver-level
        # TypeError instead of leaking the underlying unpickleable-
        # member message.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — async "
            "driver connections own a loop-bound socket and asyncio "
            "primitives tied to a specific event loop; use a "
            "connection pool or recreate the connection in the "
            "consumer process instead"
        )

    async def __aenter__(self) -> "AsyncConnection":
        try:
            await self.connect()
        except BaseException:
            # Python does not call ``__aexit__`` when ``__aenter__``
            # raises, so partial state (lazily-constructed locks
            # bound to the current loop, loop-ref) would leak — a
            # subsequent retry on a different event loop would then
            # hit "bound to a different event loop" instead of the
            # real connect error. ``close()`` is idempotent and
            # handles the never-connected case by resetting the
            # lock primitives.
            with contextlib.suppress(Exception):
                await self.close()
            raise
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if self._async_conn is None:
            # Nothing ever ran; keep the connection reusable, matching
            # stdlib sqlite3 / aiosqlite / psycopg semantics.
            return
        if exc_type is None:
            await self.commit()
        else:
            try:
                await self.rollback()
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                # Cancel / signal interrupted the rollback. Mirror the
                # client-layer ``transaction()`` ctxmgr's discipline:
                # log a breadcrumb so the audit trail is consistent
                # across layers, then re-raise so the cancel signal
                # supersedes the body exception.
                logger.debug(
                    "AsyncConnection.__aexit__ (address=%s, id=%s): "
                    "rollback interrupted by cancel/signal after body "
                    "raised %s",
                    self._address,
                    id(self),
                    exc_type.__name__,
                    exc_info=True,
                )
                raise
            except Exception:
                # The body already raised; we cannot re-raise, but a
                # silent suppress leaves no breadcrumb for an operator
                # debugging a dangling server-side transaction
                # (leader flip mid-commit, socket timeout, etc.).
                logger.debug(
                    "AsyncConnection.__aexit__ (address=%s, id=%s): "
                    "rollback failed after body raised %s",
                    self._address,
                    id(self),
                    exc_type.__name__,
                    exc_info=True,
                )
        # Do NOT close — matches stdlib sqlite3 / aiosqlite / psycopg.
        # Callers who want eager close use ``conn.close()`` explicitly
        # or go through a pool.
