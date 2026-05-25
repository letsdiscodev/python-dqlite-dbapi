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
    LEADER_ERROR_CODES as _LEADER_ERROR_CODES,
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

    Four-flag gate:

    - ``get_current_pid() != creator_pid``: forked child. The
      ``(closed_flag, connected_flag)`` snapshots belong to the
      parent process at fork-time; emitting a ResourceWarning here
      would falsely accuse the child of leaking what the parent owns.
      Mirrors the sync sibling's ``_cleanup_loop_thread`` fork-safety
      discipline and every other fork-traversing site in the package.
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
    if get_current_pid() != creator_pid:
        # Forked child. Skip — the parent owns the lifecycle.
        return
    if closed_flag[0] or not connected_flag[0]:
        return
    with contextlib.suppress(RuntimeError):
        # Defence-in-depth parity with the sync sibling
        # ``_cleanup_loop_thread`` (connection.py:1140-1157):
        # route ``address`` through ``sanitize_for_log`` before
        # the repr-escape. Python's ``repr()`` already escapes
        # control codepoints into ``\xNN`` sequences (CWE-117
        # mitigated by repr alone), but the sanitiser is the
        # package's documented belt-and-suspenders posture and
        # the sync sibling applies it; this restores parity.
        warnings.warn(
            f"AsyncConnection(address={sanitize_for_log(str(address))!r}) was "
            f"garbage-collected without await close(). Call "
            f"``await aconn.close()`` explicitly to avoid this warning "
            f"and to release the underlying socket promptly.",
            ResourceWarning,
            stacklevel=2,
        )


def _loop_affinity_exc_class(
    bound: asyncio.AbstractEventLoop | None,
) -> type[Exception]:
    """Return the right PEP 249 §3 exception class for a loop-affinity
    diagnostic.

    A closed / GC'd loop is "the interface is gone, reconstruct the
    connection" — ``InterfaceError`` in PEP 249's hierarchy, matching
    the client-layer sibling at
    ``dqliteclient.connection._check_in_use`` which already uses
    ``InterfaceError`` for the closed-loop and different-loop arms.
    Cross-driver retry middleware (psycopg parity) catches
    ``InterfaceError`` to know when to reconnect; ``ProgrammingError``
    escapes the reconnect path.

    A live-but-different loop is genuinely a programmer mistake
    (forgot to construct one connection per loop) — ``ProgrammingError``
    is correct there. The message wording is the same in both cases;
    only the class differs.
    """
    if bound is None or bound.is_closed():
        return InterfaceError
    return ProgrammingError


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


def _cascade_cursors_closed(cursors: list[Any]) -> None:
    """Mark every cursor in ``cursors`` closed and scrub its result-
    set state.

    Extracted so ``force_close_transport`` can route through
    ``loop.call_soon_threadsafe`` on a foreign-thread / live-loop
    combo. Without that routing the cursor cascade mutates loop-
    owned state without synchronisation; sibling tasks parked
    mid-fetch on the bound loop could observe a partially-cascaded
    snapshot on resume (``_closed=True`` but ``_description`` still
    populated, or vice-versa).

    Direct attribute writes only — ``AsyncCursor.close`` is async
    and would produce un-awaited coroutines from this sync /
    threadsafe-scheduled context. ``_closed = True`` is the load-
    bearing write; the cursor's own ``_check_closed`` gates all
    reads and is checked first by every method, so a cursor whose
    later field-writes are skipped on signal arrival still rejects
    fetch attempts cleanly.
    """
    for cur in cursors:
        cur._closed = True
        cur._rows = []
        cur._description = None
        cur._rowcount = -1
        cur._lastrowid = None
        cur._row_index = 0
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

    # PEP 249 optional extension ("Attributes from Module Exceptions"):
    # parity with the sync ``Connection`` class so cross-driver code can
    # write ``except aconn.Error:`` without importing the driver module.
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
    # dqlite-specific extension subclass of OperationalError. Mirrors
    # the sync Connection's class-attribute table for the same
    # introspection / autocomplete symmetry.
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
        begin_immediate: bool | None = None,
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
            dial_timeout: Per-TCP-connect budget (seconds) — mirrors
                go-dqlite's ``Config.DialTimeout``. ``None`` (default)
                collapses onto ``timeout``. Forwarded to the underlying
                DqliteConnection.
            attempt_timeout: Per-attempt envelope (seconds) covering
                dial + handshake + first RPC — mirrors go-dqlite's
                ``Config.AttemptTimeout``. ``None`` (default) collapses
                onto ``timeout``. Forwarded to the underlying
                DqliteConnection.
            dial_func: Caller-supplied async dialer replacing the
                default TCP path — mirrors go-dqlite's
                ``WithDialFunc``. Use cases: TLS, unix-socket,
                custom KEEPALIVE, out-of-band health probes.
                ``None`` (default) uses the standard
                ``asyncio.open_connection`` path. Forwarded to the
                underlying DqliteConnection. See
                :data:`dqliteclient.DialFunc`.
            busy_timeout: Maximum cumulative seconds to spend retrying
                BUSY responses before raising. Default ``5.0`` matches
                stdlib ``sqlite3.connect(timeout=5.0)``. Retries
                follow SQLite's ``sqliteDefaultBusyCallback`` curve.
                ``0`` disables retry. PRAGMA-intercept contract
                mirrors the sync sibling — see
                ``dqlitedbapi.Connection.__init__``.
        """
        import math as _math

        _validate_timeout(timeout)
        _validate_close_timeout(close_timeout)
        if dial_timeout is not None:
            _validate_timeout(dial_timeout)
        if attempt_timeout is not None:
            _validate_timeout(attempt_timeout)
        # ``busy_timeout`` validation: mirror sync sibling.
        if isinstance(busy_timeout, bool) or not isinstance(busy_timeout, (int, float)):
            raise TypeError(
                f"busy_timeout must be a number (seconds); got {type(busy_timeout).__name__}"
            )
        if not _math.isfinite(busy_timeout) or busy_timeout < 0:
            raise ValueError(
                f"busy_timeout must be a non-negative finite number; got {busy_timeout}"
            )
        # Eager address parse, matching the sync Connection and the
        # underlying DqliteConnection. A typoed DSN surfaces at
        # construction, not at first-use.
        if not isinstance(address, str):
            raise InterfaceError(
                f"address must be a 'host:port' string, got {type(address).__name__}"
            )
        if not isinstance(database, str):
            raise InterfaceError(f"database must be a str, got {type(database).__name__}")
        if not database:
            raise InterfaceError("database must be a non-empty string")
        if database != database.strip():
            # See sibling discipline in ``dqlitedbapi/connection.py``
            # for rationale.
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
        # See sync sibling for rationale: ``None`` falls back to the
        # wire-layer default; the wire layer revalidates.
        self._max_message_size = max_message_size
        self._trust_server_heartbeat = trust_server_heartbeat
        self._close_timeout = close_timeout
        self._dial_timeout = dial_timeout
        self._attempt_timeout = attempt_timeout
        self._dial_func = dial_func
        # Stdlib parity (see sync sibling) — float seconds, default
        # 5.0. Shared with PRAGMA setter; either tunes the other.
        self._busy_timeout: float = float(busy_timeout)
        # ``begin_immediate``: see sync sibling Connection.__init__
        # for the full rationale. ``None`` consults the env var
        # ``DQLITE_BEGIN_IMMEDIATE`` at construction time.
        from dqlitedbapi._pragma_intercept import begin_immediate_default_from_env

        self._begin_immediate: bool = (
            begin_immediate_default_from_env() if begin_immediate is None else bool(begin_immediate)
        )
        self._async_conn: DqliteConnection | None = None
        self._closed = False
        # Tracks the asyncio.Task that currently owns the
        # ``conn.transaction()`` context manager body; used by
        # commit() / rollback() to reject stray transaction-control
        # calls from inside the ctxmgr body (which would otherwise
        # silently exit the surrounding transaction).
        self._transaction_owner: asyncio.Task[Any] | None = None
        # stdlib ``sqlite3.Connection.row_factory`` parity. None
        # means "return plain tuples". New cursors inherit this default.
        self._row_factory: RowFactory | None = None
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
        # PEP 249 optional extension; see Connection.messages. Tuple-
        # value type is the ``exception value`` per PEP 249 §13 —
        # an Exception instance, not a string.
        self.messages: list[tuple[type[Exception], Exception]] = []
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
        # Save the finalize handle so ``close()`` and
        # ``force_close_transport()`` can ``detach()`` it on orderly
        # shutdown — otherwise the registered finalizer plus its
        # captured ``(closed_flag, connected_flag, address)`` cells
        # stay on the ``weakref`` global table for the lifetime of
        # ``self`` even after the user closed the connection.
        # Symmetric with the sync sibling's ``self._finalizer.detach()``
        # calls in ``connection.py`` (one each in ``close()``,
        # ``force_close_transport()``, and ``_cleanup_loop_thread()``)
        # and the parallel ``DqliteConnection`` /
        # ``ConnectionPool`` finalizer discipline.
        self._finalizer: weakref.finalize[Any, Any] | None = weakref.finalize(
            self,
            _async_unclosed_warning,
            self._closed_flag,
            self._connected_flag,
            address,
            self._creator_pid,
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
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Fork-after-init: asyncio primitives are bound to the parent
        # loop and the inherited socket is shared. Reject up front
        # with the same diagnostic the sync sibling and the client
        # layer use, so a forked worker sees a clear "reconstruct in
        # the child" message instead of a confusing
        # "Connection bound to a different event loop" error.
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

        Raises ``InterfaceError`` on closed / post-fork; raises
        ``ProgrammingError`` if the connection has already bound
        to a different loop. No-op when not yet bound.
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
        """Validate the current loop matches the bound loop; do NOT
        check ``_closed`` and do NOT bind on first use.

        Used by ``AsyncCursor.__aiter__`` to mirror PEP 234 / PEP 492
        sync-side behaviour: ``iter(cur) is cur`` succeeds even on
        a closed cursor / closed connection — the closed-state
        diagnostic is deferred to the first ``fetchone`` /
        ``__anext__``. The loop-binding check is preserved so a
        cross-loop misuse fails up front.

        Pid mismatch is the strictly stronger condition than loop
        mismatch (an inherited ``_loop_ref`` weakref may still
        resolve to the parent's loop object in the child's address
        space — making the loop comparison meaningless). Raise the
        canonical ``InterfaceError("used after fork")`` before
        checking the loop so cross-driver retry middleware catching
        ``InterfaceError`` (psycopg parity) sees the right
        diagnostic class. ``_check_loop_binding`` layers the same
        order (closed → pid → loop); the standalone variant must
        match.
        """
        creator_pid = getattr(self, "_creator_pid", None)
        if creator_pid is not None and get_current_pid() != creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        # ``getattr``-safe for ``_loop_ref`` mirrors the
        # ``_creator_pid`` arm above — bare-instantiation /
        # ``__new__`` test patterns can reach
        # ``_check_loop_only`` from the new commit/rollback pre-clear
        # site without the binding having been initialised.
        loop_ref = getattr(self, "_loop_ref", None)
        if loop_ref is None:
            return  # not yet bound — don't bind from here
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Outside an async context; the cursor method will raise
            # NotSupportedError or no-op anyway. Don't manufacture a
            # different error.
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

        # Fork-after-init guard on the FAST path too. ``_ensure_locks``
        # already enforces this on the lazy-create branch, but the
        # already-connected fast-path return below would otherwise hand
        # the parent's inner ``DqliteConnection`` to a forked child
        # silently — the next ``cursor()`` / ``execute()`` re-checks
        # and raises, but the eager ``await aconn.connect()`` health-
        # probe shape (and ``transaction()``'s sibling pre-check at
        # ``_check_loop_binding``) expect the diagnostic at this call
        # site, not one frame downstream. Mirrors the sync sibling's
        # pid check at the top of ``_ensure_loop`` and the symmetric
        # ``_check_loop_binding()`` precall on ``transaction()``.
        creator_pid = getattr(self, "_creator_pid", None)
        if creator_pid is not None and get_current_pid() != creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {creator_pid}, "
                f"current pid {get_current_pid()})"
            )

        # Cross-loop diagnostic on the fast path. ``transaction()``
        # already pre-calls ``_check_loop_binding`` before reaching
        # ``_ensure_connection``; ``connect()`` (the documented
        # eager-TCP-open / fail-fast entry point used by SA
        # pool_pre_ping-style probes) does not. Without the check
        # here a cross-loop ``await aconn.connect()`` on an
        # already-bound connection returns silently from the
        # fast-path arm below and the diagnostic is deferred until
        # the next ``cursor()`` / ``execute()`` — defeating the
        # health-probe shape. Mirrors the symmetric pre-check on
        # ``transaction()``; idempotent so double invocation from
        # callers that pre-check is safe.
        self._check_loop_binding()

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
                max_message_size=getattr(self, "_max_message_size", None),
                trust_server_heartbeat=self._trust_server_heartbeat,
                close_timeout=self._close_timeout,
                dial_timeout=getattr(self, "_dial_timeout", None),
                attempt_timeout=getattr(self, "_attempt_timeout", None),
                dial_func=getattr(self, "_dial_func", None),
            )
            # A concurrent close() may have flipped _closed while we were
            # suspended in _build_and_connect. close() observes
            # _async_conn is None at that point and early-returns, so if
            # we published ``built`` now the caller would hold a live
            # socket that nobody will close. Close the fresh connection
            # and signal the caller instead. Shield the close so an
            # outer cancellation cascade (engine.dispose() racing
            # acquires) cannot interrupt the inner close mid-await and
            # leak the freshly-built transport. Mirrors the pool's
            # ``asyncio.shield(conn.close())`` discipline at every
            # cleanup site.
            #
            # The suppress catches non-cancel ``Exception``s from the
            # close (e.g., ``OSError`` on a stale transport) so the
            # diagnostic ``InterfaceError`` below is the user-visible
            # signal. ``CancelledError`` is NOT in the catch set: with
            # ``asyncio.shield``, the inner close runs in the background
            # while the outer ``await`` re-raises ``CancelledError``;
            # suppressing that re-raise would swallow the cancel and
            # surface ``InterfaceError`` instead, breaking the
            # cooperative-cancellation contract for callers using
            # ``task.cancel()`` / ``asyncio.timeout(...)`` / TaskGroup
            # siblings.
            if self._closed:
                # Schedule the cleanup-close as a Task with an explicit
                # ``_observe_drain_exception`` done-callback so the
                # implicit Task ``asyncio.shield(coro)`` would otherwise
                # create is not orphaned. See sibling pattern at
                # connection.py:2310 / 2300-2308 commentary.
                from dqliteclient.cluster import _observe_drain_exception

                inner_drain = asyncio.ensure_future(built.close())
                inner_drain.add_done_callback(_observe_drain_exception)
                with contextlib.suppress(Exception):
                    await asyncio.shield(inner_drain)
                raise InterfaceError(f"Connection is closed (id={id(self)})")
            self._async_conn = built
            # Flip the finalizer's "anything to clean up" gate. From
            # this point GC without close emits the ResourceWarning;
            # before this point a never-connected instance is silent.
            self._connected_flag[0] = True

        return self._async_conn

    async def connect(self) -> None:
        """Eagerly establish the TCP session.

        Optional — the connection is lazy and the first cursor() or
        execute() will connect automatically. Await this to fail-fast
        when the cluster is unreachable, without allocating a cursor.
        Mirrors :meth:`Connection.connect` (the sync sibling).
        """
        # Project-wide invariant: every public Connection method
        # clears ``messages`` first. ``connect()`` is a dqlite
        # extension (not in PEP 249), but the uniform discipline
        # extends here so a stale entry doesn't survive an eager-
        # connect call. Mirrors the sync sibling.
        del self.messages[:]
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
        if get_current_pid() != self._creator_pid:
            self._closed = True
            self._closed_flag[0] = True
            # Detach the finalizer — symmetric with sync sibling's
            # ``self._finalizer.detach()`` inside ``close()``. Keeps
            # the ``weakref`` global table free of stale entries
            # after orderly
            # close.
            if self._finalizer is not None:
                self._finalizer.detach()
                self._finalizer = None
            # Walk the inner client conn's own fork-close path
            # before dropping the reference. The inner client's
            # ``DqliteConnection.close()`` fork short-circuit nulls
            # ``_pending_drain``: an inherited parent-loop
            # ``asyncio.Task`` reference would otherwise persist as
            # a Python attribute on the inner conn, get GC'd in
            # the child, and trip the asyncio pending-task tracker
            # — emitting "Task was destroyed but it is pending" at
            # child interpreter shutdown. The sync sibling
            # already drives the inner fork-close; mirror that
            # discipline here so the asymmetric noise goes away.
            if self._async_conn is not None:
                inner = self._async_conn
                pending = getattr(inner, "_pending_drain", None)
                if pending is not None:
                    # The Task is bound to the parent's (now-defunct)
                    # loop; the child cannot ``await`` it. Drop the
                    # Python reference so GC reclaims without the
                    # pending-task tracker complaint. ``Task.cancel()``
                    # would also be safe here but is a no-op against
                    # a parent-loop task; just clearing the slot is
                    # enough.
                    with contextlib.suppress(Exception):
                        inner._pending_drain = None
                # Same null-out for ``_protocol`` / ``_db_id`` —
                # they reference parent-loop StreamReaderProtocol
                # / StreamWriter pairs that the child cannot use.
                with contextlib.suppress(Exception):
                    inner._protocol = None
                    inner._db_id = None
            self._async_conn = None
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            self._cursors.clear()
            # Clear the transaction-owner slot as a defensive backstop —
            # see comment at the bottom of close() for rationale. Run
            # AFTER the protocol/locks are torn down so a concurrent
            # task in transaction() retains its slot ownership for as
            # long as the connection remained operable.
            self._transaction_owner = None
            return
        # Set _closed first so any task waiting on the lock sees the
        # closed state as soon as it acquires. Then drain the current
        # in-flight op (if any) under the lock.
        self._closed = True
        self._closed_flag[0] = True
        # Detach the finalizer — symmetric with sync sibling's
        # ``self._finalizer.detach()`` inside ``force_close_transport``.
        # Keeps the ``weakref`` global table free of stale entries
        # after orderly close.
        finalizer = getattr(self, "_finalizer", None)
        if finalizer is not None:
            finalizer.detach()
            self._finalizer = None

        def _cascade_cursors() -> None:
            """Run the cursor cascade. Direct attribute writes —
            ``AsyncCursor.close`` is async and would produce un-awaited
            coroutines from a synchronous loop. ``_closed = True`` is
            the LOAD-BEARING write; the cursor's own
            ``_check_closed()`` gates all reads, so a cursor whose
            later field-writes are skipped on signal-arrival still
            rejects fetch attempts cleanly."""
            try:
                for cur in list(self._cursors):
                    cur._closed = True
                    cur._rows = []
                    cur._description = None
                    cur._rowcount = -1
                    cur._lastrowid = None
                    cur._row_index = 0
                    del cur.messages[:]
                    with contextlib.suppress(TypeError):
                        cur._connection = weakref.proxy(cur._connection)
            finally:
                self._cursors.clear()

        if self._async_conn is None:
            # Never-connected path: no in-flight op to race against;
            # cascade outside the lock.
            _cascade_cursors()
            # Null the lazy locks so a subsequent fixture or
            # SQLAlchemy-glue reuse of the object in a different event
            # loop cannot observe a primitive bound to the dead loop.
            # Parity with the sync close() reset established for
            # connect_lock.
            self._connect_lock = None
            self._op_lock = None
            self._loop_ref = None
            # Clear the transaction-owner slot as a defensive backstop —
            # see comment at the bottom of close() for rationale.
            self._transaction_owner = None
            return
        # Use the already-bound op_lock directly; calling
        # ``_ensure_locks`` now raises because ``_closed`` is True.
        # If _async_conn is set, the locks were created on a prior
        # ``_ensure_locks`` call — otherwise the short-circuit above
        # at ``_async_conn is None`` returned.
        assert self._op_lock is not None
        op_lock = self._op_lock
        op_lock_timed_out = False
        try:
            # Bound the op_lock acquire by ``_SYNC_PHASES_MULTIPLIER *
            # self._timeout`` to match the sync sibling's overall
            # budget for ``_run_sync(_close_async())``. The sync side
            # applies ``sync_timeout = _SYNC_PHASES_MULTIPLIER *
            # self._timeout`` at the ``Future.result(timeout=...)``
            # boundary; without the multiplier here the async surface
            # false-times-out on benign latency (slow first-call-after-
            # connect: handshake + open_database + read+drain
            # combined) that the sync surface tolerates. The SIGTERM
            # hang protection still holds — the bound is widened, not
            # removed.
            #
            # ``self._timeout`` (NOT ``self._close_timeout``) is the
            # right per-phase budget: ``_close_timeout`` is the
            # transport-drain window, while siblings are bounded by
            # the per-RPC ``timeout``.
            phases_budget = _SYNC_PHASES_MULTIPLIER * self._timeout
            async with asyncio.timeout(phases_budget):
                async with op_lock:
                    # Run the cursor cascade INSIDE the lock so concurrent
                    # in-flight fetches (which also hold the lock for their
                    # protocol round-trip) cannot see partial state — a
                    # fetch yielded between ``_check_closed()`` and a
                    # subsequent buffer read previously could observe
                    # ``_rows = []`` mid-iteration and surface a misleading
                    # ``ProgrammingError("no results to fetch")`` instead
                    # of an ``InterfaceError("Cursor is closed")``.
                    _cascade_cursors()
                    if self._async_conn is not None:
                        await self._async_conn.close()
                        self._async_conn = None
        except TimeoutError:
            # Sibling holds op_lock past ``self._timeout``. Don't wait
            # further; force-close the transport synchronously so
            # SIGTERM / dispose don't hang. The sibling's pending
            # ``reader.read()`` will see EOF and surface a transport
            # error, which its ``_run_protocol`` finally observes.
            #
            # The user's contract violation (close racing an in-flight
            # op) surfaces as a transport failure on the in-flight
            # operation — preferable to a multi-minute hang.
            op_lock_timed_out = True
            # ``force_close_transport`` is synchronous, idempotent, and
            # never raises. It nulls ``self._async_conn`` itself.
            self.force_close_transport()
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
                # Schedule the underlying close as a Task with an
                # explicit ``_observe_drain_exception`` done-callback
                # so the implicit Task ``asyncio.shield(coro)`` would
                # otherwise create is not orphaned. The explicit
                # observer absorbs any eventual non-OSError raise on
                # the abandoned task path (interpreter shutdown,
                # transport tear-down) so no "Task exception was
                # never retrieved" warning surfaces.
                from dqliteclient.cluster import _observe_drain_exception

                inner_drain = asyncio.ensure_future(self._async_conn.close())
                inner_drain.add_done_callback(_observe_drain_exception)
                try:
                    await asyncio.shield(inner_drain)
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
                    # Drain any ``_pending_drain`` task scheduled on
                    # the inner client by a sibling task's
                    # ``_invalidate``. The inner client's canonical
                    # ``_close_impl`` awaits this task explicitly; the
                    # cross-task fallback above bypasses
                    # ``_close_impl`` entirely (raised InterfaceError
                    # before reaching it), so the drain bookkeeping
                    # has to be mirrored here. Without this, the
                    # only remaining reachability for the drain task
                    # is the inner conn — which we are about to
                    # null — and Python prints "Task was destroyed
                    # but it is pending" at GC. Defensive None /
                    # done() check: typical entry has no pending
                    # drain (the InterfaceError fired BECAUSE
                    # ``_invalidate`` had not yet run); the drain
                    # only exists if the sibling raced a prior
                    # invalidate ahead of this close.
                    pending = getattr(inner, "_pending_drain", None)
                    if pending is not None and not pending.done():
                        # Narrow suppress: KI/SystemExit are one-shot
                        # signals that must propagate; CancelledError
                        # is re-delivered at the next await so absorbing
                        # it here is the canonical shield+suppress idiom.
                        # Mirrors the narrowing of the same pattern in
                        # ``DqliteConnection._close_impl`` and
                        # ``ConnectionPool._release``.
                        with contextlib.suppress(Exception, asyncio.CancelledError):
                            await asyncio.shield(pending)
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
                    #
                    # Cancel the inner client conn's ``_pending_drain``
                    # task BEFORE nulling our reference: the orderly
                    # ``_close_impl`` path normally drains it under
                    # bounded resnapshot; on this cancel arm the
                    # shielded close was cancelled mid-drain, so the
                    # inner conn may still own a pending Task. Once
                    # we null ``self._async_conn`` the inner is
                    # unreachable from us, and GC of the inner emits
                    # the exact "Task was destroyed but it is
                    # pending" warning that ``force_close_transport``
                    # goes to extensive lengths to prevent. Mirror
                    # that discipline here with a best-effort
                    # ``cancel()`` — we are already on a cancel arm
                    # so do NOT await; the next loop tick reaps it.
                    inner = self._async_conn
                    if inner is not None:
                        pending = getattr(inner, "_pending_drain", None)
                        if pending is not None and not pending.done():
                            pending.cancel()
                    # Force-close the transport synchronously so the
                    # writer / FD is reaped regardless of which cancel
                    # delivered us here. The shielded ``inner_drain``
                    # would normally drive ``_close_impl``'s writer
                    # close; if a fresh cancel landed during the
                    # shielded await (Python's ``asyncio.shield`` only
                    # blocks the FIRST cancel) the writer is still
                    # open and the FD would leak to GC. Outer
                    # ``asyncio.timeout`` deadlines also hit this arm
                    # (the inner ``except TimeoutError`` only fires on
                    # the INNER scope's expiry); without this call
                    # operators wrapping ``await conn.close()`` in
                    # their own deadline lost the transport-hygiene
                    # backstop. ``force_close_transport`` is
                    # synchronous, idempotent, and never raises — and
                    # it sets ``self._async_conn = None`` itself.
                    self.force_close_transport()
                    self._async_conn = None
                    self._connect_lock = None
                    self._op_lock = None
                    self._loop_ref = None
                    # Defensive backstop clear — see the trailing
                    # comment in this method's normal path.
                    self._transaction_owner = None
                    # Defensive parity with the orderly + fork-shortcut
                    # arms: detach the finalizer here too so the
                    # discipline is explicit at every termination
                    # shape. The orderly top-of-close detach at line
                    # 562-565 already runs before reaching this arm
                    # on the current code shape, so this is a no-op
                    # in practice — but a future refactor that
                    # reorders or removes the top-of-close detach
                    # would silently leak the finalizer-registry
                    # entry on cancel without this redundant clear.
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
        """PEP 525 / ``contextlib.aclosing``-compatible alias for
        :meth:`close`.

        Provided for cross-driver portability with code that targets
        the aiosqlite / asyncpg / psycopg ``aclose()`` shape and with
        ``contextlib.aclosing(conn)``. Equivalent to ``await
        self.close()`` — :meth:`close` is already ``async def`` so
        the alias is a thin wrapper rather than a wrap-around-sync
        bridge. Symmetric with :meth:`AsyncCursor.aclose`.
        """
        await self.close()

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

        Concurrent-safety: this method is intended for the case where
        the bound event loop has already been closed (typical SA pool
        finalize / GC sweep / atexit). It is NOT safe to invoke
        against a live loop from a non-loop thread:
        ``StreamWriter.close()`` is documented as not thread-safe in
        stdlib asyncio; calling it from a thread other than the loop's
        owning thread races with the selector's transport state.
        Production callers (SA finalize, atexit, GC) hit the
        loop-already-dead path where this concern does not apply.
        Callers that need to wait for the async path to drain from a
        live loop should await ``close()`` from the original loop
        instead.

        The pending-drain cancel is loop-aware (see the implementation
        below): on the owning thread or with a closed loop, cancel
        runs directly; on a foreign thread with a live loop the cancel
        is scheduled via ``call_soon_threadsafe`` so the ready-queue
        is not mutated cross-thread.
        """
        # PEP 249 §6.4 + project discipline: every public Connection
        # method clears ``messages`` as the first statement.
        # ``contextlib.suppress(AttributeError)`` tolerates
        # ``__new__``-built fixtures that bypass ``__init__``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
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
        # Cascade the closed state down to every open cursor BEFORE
        # the inner snapshot — mirrors the sync sibling
        # ``Connection.close``'s ``self._cascade_cursors()`` invocation
        # at the same point in the orderly close. Without this,
        # cursors retain ``_closed = False`` + populated
        # ``_description`` / ``_rows`` / ``_rowcount`` AND strong
        # ``_connection`` references to the dead ``AsyncConnection``,
        # defeating the close-orchestrated ``weakref.proxy`` swap.
        # Direct attribute writes only — ``AsyncCursor.close`` is
        # async and would produce un-awaited coroutines from this
        # sync context. ``_closed = True`` is the load-bearing write;
        # the cursor's own ``_check_closed`` gates all reads, so a
        # cursor whose later field-writes are skipped on signal
        # arrival still rejects fetch attempts cleanly. Tolerate
        # ``_cursors`` being absent on ``__new__``-constructed
        # fixtures that bypass ``__init__``.
        #
        # Loop-aware routing: a foreign-thread caller (SA
        # do_terminate / pool.dispose from an AsyncEngine running its
        # own loop in another thread) mutating the cursor fields
        # directly races sibling tasks on the bound loop that may be
        # mid-fetch and observing the cursor's state. Each individual
        # attribute write is GIL-atomic but the SEQUENCE is not —
        # a sibling reading ``description`` then ``rowcount`` could
        # see a partially-cascaded snapshot. Mirror the writer-close
        # discipline below: ``call_soon_threadsafe`` when the loop is
        # alive on a foreign thread, direct call when on the owning
        # thread or when the loop is already closed.
        cursors = getattr(self, "_cursors", None)
        if cursors is not None:
            bound_loop_ref = getattr(self, "_loop_ref", None)
            bound_loop = bound_loop_ref() if bound_loop_ref is not None else None
            running = None
            with contextlib.suppress(RuntimeError):
                running = asyncio.get_running_loop()
            cursors_snapshot = list(cursors)
            cursors.clear()
            # Pre-set ``_closed = True`` synchronously on every cursor
            # so a sibling task on the bound loop that races a
            # ``fetchone()`` between our sync ``self._closed = True``
            # flip above and the deferred ``call_soon_threadsafe``
            # cascade observes the closed cursor immediately. The
            # single attribute write is GIL-atomic; ``_check_closed``
            # reads only ``_closed`` so there's no SEQUENCE concern
            # here (unlike the introspection-surface fields scrubbed
            # below which are sequence-sensitive and stay deferred).
            # Without this pre-set, ``conn._closed`` was visible on
            # the sibling thread while ``cur._closed`` was still
            # False, and a sibling's ``await cur.fetchone()`` returned
            # rows from a dead transport. ``_cascade_cursors_closed``
            # re-writes ``_closed = True`` (idempotent) so the
            # eventual deferred run is unaffected.
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
            return
        # Fork-after-init: ``writer.close()`` on the inherited socket
        # FD would send FIN on a connection the parent still holds
        # open. Quietly drop the local reference instead so child GC
        # has nothing left to act on. Symmetric with the
        # ``DqliteConnection.close`` and ``Connection.close`` fork
        # short-circuits; this synchronous force-close path completes
        # the symmetric pid-guard discipline.
        if get_current_pid() != self._creator_pid:
            self._async_conn = None
            return
        # Disarm the inner client's ResourceWarning finalizer
        # (``DqliteConnection._connection_unclosed_warning``) BEFORE
        # we reap the transport: the warning's three-flag gate
        # (``_closed_flag`` AND ``_connected_flag``) fails open after
        # the writer.close path runs and ``self._async_conn`` is
        # nulled — emitting a misleading "GC'd without close" on the
        # very inner connection we are explicitly closing here.
        # ``close()``'s code path detaches the inner finalizer inside
        # ``_close_impl``; ``force_close_transport`` doesn't route
        # through ``close()`` so the detach has to happen here.
        # ``getattr`` + ``suppress`` mirror the dbapi-layer
        # finalizer detach above (idempotent against concurrent
        # paths / partial init).
        inner_closed_flag = getattr(inner, "_closed_flag", None)
        if isinstance(inner_closed_flag, list) and inner_closed_flag:
            inner_closed_flag[0] = True
        inner_finalizer = getattr(inner, "_finalizer", None)
        if inner_finalizer is not None:
            with contextlib.suppress(Exception):
                inner_finalizer.detach()
            inner._finalizer = None
        # Close the transport writer if one exists; the cleanup tail
        # below (pending-drain reap and ``self._async_conn = None``)
        # runs unconditionally so a post-_invalidate state where
        # ``_protocol`` has already been cleared (or a
        # ``_writer is None`` on a partially-constructed inner) does
        # not skip the reap and the null-out.
        proto = getattr(inner, "_protocol", None)
        writer = getattr(proto, "_writer", None) if proto is not None else None
        if writer is not None:
            # ``StreamWriter.close()`` mutates the transport's
            # _SelectorSocketTransport state and is documented as
            # not thread-safe. The typical caller chain (SA
            # do_terminate / pool.dispose from an AsyncEngine
            # running its own loop in another thread) can land here
            # with the loop STILL ALIVE on a different thread; a
            # direct ``writer.close()`` then races the selector's
            # transport-state bookkeeping. Mirror the loop-aware
            # discipline applied to the pending-drain cancel below:
            # ``call_soon_threadsafe`` when the loop is alive on a
            # foreign thread, direct call when on the owning thread
            # or when the loop is already closed.
            bound_loop_ref = getattr(self, "_loop_ref", None)
            bound_loop = bound_loop_ref() if bound_loop_ref is not None else None
            running = None
            with contextlib.suppress(RuntimeError):
                running = asyncio.get_running_loop()
            try:
                if bound_loop is None or bound_loop.is_closed():
                    # Loop is dead; safe to call directly (the
                    # writer's transport teardown runs synchronously
                    # without touching the dead selector).
                    writer.close()
                elif running is bound_loop:
                    # Owning thread — direct mutation is safe.
                    writer.close()
                else:
                    # Live loop on a foreign thread: schedule via
                    # call_soon_threadsafe so the selector mutation
                    # runs on the owning thread.
                    bound_loop.call_soon_threadsafe(writer.close)
            except Exception:  # noqa: BLE001 - last-resort cleanup
                logger.debug(
                    "AsyncConnection.force_close_transport (id=%s): "
                    "writer.close() raised; ignoring",
                    id(self),
                    exc_info=True,
                )
        # Reap any ``_pending_drain`` task on the inner client. The
        # canonical async ``close()`` awaits this task; the sync
        # helper cannot await (no loop, no greenlet), so we cancel
        # and null instead. ``Task.cancel()`` is NOT documented as
        # thread-safe — CPython implements it via ``loop.call_soon``
        # which is also not thread-safe. The threadsafe analogue is
        # ``loop.call_soon_threadsafe(task.cancel)``. The typical
        # SA-finalize / atexit / GC path hits this method when the
        # task's owning loop has already been closed, in which case
        # the task is itself in a terminal state and ``cancel()`` is
        # a no-op. Use a loop-aware schedule path that defers to
        # ``call_soon_threadsafe`` when the loop is alive but owned
        # by another thread, so we never mutate the ready-queue
        # cross-thread; fall back to a direct cancel when on the
        # owning thread; short-circuit when the loop is closed
        # (cancel would be a no-op anyway). Done-check skips the
        # redundant schedule on a typical "drain already raced
        # ahead" path. Wrapped suppress because attribute writes on
        # the inner conn could in principle raise (e.g. __slots__
        # violations on a custom subclass).
        # Bounded re-snapshot loop: a concurrent ``_invalidate``
        # callback queued via ``loop.call_soon_threadsafe`` from the
        # loop thread can run between our snapshot and our null-out
        # and CREATE a NEW ``_pending_drain`` task on the same inner
        # conn. Without re-snapshotting, that fresh drain task is
        # orphaned — surfacing as "Task was destroyed but it is
        # pending" at GC under SA finalize-from-foreign-thread paths.
        # Mirrors the in-thread re-snapshot loop in
        # ``DqliteConnection._close_impl`` (cap-and-fail-loud
        # discipline).
        resnapshot_cap = 3
        for _attempt in range(resnapshot_cap):
            pending = getattr(inner, "_pending_drain", None)
            with contextlib.suppress(Exception):
                inner._pending_drain = None
            if pending is None or pending.done():
                break
            with contextlib.suppress(Exception):
                # Determine whether the task's owning loop is alive
                # AND owned by this thread. Only the
                # owning-thread-on-live-loop path can mutate
                # ``Task.cancel`` directly; every other path must
                # defer via ``call_soon_threadsafe`` (or fall back to
                # a direct cancel when the loop is gone — Task.cancel
                # is a no-op on a closed loop, but still inside the
                # asyncio C contract).
                try:
                    pending_loop = pending.get_loop()
                except Exception:
                    pending_loop = None
                running = None
                with contextlib.suppress(RuntimeError):
                    running = asyncio.get_running_loop()
                if pending_loop is None or pending_loop.is_closed():
                    # Loop is gone; cancel is a no-op anyway. Call
                    # directly — matches the pre-fix contract callers
                    # depend on for the SA finalize / atexit / GC path.
                    pending.cancel()
                elif running is pending_loop:
                    # On the owning thread; safe to mutate directly.
                    pending.cancel()
                else:
                    # Live loop, foreign thread: schedule via
                    # call_soon_threadsafe so the cancel runs on the
                    # owning thread without racing the ready-queue.
                    # Wrap so the cancelled task's CancelledError is
                    # observed via a done-callback — otherwise asyncio's
                    # task-finalisation logger emits "Task exception
                    # was never retrieved" at GC under SA finalize-
                    # from-foreign-thread paths. Mirrors the
                    # ``_observe_drain_exception`` pattern used in
                    # the client layer.
                    def _cancel_and_observe(target: asyncio.Task[Any]) -> None:
                        target.cancel()

                        def _observe(t: asyncio.Task[Any]) -> None:
                            # Narrow to Exception — KeyboardInterrupt /
                            # SystemExit must propagate through
                            # done-callbacks. Matches the project-wide
                            # narrow-suppress discipline established at
                            # _close_impl / pool-release-shielded /
                            # aio-terminate / _observe_drain_exception.
                            if not t.cancelled():
                                with contextlib.suppress(Exception):
                                    t.exception()

                        target.add_done_callback(_observe)

                    pending_loop.call_soon_threadsafe(_cancel_and_observe, pending)
        else:
            # Cap exhausted: a racing ``_invalidate`` keeps creating
            # fresh ``_pending_drain`` tasks each iteration. Final
            # defensive null-out + WARNING so operators see the
            # pathological feedback loop. The last iteration's
            # null-out at the top of the body is redundantly applied
            # here for symmetry with the cap-exhausted branch in
            # ``DqliteConnection._close_impl``.
            with contextlib.suppress(Exception):
                inner._pending_drain = None
            logger.warning(
                "AsyncConnection.force_close_transport: inner._pending_drain still "
                "set after %d re-snapshot iterations; cancelling residual task to "
                "avoid 'Task was destroyed but it is pending' at GC. This indicates "
                "a pathological _invalidate feedback loop on inner conn.",
                resnapshot_cap,
            )
        # Mirror the fork-branch null-out so the AsyncConnection
        # does not keep claiming to reference a live inner conn
        # after force-close. The fork-after-init branch in this
        # method (the ``if get_current_pid() != self._creator_pid:``
        # arm) already nulls; this brings the regular non-fork path
        # to the same discipline.
        self._async_conn = None

    @property
    def in_transaction(self) -> bool:
        """Whether the connection currently has an open transaction.

        **Divergence from stdlib**: stdlib
        ``sqlite3.Connection.in_transaction`` raises
        ``ProgrammingError`` on a closed connection; this driver
        returns ``False`` instead (symmetric with the sync sibling
        :attr:`dqlitedbapi.Connection.in_transaction`), by definition
        (a closed connection cannot hold an open transaction). Never-
        connected connections likewise return ``False``. This makes
        the getter safe to use in shutdown paths that need to decide
        whether to commit or rollback before close, without an extra
        closed-state try / except scaffold. Cross-driver code that
        relies on stdlib's raise behaviour to detect a closed
        connection should use the ``closed``-state probe directly,
        not ``in_transaction``.

        Raises ``ProgrammingError`` if read from a foreign event loop
        — symmetric with the sync sibling's ``_check_thread()`` raise
        on cross-thread access. Without this, the underlying client-
        layer ``DqliteConnection.in_transaction`` would silently read
        loop-bound state from the wrong loop, and a follow-up
        ``rollback()`` would then raise — making the property
        un-usable as a cleanup-path discriminator. Uses
        ``_check_loop_only`` (not ``_check_loop_binding``) so the
        check does not lazy-bind the loop on first read.

        **Lazy-loop-bind divergence**: on an ``AsyncConnection`` that
        has never been awaited (loop ref not yet captured), accesses
        from any thread / loop return ``False`` silently — the
        cross-loop diagnostic is only enforceable after the first
        await binds the connection. The sync sibling raises eagerly on
        cross-thread access from any state because it captures the
        creator-thread at ``__init__``; the async side cannot do the
        same without breaking SA's lazy-construct-without-loop adapter
        pattern. Code that needs the eager cross-context check on a
        never-awaited connection must call ``connect()`` first.

        **Closed-state precedence**: the closed short-circuit runs
        BEFORE ``_check_loop_only()`` so a foreign-loop reader of a
        closed connection (e.g. a shutdown coroutine running on
        loop B that decides commit-vs-rollback against a connection
        opened on loop A) gets the documented ``False`` rather than
        a ``LoopError`` cross-loop violation. A closed connection
        is observably immutable; loop affinity becomes moot once
        ``close()`` has run. Mirrors the sync sibling's
        closed-then-thread ordering.
        """
        # Snapshot the reference once: ``close()`` running concurrently
        # may null ``_async_conn`` between a None-check and an attribute
        # read. The local binding is immutable for the duration of the
        # property body, eliminating that window. ``bool(...)`` keeps the
        # mock-adapter safety from the stdlib-parity introduction.
        conn = self._async_conn
        if conn is None or self._closed:
            return False
        self._check_loop_only()
        return bool(conn.in_transaction)

    @property
    def autocommit(self) -> "bool | int":
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

        **Setter / getter round-trip**: stores the setter input on
        ``self._autocommit_value`` and returns it. ``True`` and
        stdlib's ``LEGACY_TRANSACTION_CONTROL`` (``-1``) are both
        accepted; both no-op the wire layer (dqlite is fixed-mode
        autocommit) but the property reflects the caller's last
        input. The stdlib 3.12+ idiom round-trips on this driver.
        Setting to ``False`` (or any non-``True``, non-``-1`` value)
        raises ``NotSupportedError``.

        **Closed-state behaviour**: raises ``InterfaceError`` on a
        closed connection, matching stdlib `sqlite3`'s
        ``ProgrammingError("Cannot operate on a closed database.")``.
        Sibling discipline to the sync ``Connection.autocommit``.

        **Fork-after-init**: raises ``InterfaceError("...used after
        fork...")`` when read from a forked child process. The
        ``_autocommit_value`` instance attribute is fork-inheritable
        (a plain attribute); mirrors the sibling ``_stub_unsupported``
        / ``_ensure_locks`` canonical guard so every public surface
        on this class surfaces fork-after-init before any value read.
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
        # PEP 249 §6.4: ``Connection.messages`` is cleared by every
        # standard connection method before the call runs. Sync sibling
        # clears unconditionally as the first statement; mirror that
        # here so the contract holds on the async surface too. Wrap
        # in ``suppress`` because ``messages`` may not exist yet on a
        # partially-constructed instance.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        # Loop-binding affinity contract — even the no-op accept-path
        # is an attempt that must surface as a contract violation if
        # invoked from a foreign loop. Sibling sync setter calls
        # ``_check_thread()`` for the same reason.
        self._check_loop_binding()
        # See sync sibling for full rationale: accept True or the
        # stdlib sentinel ``sqlite3.LEGACY_TRANSACTION_CONTROL`` (==-1)
        # and reject everything else. The ``-1`` arm uses an exact-int
        # gate (``isinstance(value, int) and not isinstance(value, bool)
        # and value == -1``) matching stdlib's
        # ``Modules/_sqlite/connection.c`` discipline — loose ``value
        # == -1`` previously accepted ``-1.0`` / ``Decimal('-1')`` /
        # custom ``__eq__`` objects. On accept, canonicalise-store
        # as ``int(-1)``.
        if value is True:
            self._autocommit_value: bool | int = value
            return
        if isinstance(value, int) and not isinstance(value, bool) and value == -1:
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
        """stdlib pre-3.12 ``sqlite3.Connection.isolation_level``-
        parity surface. See sync sibling for full rationale.

        **Setter / getter round-trip**: stores the setter input on
        ``self._isolation_level_value`` and returns it. The default
        (never-set) return is ``None`` — the autocommit sentinel.
        Cross-driver ``dst.isolation_level = src.isolation_level``
        round-trips on this driver.

        **Closed-state**: raises ``InterfaceError`` on a closed
        connection, matching stdlib `sqlite3`'s
        ``ProgrammingError("Cannot operate on a closed database.")``.

        **Fork-after-init**: raises ``InterfaceError("...used after
        fork...")`` when read from a forked child process —
        ``_isolation_level_value`` is fork-inheritable. Mirrors the
        sibling ``autocommit`` getter's guard."""
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
        # PEP 249 §6.4 messages-clear; see ``autocommit.setter``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        # Loop-binding affinity contract — see ``autocommit.setter``.
        self._check_loop_binding()
        # See sync sibling for accept-set + ProgrammingError-vs-
        # NotSupportedError class rationale. We STORE the caller's
        # input on ``self._isolation_level_value`` so the getter
        # round-trips — matches the sync sibling's storage discipline.
        if value is None:
            self._isolation_level_value: str | None = value
            return
        if isinstance(value, str) and value.upper() in _STDLIB_IMPLICIT_TX_VALUES:
            self._isolation_level_value = value
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

        Outer-cancel caveat: ``asyncio.timeout`` re-classifies
        ``CancelledError`` to ``TimeoutError`` only when the
        CURRENT scope's deadline expires. If an OUTER deadline
        (operator-wrapped ``async with asyncio.timeout(N): await
        conn.commit()`` where ``N < commit_budget``) cancels this
        method, the inner ``except TimeoutError`` arm does NOT fire
        and the phase-aware ``OperationalError`` diagnostic is
        bypassed — the caller observes raw ``CancelledError``
        instead, and the connection state is ambiguous (the COMMIT
        may or may not have reached the leader). Operators
        wrapping this method in an outer deadline should treat
        ``CancelledError`` as "invalidate the connection" — call
        ``force_close_transport()`` or drop the connection from
        the pool before retry.
        """
        # Loop-affinity check BEFORE the messages-clear so a stray
        # cross-loop ``await aconn.commit()`` does NOT scribble the
        # bound-loop's ``messages`` list. ``_check_loop_only`` is the
        # closed-state-tolerant variant — it raises ``ProgrammingError``
        # on cross-loop misuse and ``InterfaceError`` on post-fork pid
        # mismatch, but is a no-op on closed conns, on not-yet-bound
        # conns, and on same-loop callers. The closed-path clear
        # below therefore still runs for the PEP 249 §6.1.1 contract
        # (messages cleared on every invocation including the raise
        # arm). The owner-Task identity check further below would
        # also have misfired across loops — putting the loop check
        # first short-circuits that branch on the wrong-loop path.
        self._check_loop_only()
        # PEP 249 §6.1.1: ``Connection.messages`` is cleared "prior to
        # executing the call" on every standard connection method —
        # including the closed-path raise below. Sync sibling clears
        # unconditionally as the very first statement; mirror that here
        # so the contract holds regardless of which branch we take.
        del self.messages[:]
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if self._async_conn is None:
            return
        # Reject stray ``await conn.commit()`` from inside a
        # ``conn.transaction()`` body. The ctxmgr owns transaction
        # boundaries; a body's explicit commit silently ends the
        # transaction (subsequent body statements run in autocommit;
        # the surrounding rollback-at-exit no-ops because
        # in_transaction is already False), an asymmetric data-
        # correctness hazard. asyncpg / psycopg both reject nested
        # explicit transaction control on the same shape.
        if (
            self._transaction_owner is not None
            and self._transaction_owner is asyncio.current_task()
        ):
            raise InterfaceError(
                "commit() cannot be issued inside conn.transaction(); "
                "the context manager owns transaction boundaries — "
                "exit the ``async with`` block first."
            )
        # Cancel-after-invalidate contract: a prior commit/rollback that
        # was cancelled mid-flight invalidates the inner client conn
        # AND clears its ``in_transaction`` flag. A naive retry would
        # then short-circuit on the False flag and silently return —
        # hiding partial-commit ambiguity (the cancelled commit may or
        # may not have reached the leader). The pre-lock check below
        # is a fast-path raise; the authoritative recheck happens
        # under ``op_lock`` so a sibling-task ``_invalidate`` that
        # races our ``async with op_lock`` acquire cannot slip
        # through the ``in_transaction``-False short-circuit either.
        if getattr(self._async_conn, "_protocol", "_sentinel") is None:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}); reconnect before "
                "retrying commit / rollback. The prior call may have "
                "reached the leader before cancel landed; server-side "
                "transaction state is ambiguous."
            )
        _, op_lock = self._ensure_locks()
        # Bound the op_lock acquire by ``self._timeout`` — the sync
        # sibling's ``_op_lock.acquire(timeout=self._timeout)`` in
        # ``Connection._run_sync`` and the matching ``close()``
        # discipline above have the same shape. Without the bound,
        # ``commit()`` waits indefinitely on a sibling task parked on a
        # slow ``reader.read()`` (especially under
        # ``trust_server_heartbeat=True`` widening the per-read
        # deadline up to 300 s). Under shutdown — SA
        # ``engine.dispose()`` or app SIGTERM with a budget — the
        # caller would otherwise hang for the full per-read deadline.
        # ``_SYNC_PHASES_MULTIPLIER * self._timeout`` mirrors the sync
        # sibling ``Connection._run_sync``'s overall budget so async
        # commit absorbs the same handshake + open + COMMIT phase
        # combination the sync side tolerates. See ``close()`` for the
        # full rationale.
        commit_budget = _SYNC_PHASES_MULTIPLIER * self._timeout
        entered_lock = False
        # ``request_in_flight`` tracks whether the COMMIT wire round-
        # trip has been initiated AND not yet confirmed-complete. If
        # an outer cancel / signal / timeout lands while this flag is
        # True the wire state is partial: the COMMIT bytes may have
        # reached the leader, the Raft log entry may or may not be
        # appended, the response may or may not be in flight. The
        # transport cannot be trusted on the next op. Force-close so
        # SA's ``is_disconnect`` invalidates the slot (raw
        # CancelledError doesn't trigger is_disconnect; without the
        # force-close, the slot stays in the pool with ambiguous
        # server-side state). Mirrors the close() arm's
        # force-close-on-cancel discipline. Sync sibling lives at
        # ``Connection._commit_async``.
        request_in_flight = False
        # Capture entry time so the TimeoutError arm can annotate the
        # OperationalError diagnostic when both the operator's outer
        # ``asyncio.timeout`` AND our inner ``commit_budget`` deadlines
        # fire near-simultaneously: the inner converts CancelledError
        # to TimeoutError and the message would otherwise name
        # ``commit_budget`` even though the actual elapsed time was
        # set by the operator's shorter outer scope. Annotating with
        # the measured elapsed lets the operator correlate against
        # their wrapper's budget.
        _start_monotonic = time.monotonic()
        try:
            async with asyncio.timeout(commit_budget):
                async with op_lock:
                    entered_lock = True
                    # Re-check under the lock: a concurrent close() may have
                    # acquired op_lock before us, closed the connection, and
                    # released. The ``_protocol is None`` check is repeated
                    # here so a sibling-task ``_invalidate`` racing the
                    # ``async with op_lock`` acquire (after our pre-lock
                    # fast-path passed) does not slip through into the
                    # ``in_transaction``-False short-circuit and silently
                    # mask partial-commit ambiguity.
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
                        # Parity with ``Connection._commit_async``;
                        # ``_call_client`` maps raw client errors onto
                        # PEP 249 ``Error`` subclasses.
                        request_in_flight = True
                        # Stdlib parity: the C-level
                        # ``sqlite3_busy_timeout`` callback fires on
                        # every SQL statement including COMMIT (which
                        # is just SQL to SQLite). Wrap with the
                        # SQLite-curve retry so a contended COMMIT
                        # survives the same way an INSERT does. The
                        # retry stays inside ``op_lock`` (fine — the
                        # wire is single-threaded per connection) and
                        # within the surrounding ``asyncio.timeout``
                        # (the commit budget bounds the entire retry
                        # loop, so the configured budget caps the
                        # cumulative work even if the SQLite curve
                        # itself would budget more).
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
                        # The wire round-trip completed (even if with
                        # a non-success FailureResponse the server
                        # acknowledged); state is well-defined.
                        request_in_flight = False
                        if _is_no_transaction_error(e):
                            return
                        # Leader flip mid-COMMIT: the Raft log entry
                        # MAY have been replicated before the flip,
                        # OR the flip may have occurred before append.
                        # Rewrap as ``AmbiguousCommitError`` so
                        # middleware that catches ``OperationalError``
                        # still receives the failure but cross-driver
                        # retry code can branch on the in-doubt shape
                        # via ``isinstance(exc, AmbiguousCommitError)``.
                        # Retrying non-idempotent DML against this case
                        # risks silent duplicate writes.
                        if e.code in _LEADER_ERROR_CODES:
                            raise AmbiguousCommitError(
                                "ambiguous commit: leader flipped during "
                                "COMMIT; the write may or may not have "
                                f"been persisted. Original: {e}",
                                code=e.code,
                                raw_message=getattr(e, "raw_message", None),
                            ) from e
                        raise
        except TimeoutError as e:
            # The budget covers both lock acquire and the COMMIT
            # round-trip. ``entered_lock`` distinguishes which phase
            # exhausted the budget so operators triaging the partial-
            # commit ambiguity see the right diagnostic:
            # - "op_lock acquire" when a sibling held the lock past the
            #   bound (programmer bug — two tasks on one connection),
            # - "COMMIT round-trip" when the lock was acquired but the
            #   wire RTT timed out (network bug — leader flip, server
            #   stall). Surface as ``OperationalError`` so SA's
            #   ``is_disconnect`` classifies the failure and the pool
            #   invalidates the slot. Connection state is ambiguous
            #   from the caller's perspective either way.
            if request_in_flight:
                self.force_close_transport()
            phase = "COMMIT round-trip" if entered_lock else "op_lock acquire"
            # Annotate the diagnostic with elapsed-vs-budget so the
            # operator can correlate against an outer scope's
            # ``asyncio.timeout`` budget. When ``elapsed`` is well
            # below ``commit_budget``, the inner deadline likely did
            # not actually expire — an outer cancel/timeout cut us
            # short and CPython's nested-Timeout ``__aexit__``
            # converted the CancelledError into our local
            # TimeoutError. The force-close above still fires for
            # the same partial-state hazard either way.
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
            # Outer-cancel / signal landed during the COMMIT round-
            # trip. The wire state is partial; force-close so SA's
            # is_disconnect classifier (substring scan on the next
            # op) invalidates the slot. Without the force-close, a
            # raw CancelledError doesn't trip is_disconnect and the
            # ambiguous-state connection stays in the pool.
            if request_in_flight:
                self.force_close_transport()
            raise

    async def rollback(self) -> None:
        """Roll back any pending transaction.

        Same no-op rules as :meth:`commit`, including the autocommit-
        by-default contract: "no active transaction" is the common
        case unless the caller issued an explicit ``BEGIN``.

        Outer-cancel caveat: same as :meth:`commit` — an outer
        ``asyncio.timeout`` deadline cancels this method without
        re-classifying to ``TimeoutError``, bypassing the
        phase-aware ``OperationalError`` diagnostic. Treat
        ``CancelledError`` as "invalidate the connection".
        """
        # Loop-affinity check before the messages-clear; see commit()
        # above for the foreign-loop / state-mutation rationale.
        self._check_loop_only()
        # PEP 249 §6.1.1 messages-clear contract; see commit() above.
        del self.messages[:]
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
        # Same invalidated-inner detection as commit() above; the
        # under-lock recheck repeats the ``_protocol is None`` test
        # so a sibling-task ``_invalidate`` racing the lock acquire
        # cannot slip through the ``in_transaction``-False
        # short-circuit.
        if getattr(self._async_conn, "_protocol", "_sentinel") is None:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}); reconnect before "
                "retrying commit / rollback."
            )
        _, op_lock = self._ensure_locks()
        # Bound the op_lock acquire by ``_SYNC_PHASES_MULTIPLIER *
        # self._timeout`` — same rationale as ``commit()``: avoid an
        # unbounded shutdown hang under partition +
        # ``trust_server_heartbeat=True`` while still absorbing the
        # multi-phase first-call-after-connect budget the sync sibling
        # tolerates.
        rollback_budget = _SYNC_PHASES_MULTIPLIER * self._timeout
        entered_lock = False
        # ``request_in_flight`` tracks whether the ROLLBACK wire round-
        # trip is in flight; see commit() for the partial-state +
        # force-close rationale.
        request_in_flight = False
        # Capture entry time so the TimeoutError arm can annotate
        # the diagnostic when both outer and inner deadlines fire
        # near-simultaneously; mirrors commit() above.
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
                    # Clear ``messages`` under the lock; see ``commit`` rationale.
                    del self.messages[:]
                    # Read ``in_transaction`` under the lock; see commit() for
                    # the rationale (avoid stale-True wasted ROLLBACK-RTT).
                    if not getattr(self._async_conn, "in_transaction", False):
                        return
                    try:
                        # Parity with ``Connection._rollback_async``;
                        # see ``commit``.
                        request_in_flight = True
                        await _call_client(self._async_conn.execute("ROLLBACK"))
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
            # Outer-cancel / signal landed during the ROLLBACK round-
            # trip. The wire state is partial; force-close so SA's
            # is_disconnect classifier invalidates the slot on the
            # next op. See commit() for full rationale.
            if request_in_flight:
                self.force_close_transport()
            raise

    @contextlib.asynccontextmanager
    async def transaction(self) -> "AsyncIterator[None]":
        """Async context manager wrapping ``BEGIN`` / ``COMMIT`` /
        ``ROLLBACK``.

        Mirrors ``asyncpg.Connection.transaction()`` and
        ``psycopg.AsyncConnection.transaction()`` — the canonical
        async-DB-API pattern that asyncpg / psycopg / SA ORM expect
        when a caller writes ``async with conn.transaction(): ...``.
        Without this method, ``async with conn.transaction()`` raised
        ``AttributeError`` outside the ``dbapi.Error`` hierarchy.

        Delegates to the underlying client-layer
        ``DqliteConnection.transaction()`` whose cancellation-aware
        rollback discipline is the source of truth for transaction
        semantics in this driver.

        Re-raises ``InterfaceError`` if the connection is closed.
        """
        # PEP 249 §6.4 messages-clear contract: every public method
        # clears messages "prior to executing the call". Sync sibling
        # at ``connection.py:transaction`` does this; every other
        # AsyncConnection entry point (connect/close/commit/rollback/
        # cursor/execute/executemany/setters) does this. The
        # transaction() ctxmgr is the lone deviation.
        del self.messages[:]
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Loop-binding check matches the sibling entry points
        # (``cursor`` / ``commit`` / ``rollback`` all go through
        # ``_ensure_locks`` whose first call binds the loop). On the
        # already-connected fast path, ``_ensure_connection`` short-
        # circuits BEFORE ``_ensure_locks`` runs — without this
        # explicit check, a caller who established the connection on
        # loop A and later does ``async with conn.transaction():`` on
        # loop B got the cross-loop diagnostic from the underlying
        # ``asyncio.Lock`` ("bound to a different event loop") rather
        # than the dbapi-level ``ProgrammingError`` shape produced by
        # every sibling entry point.
        self._check_loop_binding()
        # Materialise the underlying client connection if we haven't
        # yet — ``transaction()`` is an entry point on its own.
        async_conn = await self._ensure_connection()
        # Foreign-thread ``force_close_transport`` may have set
        # ``self._closed = True`` between ``_ensure_connection``'s
        # fast-path return and us. Without this re-check the
        # ``_transaction_owner`` slot below gets reserved against a
        # closed connection; the subsequent ``async_conn.transaction()``
        # raises against a dead inner and the slot remains pinned to
        # the now-dying task — sibling ``conn.transaction()`` calls
        # then see a misleading ``Nested conn.transaction() not
        # supported`` diagnostic rather than the truthful ``Connection
        # is closed`` shape. Mirrors the in-lock recheck discipline
        # already in place at ``commit()`` / ``rollback()``. Check
        # only ``_closed`` (not ``_async_conn is None``): legitimate
        # test fixtures patch ``_ensure_connection`` to return a
        # stand-in inner without updating ``_async_conn``, and the
        # load-bearing close-state observable is ``_closed``.
        if self._closed:
            raise InterfaceError(f"Connection closed during transaction setup (id={id(self)})")
        # Track the owning task while the body runs so explicit
        # ``await conn.commit()`` / ``await conn.rollback()`` calls
        # from the SAME task inside the body raise instead of
        # silently exiting the transaction. asyncpg / psycopg both
        # reject nested explicit transaction control; this driver
        # used to silently route the body's stray commit through to
        # the client, ending the transaction without exiting the
        # context manager — subsequent body statements then ran in
        # autocommit mode and the surrounding ``async with`` rollback-
        # at-exit no-op'd because in_transaction was already False.
        if self._transaction_owner is not None:
            raise InterfaceError(
                f"Nested conn.transaction() not supported (id={id(self)}); "
                "exit the outer block before opening a new one."
            )
        token = asyncio.current_task()
        # Set the owner slot INSIDE the try frame so a BaseException
        # (KeyboardInterrupt / SystemExit) at the bytecode boundary
        # between the assignment and the try-setup cannot leak the
        # slot pinned to a now-dying task. Mirrors the cursor's
        # ``_executing_task`` set-inside-try discipline.
        try:
            self._transaction_owner = token
            async with async_conn.transaction():
                yield
        finally:
            # Only clear if we still own the slot — a sibling task
            # that somehow re-entered (shouldn't happen given the
            # guard above) would otherwise have its slot wiped.
            if self._transaction_owner is token:
                self._transaction_owner = None

    def cursor(self, **unknown_kwargs: object) -> AsyncCursor:
        """Return a new AsyncCursor object.

        This is intentionally sync — SQLAlchemy calls cursor() from
        sync context within its greenlet-based async adapter. Loop
        binding is validated best-effort: if a different loop is
        running than the one this connection was first used on, raise
        ``ProgrammingError`` up front rather than letting the first
        await inside ``_ensure_locks`` surface the same error with a
        less specific diagnostic. No running loop (SA greenlet glue)
        is a valid case — skip the loop check.

        Fork-after-init guard: every other public method on this
        class (``_ensure_locks``, ``_check_loop_binding``, ``close``,
        ``force_close_transport``) compares ``_current_pid`` against
        ``self._creator_pid`` and raises if a fork has crossed the
        boundary. Without the same check here, a forked child calling
        ``aconn.cursor()`` from sync context (the SA-glue shape) would
        silently register a parent-pinned cursor in the child's
        ``self._cursors`` WeakSet and return a live wrapper. Match the
        sync sibling (``Connection.cursor`` enforces it via
        ``_check_thread``).

        **Porting note (aiosqlite)**: aiosqlite's
        ``Connection.cursor`` is ``async def`` — the standard
        aiosqlite pattern is ``cur = await conn.cursor()``. dqlite's
        ``AsyncConnection.cursor`` is sync (returns ``AsyncCursor``
        directly); ``await conn.cursor()`` raises
        ``TypeError: object AsyncCursor can't be used in 'await'
        expression``. Drop the ``await``::

            cur = conn.cursor()
            async with cur:
                ...
        """
        del self.messages[:]
        # Affinity precedence: closed → pid → loop → kwarg-shape.
        # Hoist the fork-after-init + loop-binding checks above the
        # ``unknown_kwargs`` rejection so a forked / cross-loop
        # caller using a bogus kwarg sees the canonical affinity
        # diagnostic (``InterfaceError("after fork")`` or
        # ``ProgrammingError("...different event loop...")``) rather
        # than ``NotSupportedError(unknown kwarg)``. Otherwise the
        # next no-kwarg call would still trip the affinity check and
        # the operator gets two different diagnostics for the same
        # underlying misuse. See sync sibling at
        # ``connection.py:Connection.cursor`` for the full rationale.
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
                # No running loop — SA greenlet glue calls cursor()
                # from sync context. Skip the check.
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
        # Re-check ``_closed`` after add: ``cursor()`` is sync-on-loop
        # but the surrounding ``close()`` cascade snapshots
        # ``list(self._cursors)`` so a freshly-added cursor that beat
        # the snapshot would skip the cascade and be returned to the
        # caller as a usable wrapper attached to a connection that is
        # mid-close. Defense-in-depth: if ``_closed`` flipped to True
        # between the AsyncCursor construction and the WeakSet add,
        # run the full scrub the cascade would have applied AND
        # discard the entry so ``self._cursors`` matches the
        # ``self._cursors.clear()`` postcondition rather than
        # retaining a stale (closed-but-late-added) entry. Without
        # ``discard()``, post-close diagnostics that read
        # ``len(conn._cursors)`` see a stale count, and any future
        # cascade field added (e.g. a buffer pointer) would silently
        # leak on the late-added cursor.
        if self._closed:
            cur._closed = True
            cur._rows = []
            cur._description = None
            cur._rowcount = -1
            cur._lastrowid = None
            cur._row_index = 0
            del cur.messages[:]
            # Mirror the cascade's ``weakref.proxy`` swap so a
            # race-leaked cursor does not strong-pin the closed
            # AsyncConnection's loop-bound state (lazy
            # ``asyncio.Lock`` / ``weakref.finalize`` / inner client
            # conn). Cascade canonical pattern at the
            # ``Cursor.close``-cascade loop earlier in this file; the
            # per-cursor close arm at ``aio/cursor.py`` follows the
            # same. ``contextlib.suppress(TypeError)`` for the rare
            # path where the connection object does not support
            # weakref (test fakes typed with ``object()``).
            with contextlib.suppress(TypeError):
                cur._connection = weakref.proxy(cur._connection)
            self._cursors.discard(cur)
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
        """``True`` once :meth:`close` has been called OR the inner
        client connection has been invalidated (cancel-mid-execute,
        leader-flip, transport reset).

        Peer-driver parity (psycopg, asyncpg) — both return True for
        invalidated connections so cross-driver code branching on
        ``conn.closed`` to drive reconnect heuristics works correctly.
        Without the OR-in, an invalidated AsyncConnection still
        reported ``closed == False`` while every operation surfaced
        ``InterfaceError("Not connected")`` — a common-case
        observability gap.

        Use :attr:`invalidated` if you specifically need to
        distinguish the two states.
        """
        return self._closed or self.invalidated

    @property
    def invalidated(self) -> bool:
        """``True`` if the inner client connection has been invalidated
        but :meth:`close` has not yet been called explicitly.

        Invalidation happens on cancel-mid-execute / leader-flip /
        transport reset. Returns ``False`` if the connection has been
        explicitly closed (``closed`` is the canonical signal then),
        if it was never connected, or if it is alive and well.

        asyncpg returns True from ``is_closed()`` for the same state;
        psycopg exposes ``connection.broken``. This driver splits the
        two: ``closed`` ORs both states (peer-driver parity);
        ``invalidated`` lets callers distinguish.
        """
        if self._closed:
            return False
        inner = self._async_conn
        if inner is None:
            return False
        return getattr(inner, "_protocol", "_sentinel") is None

    @property
    def row_factory(self) -> RowFactory | None:
        """stdlib ``sqlite3.Connection.row_factory`` parity hook.
        See sync sibling for full docs. New cursors inherit this
        default; per-cursor override via ``cur.row_factory = ...``."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        # Loop-affinity check: the sync sibling's ``row_factory.setter``
        # at ``connection.py:Connection.row_factory.setter`` calls
        # ``self._check_thread()`` to enforce the documented "every
        # state-mutating method enforces affinity" claim. The async
        # sibling needs the equivalent loop-binding check so a
        # foreign-loop caller cannot mutate ``_row_factory`` and
        # silently affect cursors spawned on the legitimate loop.
        # ``_check_loop_binding`` raises ``InterfaceError`` on closed
        # so the closed-state precedence holds without a separate
        # check. ``del self.messages[:]`` mirrors PEP 249 §6.4 +
        # project discipline. ``contextlib.suppress(AttributeError)``
        # tolerates ``__new__``-built fixtures that bypass ``__init__``.
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
        """stdlib ``sqlite3.Connection.text_factory``-parity stub.
        See sync sibling."""
        return str

    @text_factory.setter
    def text_factory(self, value: object) -> None:
        # PEP 249 §6.4 messages-clear; see ``autocommit.setter``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        # Loop-binding affinity contract — see ``autocommit.setter``.
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
        """Stdlib ``sqlite3.Connection`` / aiosqlite convenience extension
        (NOT part of PEP 249 §10 optional extensions) — open a cursor,
        run ``execute``, return the cursor.

        Mirrors stdlib ``sqlite3.Connection.execute`` and
        aiosqlite's ``Connection.execute``. Sync sibling
        ``Connection.execute`` already exists; this completes the
        async parity.

        On a synchronous failure of ``cur.execute(...)`` close the
        freshly-opened cursor before re-raising so the caller's
        exception path doesn't leak an unowned cursor with loop-
        bound state.

        **Porting note (aiosqlite chained-CM idiom is NOT supported)**:
        aiosqlite's ``Connection.execute`` returns a ``Result`` object
        that's awaitable AND an async context manager, allowing
        ``async with conn.execute(sql) as cur:``. dqlite's
        ``AsyncConnection.execute`` is a plain coroutine; the chained
        form raises ``TypeError`` ('coroutine' object does not support
        the asynchronous context manager protocol). Use::

            cur = await conn.execute(sql)
            async with cur:
                ...

        instead. Same applies to ``executemany``.
        """
        # PEP 249 §6.4 messages-clear contract; eager-clear mirrors
        # sync sibling ``Connection.execute``.
        # ``self.cursor()`` would clear it as a side-effect, but the
        # contract is explicitly "the next method call clears" — not
        # "the cursor created on the next line clears". The eager-
        # clear discipline also defends against future refactors that
        # move ``self.cursor()`` later in the body.
        del self.messages[:]
        cur = self.cursor()
        try:
            if parameters is None:
                await cur.execute(operation)
            else:
                await cur.execute(operation, parameters)
        except BaseException:
            # Widened suppress: ``cur.close()`` is sync (GIL-atomic
            # attribute writes + a weakref.proxy swap), but a
            # KeyboardInterrupt landing at a bytecode boundary inside
            # the close body could escape a narrower
            # ``suppress(Exception)`` and replace the original
            # ``BaseException`` we are about to ``raise``. Aligning
            # with the sibling ``aconnect()`` discipline at
            # ``aio/__init__.py:454`` keeps the original visible.
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
        """Stdlib ``sqlite3.Connection`` / aiosqlite convenience extension
        (NOT part of PEP 249 §10 optional extensions) — open a cursor,
        run ``executemany``, return the cursor. Mirrors stdlib
        ``sqlite3.Connection.executemany`` and aiosqlite's
        ``Connection.executemany``.

        **Porting note (aiosqlite chained-CM idiom is NOT supported)**:
        aiosqlite's ``Connection.executemany`` returns an awaitable
        async context manager, allowing
        ``async with conn.executemany(sql, seq) as cur:``. dqlite's
        ``AsyncConnection.executemany`` is a plain coroutine; the
        chained form raises ``TypeError``. Use::

            cur = await conn.executemany(sql, seq)
            async with cur:
                ...

        instead. Same applies to ``execute``.
        """
        # PEP 249 §6.4 messages-clear; see ``execute`` above.
        del self.messages[:]
        # Closed-state precedence: see sync sibling for rationale —
        # route through ``self.cursor()`` so a closed connection
        # raises ``InterfaceError`` BEFORE the outer-shape check.
        cur = self.cursor()
        # Reject the outer shapes that would silently iterate over keys
        # (dict) / characters (str / bytes / bytearray / memoryview), or
        # iterate in non-deterministic order (set / frozenset). The
        # shared ``_validate_executemany_seq_shape`` helper is the
        # single source of truth so this shortcut and
        # ``AsyncCursor.executemany`` produce one diagnostic and one
        # accept/reject contract. ``Mapping`` at large is NOT rejected
        # so OrderedDict-of-rows still works.
        try:
            _validate_executemany_seq_shape(seq_of_parameters)
        except ProgrammingError:
            # See ``execute`` above for the widened-suppress rationale.
            with contextlib.suppress(Exception, asyncio.CancelledError):
                cur.close()
            raise
        try:
            await cur.executemany(operation, seq_of_parameters)
        except BaseException:
            # See ``execute`` above for the widened-suppress rationale.
            with contextlib.suppress(Exception, asyncio.CancelledError):
                cur.close()
            raise
        return cur

    # PEP 249 §7 (TPC) and stdlib sqlite3 parity stubs. Without these
    # a caller hits AttributeError which escapes the dbapi.Error
    # hierarchy. Same shape as the sync sibling.
    #
    # Plain ``def`` (NOT ``async def``) so a forgotten ``await`` —
    # ``aconn.tpc_begin(xid)`` instead of ``await aconn.tpc_begin(xid)``
    # — surfaces the ``NotSupportedError`` immediately rather than
    # producing a discarded coroutine that warns "coroutine was never
    # awaited" at GC. Mirrors the ``executescript`` / ``backup`` stubs
    # which were converted from ``async def`` to ``def`` for the same
    # diagnostic reason.

    # ``*args, **kwargs`` shape so any caller signature reaches
    # ``_stub_unsupported``. See sync sibling for rationale.
    def tpc_begin(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_prepare(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_commit(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_rollback(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_recover(self, *args: object, **kwargs: object) -> NoReturn:
        # Annotated NoReturn (always raises). PEP 249 §7 specifies
        # tpc_recover() returns list[Xid] for an actual implementation
        # — feature-detection callers should use ``hasattr(conn,
        # "tpc_recover")`` plus a try/except rather than inspecting
        # the return annotation, since this stub will always raise.
        self._stub_unsupported("dqlite does not support two-phase commit")

    def xid(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite does not support two-phase commit")

    def _stub_unsupported(self, msg: str) -> NoReturn:
        """Shared helper for ``NotSupportedError`` stubs: clear
        ``self.messages`` per PEP 249 §6.4 messages-clear contract,
        check fork-after-init (canonical ``InterfaceError("after
        fork")``) and closed-state per stdlib precedence, then raise.
        Mirrors the sync sibling ``Connection._stub_unsupported``.

        Pid check runs BEFORE the closed-check so a forked child sees
        the canonical fork diagnostic (which routes through cross-
        driver retry middleware as ``InterfaceError``) instead of
        ``NotSupportedError`` — the latter is in the ``DatabaseError``
        subtree and is NOT caught by ``is_disconnect`` classifiers.
        Every other public method on this class runs the same pid
        guard up front; the stub family is the missing twin."""
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
        """stdlib ``sqlite3``-parity stub. dqlite has no
        multi-statement-script primitive on the wire (each statement
        requires a separate Prepare → Exec / Query round-trip); raises
        ``NotSupportedError`` rather than escaping ``dbapi.Error`` as
        ``AttributeError``. Mirrors the sync sibling.

        Defined as a plain ``def`` (not ``async def``) so the
        unconditional raise fires on the call line — symmetric with
        the sync sibling. An ``async def`` stub would defer the raise
        to ``await``, and a caller who forgot the ``await`` would
        observe a silent no-op with only a GC-time
        ``RuntimeWarning("coroutine was never awaited")``, defeating
        the diagnostic-leak prevention this stub family was added for.
        """
        self._stub_unsupported(
            "dqlite does not support stdlib sqlite3 executescript; "
            "split the script and execute each statement individually"
        )

    def interrupt(self) -> NoReturn:
        """stdlib ``sqlite3``-parity stub. See sync sibling."""
        self._stub_unsupported(
            "dqlite does not surface interrupt() at the dbapi layer; "
            "use asyncio.timeout(...) or rely on the per-RPC timeout"
        )

    # stdlib ``sqlite3.Connection``-parity stubs (see sync sibling
    # for full rationale). VDBE-callback / db-status / db-config /
    # serialize / blob-open primitives are not wire-supportable.

    def set_authorizer(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not expose a per-prepare authorization callback")

    def set_progress_handler(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not expose a VDBE progress callback")

    def set_trace_callback(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not expose a per-statement trace callback")

    def total_changes(self, *args: object, **kwargs: object) -> NoReturn:
        """dqlite-server does not surface a total_changes counter on
        the wire.

        Stdlib ``sqlite3.Connection.total_changes`` is an int-valued
        attribute. This driver exposes it as a callable stub
        (parens required) to keep the ``hasattr(aconn, "total_changes")``
        invariant that the rest of the stub family relies on —
        ``hasattr`` would propagate the ``NotSupportedError`` raised
        from a property-getter, breaking cross-driver feature-probe
        code. Pinned by tests/test_total_changes_hasattr_safe.py and
        tests/test_pep249_stub_hasattr_divergence.py.
        """
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
        # ``*args/**kwargs`` shape so any caller signature reaches
        # ``_stub_unsupported`` rather than leaking ``TypeError``
        # outside ``dqlitedbapi.Error``. See sync sibling.
        self._stub_unsupported("dqlite-server does not support runtime extension loading")

    def load_extension(self, *args: object, **kwargs: object) -> NoReturn:
        self._stub_unsupported("dqlite-server does not support runtime extension loading")

    def backup(self, *args: object, **kwargs: object) -> NoReturn:
        # Plain ``def`` (NOT ``async def``) so a forgotten ``await
        # aconn.backup(target)`` raises ``NotSupportedError`` on the
        # call line rather than producing a discarded coroutine that
        # warns "coroutine was never awaited" at GC. Mirrors the
        # discipline applied to ``executescript`` / ``interrupt`` /
        # ``tpc_*``: a non-async stub gives a sharp diagnostic at the
        # call site instead of a delayed "never awaited" warning.
        self._stub_unsupported(
            "dqlite does not support the stdlib sqlite3 online backup API; "
            "use the dqlite-server dump/restore mechanism instead"
        )

    def iterdump(self, *, filter: str | None = None, **kwargs: object) -> NoReturn:
        # Spell ``filter=`` explicitly so ``inspect.signature`` matches
        # the documented stdlib-3.13 shape ``(*, filter=None)``.
        # ``**kwargs`` still absorbs any future Python additions so
        # callers route through ``_stub_unsupported``. Stdlib's
        # ``iterdump`` is keyword-only (no positional args after
        # ``self``); we mirror that.
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
        # Sibling-discipline parity with ``DqliteConnection.__repr__`` and
        # the sync ``Connection.__repr__``: sanitize ``_address`` before
        # the ``!r`` interpolation so attacker-influenced bidi /
        # zero-width / line-separator codepoints render as ``?`` rather
        # than the cosmetically-different ``\uXXXX`` escape Python's
        # ``str.__repr__`` produces. Log-reader UX parity across layers.
        safe_addr = sanitize_for_log(str(self._address))
        return f"<AsyncConnection address={safe_addr!r} database={self._database!r} {state}>"

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

    async def __aenter__(self) -> Self:
        """Materialise the underlying connection and return self.

        .. note::

            **Asymmetric lifecycle.** ``__aenter__`` lazily calls
            :meth:`connect` on entry, but :meth:`__aexit__` performs
            commit / rollback only — it does **NOT** close the
            connection (matches stdlib ``sqlite3.Connection.__exit__``;
            diverges from ``aiosqlite`` and ``psycopg``, both of which
            close on exit). After ``async with`` exits, the underlying
            client connection is still alive: the loop-bound socket,
            ``op_lock``, and finalizer all persist until either an
            explicit ``await aconn.close()`` or GC drives the
            finalizer.

            Cross-driver porters from ``aiosqlite``::

                async with aiosqlite.connect("foo.db") as db:
                    await db.execute("...")
                # db is closed here

            must add an explicit close on the dqlite side::

                async with AsyncConnection("addr") as aconn:
                    await aconn.execute("...")
                await aconn.close()  # required — __aexit__ does not close

            Alternatively, hand ownership to a pool that manages the
            close-time. The asymmetry is documented in :meth:`__aexit__`
            with the stdlib-parity rationale.
        """
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
            # lock primitives. Log close-side errors at DEBUG so a
            # forensic trail exists for an operator triaging cleanup-
            # time failures.
            #
            # ``asyncio.shield`` lets the inner ``close()`` task finish
            # its drain even when the cancellation that triggered the
            # original ``connect()`` failure is the SAME inner cancel
            # chain — without the shield, ``close()`` would itself be
            # cancelled before draining and the original
            # ``CancelledError`` context would be lost mid-flight.
            # ``contextlib.suppress(asyncio.CancelledError)`` absorbs a
            # FRESH outer cancel landing during this cleanup so the
            # bare ``raise`` below re-delivers the ORIGINAL connect-
            # time exception (asyncio re-raises the cancel at the next
            # await on this task). Sibling ``aconnect()`` at
            # ``aio/__init__.py:454`` uses the same discipline; without
            # this suppress the two entry points behaved differently
            # on outer-cancel-during-cleanup (``aconnect()`` re-raised
            # the original; ``__aenter__`` re-raised the cancel).
            # Schedule the cleanup-close as a Task with an explicit
            # ``_observe_drain_exception`` done-callback so the
            # implicit Task ``asyncio.shield(coro)`` would otherwise
            # create is not orphaned. See sibling pattern at
            # ``connect()`` line 564-573 / __init__.py.
            from dqliteclient.cluster import _observe_drain_exception

            inner_drain = asyncio.ensure_future(self.close())
            inner_drain.add_done_callback(_observe_drain_exception)
            try:
                with contextlib.suppress(asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                    # Absorb KeyboardInterrupt / SystemExit during the
                    # cleanup-close too. The function's outer
                    # ``except BaseException`` catch preserves the
                    # original connect-time exception across cleanup;
                    # a KI/SE delivered inside the shielded close (or
                    # its done-callback chain) would otherwise
                    # propagate and SUPPLANT the saved original via
                    # Python's implicit-context machinery -- defeating
                    # the "preserve original" invariant for the two
                    # signal types that fell outside the narrower
                    # suppress.
                    await asyncio.shield(inner_drain)
            except Exception:
                logger.debug(
                    "AsyncConnection.__aenter__ (id=%s, address=%s): "
                    "exception during cleanup-close after failed connect",
                    id(self),
                    sanitize_for_log(str(self._address)),
                    exc_info=True,
                )
            # Defensive null-out of the lazy loop-bound primitives.
            # ``close()`` may have short-circuited at its TOP-of-method
            # ``if self._closed: return`` guard because a concurrent
            # close (foreign-thread ``force_close_transport``, or a
            # sibling task's ``close()`` raced inside a TaskGroup)
            # flipped ``_closed=True`` between ``_ensure_locks`` and
            # the cleanup-close. The short-circuit skips the
            # never-connected branch (``:806-820``) that would
            # otherwise null these slots. Without this defensive
            # cleanup, a subsequent ``AsyncConnection`` reuse on a
            # different loop (test fixtures, SA pool recycling an
            # instance) sees ``_connect_lock is not None`` in
            # ``_ensure_locks`` and raises a misleading "bound to a
            # different event loop" diagnostic instead of the
            # documented fresh-connect path. Idempotent with the
            # nulling already performed inside ``close()`` on the
            # non-short-circuit paths.
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
        """Commit on clean exit, rollback on exception, do NOT close.

        Matches stdlib ``sqlite3.Connection`` parity: ``__aexit__``
        finishes the transaction (commit on clean exit, rollback on
        exception) but leaves the underlying connection open so the
        same instance is reusable in a subsequent ``async with`` block.
        This diverges from ``aiosqlite.Connection.__aexit__`` (which
        closes the connection) — see the README "Differences from
        aiosqlite" section for the rationale.

        Both arms tolerate cancel/signal during commit-or-rollback:
        the server-side state may be ambiguous if the request reached
        the leader before the cancel landed, but the cancel still
        propagates faithfully to the caller (with a DEBUG breadcrumb
        for operator forensics).
        """
        if self._closed or self._async_conn is None:
            # Nothing ever ran (``_async_conn is None``), OR a foreign
            # thread closed mid-``async with`` block via
            # ``force_close_transport`` (publicly documented entry
            # point — SA pool reclaim threads, signal handlers).
            # Without the ``_closed`` arm the subsequent
            # ``await self.commit()`` raises ``InterfaceError`` from
            # the closed-state guard and supplants the body exception
            # (or the clean exit's success) with a closed-state
            # misuse error.
            return
        if exc_type is None:
            try:
                await self.commit()
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                # Same partial-commit hazard the rollback arm flags:
                # the COMMIT may have reached the leader before the
                # cancel landed, leaving server-side state ambiguous.
                # Log a breadcrumb at DEBUG so an operator triaging a
                # dangling server-side transaction has a forensic
                # trail; then re-raise so the cancel propagates
                # faithfully. Symmetric with the rollback arm below.
                logger.debug(
                    "AsyncConnection.__aexit__ (address=%s, id=%s): "
                    "commit interrupted by cancel/signal; "
                    "server-side commit state may be ambiguous",
                    sanitize_for_log(str(self._address)),
                    id(self),
                    exc_info=True,
                )
                # SA's ``is_disconnect`` classifier does NOT match
                # ``CancelledError`` / ``KeyboardInterrupt`` /
                # ``SystemExit`` — the slot would otherwise be returned
                # to the pool with ambiguous server-side commit state
                # AND an inner ``commit()`` arm that only force-closes
                # when ``request_in_flight=True`` (the wire round-trip
                # began). Cancels landing before / after that window
                # leave the slot alive. ``force_close_transport()`` is
                # synchronous, idempotent, never raises — call it after
                # the DEBUG log and before the re-raise so the next
                # checkout's first op sees a closed transport and the
                # pool reaps the slot.
                self.force_close_transport()
                raise
        else:
            try:
                await self.rollback()
            except (asyncio.CancelledError, KeyboardInterrupt, SystemExit):
                # Cancel / signal interrupted the rollback. Mirror the
                # client-layer ``transaction()`` ctxmgr's discipline:
                # log a breadcrumb so the audit trail is consistent
                # across layers, then re-raise so the cancel signal
                # supersedes the body exception.
                #
                # **Policy: structured-concurrency cancel-wins.** This
                # arm intentionally re-raises the cancel signal even
                # when the body raised a transport/integrity exception.
                # PEP 343 semantics make ``raise`` here supplant the
                # body exception (which survives only on ``__context__``).
                # SQLAlchemy's ``is_disconnect`` classifier walks
                # ``__cause__`` only (not ``__context__``) so a
                # cancel-during-rollback after a transport-class body
                # exception is classified by SA as non-DBAPI and the
                # slot is NOT invalidated. Operators relying on the
                # body exception class to reach a disconnect classifier
                # must observe it through ``except BaseException``
                # in the surrounding scope and consult ``__context__``
                # explicitly, OR use ``asyncio.timeout(...)`` which
                # translates the cancel to a ``TimeoutError`` (also
                # not a DBAPI class but at least observable). The
                # alternative — preserving the body exception and
                # making the cancel observable via ``Task.cancelling()``
                # — would diverge from the project-wide structured-
                # concurrency posture used in the SA adapter and the
                # commit arm above. Pin: ``test_aexit_rollback_debug_log
                # ::test_aexit_rollback_cancelled_error_propagates``.
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
                # The body already raised; we cannot re-raise, but a
                # silent suppress leaves no breadcrumb for an operator
                # debugging a dangling server-side transaction
                # (leader flip mid-commit, socket timeout, etc.).
                logger.debug(
                    "AsyncConnection.__aexit__ (address=%s, id=%s): "
                    "rollback failed after body raised %s",
                    sanitize_for_log(str(self._address)),
                    id(self),
                    exc_type.__name__,
                    exc_info=True,
                )
                # Rollback failed; the server-side transaction may
                # still be open. SA's ``is_disconnect`` walks
                # ``__cause__`` and we are NOT raising — PEP 343
                # returning None lets the body exception propagate
                # but SA classifies against the body, not the
                # rollback failure. Force-close the transport so the
                # next checkout's first op sees a closed transport
                # and the pool reaps the slot, preventing statements
                # from running inside the orphaned transaction.
                # ``force_close_transport`` is synchronous,
                # idempotent, and never raises.
                self.force_close_transport()
        # Do NOT close — matches stdlib ``sqlite3.Connection.__exit__``.
        # NOTE: aiosqlite's ``Connection.__aexit__`` DOES call
        # ``self.close()`` and psycopg async ``Connection.__aexit__``
        # also closes; this driver follows stdlib's per-connection
        # semantics rather than the close-on-exit shape used by
        # aiosqlite/psycopg.
        #
        # See also the operational caveat on ``commit()``: a leader
        # flip mid-COMMIT can surface as ``OperationalError`` from
        # this method on a clean-exit ``async with conn:``.
        #
        # Callers who want eager close use ``conn.close()`` explicitly
        # or go through a pool.


# Parity with sync ``Connection.Cursor``: expose ``AsyncCursor`` so
# cross-driver adapter / instrumentation code can isinstance-check the
# cursor type without importing ``dqlitedbapi.aio.cursor``. Assigned
# outside the class body to avoid shadowing the ``AsyncCursor`` type
# name in method annotations within the class scope. Not a factory
# hook — ``cursor()`` still instantiates ``AsyncCursor`` directly.
#
# Both attribute names are exposed:
# - ``AsyncCursor`` mirrors the explicit type name (load-bearing for
#   code that introspects under that name).
# - ``Cursor`` mirrors the aiosqlite convention
#   (``aiosqlite.Connection.Cursor`` exposes the async cursor under
#   the bare ``Cursor`` name) so cross-driver adapter code targeting
#   the aiosqlite-shape async dbapi can write
#   ``isinstance(cur, conn.Cursor)`` against either driver. The sync
#   sibling at ``connection.py`` carries the same ``Cursor`` alias —
#   exposing the async surface here under the same name honours the
#   "Parity with sync ``Connection.Cursor``" comment above literally.
AsyncConnection.AsyncCursor = AsyncCursor  # type: ignore[attr-defined]
AsyncConnection.Cursor = AsyncCursor  # type: ignore[attr-defined]
