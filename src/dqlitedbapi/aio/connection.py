"""Async connection implementation for dqlite."""

import asyncio
import contextlib
import logging
import weakref
from types import TracebackType

from dqliteclient import DqliteConnection
from dqliteclient.connection import _parse_address as _client_parse_address
from dqliteclient.protocol import _validate_positive_int_or_none
from dqlitedbapi import exceptions as _exc
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.connection import (
    _build_and_connect,
    _is_no_transaction_error,
    _validate_close_timeout,
    _validate_timeout,
)
from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import InterfaceError, OperationalError, ProgrammingError

__all__ = ["AsyncConnection"]

logger = logging.getLogger(__name__)


class AsyncConnection:
    """Async database connection, loop-bound.

    Binds to the first asyncio event loop on which any method runs.
    Subsequent calls from a different loop raise ``ProgrammingError``;
    instances are NOT reusable across ``asyncio.run()`` invocations or
    across threads with their own loops.

    Safe for concurrent tasks on the SAME loop: the internal
    ``_op_lock`` serialises in-flight operations so commit/execute/
    rollback cannot interleave.
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
        max_total_rows: int | None = 10_000_000,
        max_continuation_frames: int | None = 100_000,
        trust_server_heartbeat: bool = False,
        close_timeout: float = 0.5,
    ) -> None:
        """Initialize connection (does not connect yet).

        Args:
            address: Node address in "host:port" format
            database: Database name to open
            timeout: Connection timeout in seconds (positive, finite)
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
        self._max_total_rows = _validate_positive_int_or_none(max_total_rows, "max_total_rows")
        self._max_continuation_frames = _validate_positive_int_or_none(
            max_continuation_frames, "max_continuation_frames"
        )
        self._trust_server_heartbeat = trust_server_heartbeat
        self._close_timeout = close_timeout
        self._async_conn: DqliteConnection | None = None
        self._closed = False
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
        loop = asyncio.get_running_loop()
        if self._connect_lock is None:
            self._loop_ref = weakref.ref(loop)
            self._connect_lock = asyncio.Lock()
            self._op_lock = asyncio.Lock()
        else:
            bound = self._loop_ref() if self._loop_ref is not None else None
            if bound is not loop:
                raise ProgrammingError(
                    "AsyncConnection was first used on a different event loop; "
                    "AsyncConnection instances are loop-bound and cannot be "
                    "reused across asyncio.run() invocations."
                )
        # ``_op_lock`` is created together with ``_connect_lock`` above;
        # the assertion keeps mypy narrow without a runtime cost.
        assert self._op_lock is not None
        return self._connect_lock, self._op_lock

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
        # Set _closed first so any task waiting on the lock sees the
        # closed state as soon as it acquires. Then drain the current
        # in-flight op (if any) under the lock.
        self._closed = True
        # Cascade to tracked cursors before the teardown drains the
        # wire so buffered fetches stop answering from stale rows.
        # Writes go directly to the cursor's private attributes; the
        # async Cursor's own close() path re-enters op_lock, which
        # would deadlock against the _op_lock acquire below.
        for cur in list(self._cursors):
            cur._closed = True
            cur._rows = []
            cur._description = None
            cur._rowcount = -1
            cur._lastrowid = None
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
        async with op_lock:
            if self._async_conn is not None:
                await self._async_conn.close()
                self._async_conn = None
        # Reset the locks *after* closing so any task that was parked on
        # ``op_lock`` observes the "_closed -> raise InterfaceError"
        # re-check before it touches the now-None primitive.
        self._connect_lock = None
        self._op_lock = None
        self._loop_ref = None

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

    async def commit(self) -> None:
        """Commit any pending transaction.

        Silent no-op if the connection has never been used (preserves
        the existing "no spurious connect" contract) or if the server
        reports "no transaction is active" (matches stdlib sqlite3).

        Operational caveat: on a leader flip mid-transaction, COMMIT
        can raise ``OperationalError`` with a leader-change code. The
        write may or may not have been persisted — callers should use
        idempotent DML or out-of-band state-checks before retrying.
        Same caveat applies to ``__aexit__``'s clean-exit commit.
        """
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
            try:
                # Parity with ``Connection._commit_async``; ``_call_client``
                # maps raw client errors onto PEP 249 ``Error`` subclasses.
                await _call_client(self._async_conn.execute("COMMIT"))
            except OperationalError as e:
                if not _is_no_transaction_error(e):
                    raise

    async def rollback(self) -> None:
        """Roll back any pending transaction. Same no-op rules as commit."""
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
                        "AsyncConnection.cursor() called from a different "
                        "event loop; AsyncConnection instances are loop-bound."
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

    def __repr__(self) -> str:
        state = "closed" if self._closed else ("connected" if self._async_conn else "unused")
        return f"<AsyncConnection address={self._address!r} database={self._database!r} {state}>"

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
