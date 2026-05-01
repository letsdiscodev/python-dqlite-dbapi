"""PEP 249 Connection implementation for dqlite."""

import asyncio
import concurrent.futures
import contextlib
import logging
import math
import os
import threading
import warnings
import weakref
from collections.abc import Coroutine, Iterable, Sequence
from types import TracebackType
from typing import Any, Final, NoReturn, Self

import dqliteclient.exceptions as _client_exc
from dqliteclient import DqliteConnection, validate_positive_int_or_none
from dqliteclient import connection as _client_conn_mod
from dqliteclient.connection import parse_address as _client_parse_address
from dqlitedbapi import exceptions as _exc
from dqlitedbapi.cursor import Cursor, _call_client
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
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
from dqlitewire import NO_TRANSACTION_MESSAGE_SUBSTRINGS
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
#
# We also deliberately do NOT include code=0. Upstream emits
# ``failure(req, 0, "empty statement")`` from
# ``gateway.c::handle_prepare_done_cb`` when the SQL parses to no
# statement (empty / comment-only / whitespace-only). The wire layer
# accepts ``code=0`` as a legal ``FailureResponse`` (see the wire-side
# ``code=0`` round-trip pin), and the dbapi must surface that as a
# normal ``OperationalError`` rather than silently swallow it at the
# commit/rollback boundary — masking it would hide a real diagnostic
# from callers who issued an empty COMMIT/ROLLBACK by accident.
_NO_TX_CODES: Final[frozenset[int]] = frozenset({1})
# Substrings that mark a benign "no transaction was active" reply.
# Imported from ``dqlitewire`` so the dbapi recogniser and the
# client-layer ``_is_no_tx_rollback_error`` share one source of
# truth — a wording drift in the server (or in the embedded SQLite
# version) that drops one of these clauses cannot produce silent
# layer divergence (client suppressing while the dbapi raises).
_NO_TX_SUBSTRINGS: Final[tuple[str, ...]] = NO_TRANSACTION_MESSAGE_SUBSTRINGS

# Bound (in seconds) for joining the background event-loop thread on
# teardown. Worst-case scenario: a coroutine queued on the loop is
# mid-await on a wire call when shutdown is requested. The loop is
# asked to stop; the in-flight task resolves to CancelledError; the
# thread then joins. 5 seconds covers the slow-network worst case
# while preventing process hang on close. Both the finalizer
# (``_cleanup_loop_thread``) and ``Connection.close()`` use this
# bound — keep them in step via the constant.
_LOOP_THREAD_JOIN_TIMEOUT_SECONDS: Final[float] = 5.0


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
    ``validate_positive_int_or_none`` in the client layer.
    """
    if isinstance(timeout, bool):
        raise ProgrammingError(f"timeout must be a positive finite number, got {timeout!r} (bool)")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ProgrammingError(f"timeout must be a positive finite number, got {timeout}")


def _wrap_positive_int(value: int | None, name: str) -> int | None:
    """Wrap the client-layer ``validate_positive_int_or_none``'s
    ``TypeError`` / ``ValueError`` into PEP 249 ``ProgrammingError``.

    PEP 249 §7 requires every error originating from the driver to be
    a subclass of ``Error``. The client-layer validator deliberately
    raises Python-convention exceptions — correct for client-only
    consumers; the dbapi entry points are the boundary
    that translates to PEP 249 shapes. Sibling pattern to the
    ``_client_parse_address`` ``ValueError → InterfaceError`` wrap and
    to ``_validate_timeout``'s direct ``ProgrammingError``.
    """
    try:
        return validate_positive_int_or_none(value, name)
    except (TypeError, ValueError) as e:
        raise ProgrammingError(str(e)) from e


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
        #
        # Route through the same primary-code classifier the cursor
        # path uses so connect-time CORRUPT / NOTADB / FORMAT etc.
        # surface as the right PEP 249 subclass instead of a bare
        # OperationalError. Without this, an operator pointing dqlite
        # at a non-database file sees `OperationalError("Failed to
        # connect: ...")` instead of the more diagnostic
        # `DatabaseError`.
        from dqlitedbapi.cursor import _classify_operational

        exc_cls = _classify_operational(e.code)
        # Preserve the un-modified server text on raw_message so
        # callers reading the un-truncated diagnostic see exactly
        # what the server emitted. The "Failed to connect: " prefix
        # belongs on the user-facing ``message`` only — prefixing
        # raw_message would contaminate the "verbatim server text"
        # contract.
        if issubclass(exc_cls, DatabaseError) or issubclass(exc_cls, InterfaceError):
            raise exc_cls(
                f"Failed to connect: {e.message}",
                code=e.code,
                raw_message=e.raw_message,
            ) from e
        # Fallback if a future class lands outside both umbrellas.
        raise OperationalError(
            f"Failed to connect: {e.message}",
            code=e.code,
            raw_message=e.raw_message,
        ) from e
    except _client_exc.ClusterPolicyError as e:
        # Deterministic configuration mismatch. Route through
        # ``InterfaceError`` with a distinguishing ``"Cluster policy
        # rejection;"`` prefix so callers can branch on the message
        # without importing client-layer types. SA's ``is_disconnect``
        # narrows ``InterfaceError`` matching to "connection is
        # closed" / "cursor is closed", so the pool does NOT enter a
        # retry loop against the permanent policy rejection — matches
        # the ``_call_client`` query-path wrap. Plumb code=None /
        # raw_message symmetric with the seven sibling per-class
        # arms below; the prefix is on ``message`` only, leaving
        # ``raw_message`` as the verbatim server text.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(
            f"Cluster policy rejection; {e}",
            code=None,
            raw_message=raw_msg,
        ) from e
    except _client_exc.DqliteConnectionError as e:
        # Transport / handshake failure at the connect layer (TCP
        # refused, DNS failure, server-reset). The cursor-path
        # classifier maps DqliteConnectionError to OperationalError;
        # mirror it on the connect path so SA's pool retry loop sees
        # the right shape and the substring scan can classify it.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(f"Failed to connect: {e}", code=None, raw_message=raw_msg) from e
    except _client_exc.ClusterError as e:
        # Non-policy ClusterError — transient at the cluster discovery
        # layer (no leader yet, all nodes unreachable). Surface as
        # OperationalError so the SA pool's retry loop classifies it
        # correctly, with raw_message preserved.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(f"Failed to connect: {e}", code=None, raw_message=raw_msg) from e
    except _client_exc.ProtocolError as e:
        # Wire-level desync during handshake (very rare). Match the
        # cursor-path classifier's wording so SA's substring scan
        # sees the canonical "wire decode failed" prefix.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(f"wire decode failed: {e}", code=None, raw_message=raw_msg) from e
    except _client_exc.DataError as e:
        # Encode-side error during the open handshake (e.g. a binary
        # database name that fails encode_text). Surface as DataError
        # per PEP 249 §7 — symmetric with the cursor-path classifier.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise DataError(str(e), code=None, raw_message=raw_msg) from e
    except _client_exc.InterfaceError as e:
        # Driver-misuse on the connect path (e.g. cross-loop reuse of
        # an inner DqliteConnection). Surface as InterfaceError per
        # PEP 249 — symmetric with the cursor-path classifier.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(str(e), code=None, raw_message=raw_msg) from e
    except _client_exc.DqliteError as e:
        # Catch-all for any future DqliteError subclass not enumerated
        # above. Surface as InterfaceError so an unexpected error
        # type stays inside PEP 249's Error hierarchy. Mirrors the
        # cursor-path classifier's catch-all at the end of
        # ``_call_client``.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(
            f"unrecognized client error ({type(e).__name__}): {e}",
            code=None,
            raw_message=raw_msg,
        ) from e
    except OSError as e:
        # Transport-level error escaping the client's wrap discipline
        # (e.g. an asyncio cancellation that bypassed the inner
        # try/except, or a refactor regression that newly leaks
        # ConnectionResetError past the client layer). PEP 249 §7
        # requires database-sourced failures to surface as Error
        # subclasses; OperationalError is the right shape for
        # transport.
        raise OperationalError(f"Failed to connect: {e}", code=None, raw_message=str(e)) from e
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

    Substring guard rationale: ``DQLITE_ERROR = 1`` (defined in
    ``dqlite-upstream/include/dqlite.h``) shares the wire low-byte
    with ``SQLITE_ERROR = 1``. Upstream emits ``DQLITE_ERROR`` from
    ``gateway.c::handle_request_transfer`` ("leadership transfer
    failed") on the ``REQUEST_TRANSFER`` path. The Python client
    does not invoke that request type today, so the collision is
    latent — but the substring filter is the only thing standing
    between the latent dqlite-namespace code-1 emission and a
    silent swallow by ``commit()`` / ``rollback()``. Drop the
    filter only if the wire layer gains a namespace-discriminator
    byte upstream.
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
    # Match against the un-truncated server text (raw_message) rather
    # than ``str(exc)`` (truncated). A long server message that has
    # the no-tx clause beyond the truncation cap would otherwise miss
    # the substring and surface the no-tx as a real error.
    raw = getattr(exc, "raw_message", None) or str(exc)
    lowered = raw.lower()
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
            # call. Log at debug so the swallow is observable for
            # operators triaging finalize-time anomalies; the
            # ``pragma: no cover`` stays because the path is genuinely
            # racy and not reproducible in tests.
            logger.debug(
                "Connection._cleanup_loop_thread: loop.call_soon_threadsafe "
                "raised RuntimeError (loop likely closed mid-call)",
                exc_info=True,
            )
        with contextlib.suppress(RuntimeError):
            thread.join(timeout=_LOOP_THREAD_JOIN_TIMEOUT_SECONDS)
        try:
            if not loop.is_closed():
                loop.close()
        except RuntimeError:  # pragma: no cover - race: loop restarted mid-finalize
            # Raised if the loop was somehow restarted mid-finalization.
            # Same operator-visibility rationale as above.
            logger.debug(
                "Connection._cleanup_loop_thread: loop.close() raised "
                "RuntimeError (loop likely restarted mid-finalize)",
                exc_info=True,
            )


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
            timeout: Per-RPC-phase timeout in seconds (must be positive
                and finite; validated here so direct ``Connection(...)``
                calls don't silently accept bad values that later
                produce hangs or stranger downstream errors). Each phase
                of an operation (send, read, any continuation drain)
                gets the full budget independently — a single call can
                take up to roughly N × ``timeout`` end-to-end. Wrap
                callers in ``asyncio.timeout(...)`` to enforce a
                wall-clock deadline.
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
        self._max_total_rows = _wrap_positive_int(max_total_rows, "max_total_rows")
        self._max_continuation_frames = _wrap_positive_int(
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
        # ``threading.get_ident()`` returns the OS pthread tid which on
        # Linux/macOS may match across fork (the child's main thread
        # tid usually equals the pid). Fork-after-init is unsupported:
        # the inherited TCP socket would be shared with the parent and
        # writes would interleave on the wire, the inherited daemon
        # loop thread does not survive fork, and asyncio primitives
        # bound to the parent's loop are unusable in the child. Store
        # the creator pid so cross-fork use raises a clear
        # ``InterfaceError`` from any public method, instead of silent
        # corruption. Symmetric with the pickle / copy / deepcopy
        # guards on this class.
        self._creator_pid = os.getpid()
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
        """Raise on cross-process (fork) or cross-thread misuse.

        - InterfaceError if called from a forked child (pid mismatch).
        - ProgrammingError if called from a different thread than the creator.
        """
        if _client_conn_mod._current_pid != self._creator_pid:
            raise InterfaceError(
                "Connection used after fork; reconstruct from configuration in the target process."
            )
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
            # If KI/SystemExit landed in the bytecode-narrow gap
            # between ``acquire(timeout=...)`` returning True and
            # ``acquired = ...`` STORE_FAST executing, the lock IS
            # held but the local ``acquired`` is unbound — the outer
            # try/finally below would then skip the release and
            # permanently leak the lock. Best-effort release here:
            # ``threading.Lock.release()`` raises RuntimeError when
            # the lock is unlocked, so a suppress makes the call
            # safe in the more-common "KI landed before acquire
            # could complete" case too.
            with contextlib.suppress(RuntimeError):
                self._op_lock.release()
            # The coroutine was never scheduled on the loop, so close
            # it explicitly to suppress "coroutine was never awaited"
            # ResourceWarnings (and free its frame).
            coro.close()
            # Mirror the post-acquire KI arm's synchronous null-out
            # discipline. The loop-thread coroutine for the prior
            # in-flight op holds ``_in_use=True`` and is parked on a
            # slow ``reader.read()``; the queued
            # ``call_soon_threadsafe(_invalidate)`` only lands when
            # the read yields (potentially up to the read deadline
            # away). Without the synchronous null-out, a retry from
            # the signal handler reads a stale non-None
            # ``self._async_conn``, hits ``_check_in_use``, and
            # raises "another operation is in progress" — wedging
            # the connection until the prior coroutine drains.
            #
            # ``self._async_conn = None`` is a single STORE_ATTR
            # (GIL-atomic on CPython); the loop-thread coroutine
            # holds its own local reference to the dying conn and
            # will reap its transport via the scheduled
            # ``_invalidate`` below.
            #
            # Gated on ``_in_use`` (preserved from the original
            # code) so a KI raised during a quiet acquire (no prior
            # op) does not invalidate gratuitously.
            dying = self._async_conn
            if dying is not None and self._loop is not None and dying._in_use:
                self._async_conn = None
                with contextlib.suppress(RuntimeError):
                    self._loop.call_soon_threadsafe(
                        dying._invalidate,
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
                # Synchronously null ``self._async_conn`` from the
                # calling thread, mirroring the
                # ``(KeyboardInterrupt, SystemExit)`` arm below. Same
                # rationale: a slow ``reader.read()`` parked on the
                # loop has not yet reached a scheduling checkpoint,
                # so the ``call_soon_threadsafe(_invalidate)`` we
                # queue next will only land when that read yields
                # (potentially up to the read deadline away).
                # Without the synchronous null-out, the caller's
                # retry hits ``_check_in_use`` against the still-
                # latched ``_in_use=True`` on the dying conn and
                # raises "another operation is in progress" until
                # the slow read finally drains.
                #
                # ``self._async_conn = None`` is a single STORE_ATTR
                # (GIL-atomic on CPython); the loop-thread coroutine
                # holds its own local reference to the dying conn and
                # will reap its own transport via the scheduled
                # ``_invalidate`` below.
                #
                # The null-out is placed AFTER the
                # ``recovered_error`` race-recovery branch above so a
                # coroutine that actually completed (success or
                # late server-side exception) does not get its
                # connection state torn out from under it — the
                # recovery branch returns / raises directly without
                # falling through to here.
                dying = self._async_conn
                self._async_conn = None
                # Poison the underlying connection. The coroutine may have
                # half-written a request; the wire is in unknown state.
                # Fire-and-forget on the loop thread (don't await).
                if dying is not None:
                    # RuntimeError if the loop is already shutting down.
                    with contextlib.suppress(
                        RuntimeError
                    ):  # pragma: no cover - race: loop closing mid-schedule
                        loop.call_soon_threadsafe(
                            dying._invalidate,
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
                    # Operation actually completed on the server (the
                    # race-recovery branch above caught a real
                    # exception from ``future.result(timeout=0)``).
                    # Re-raise it directly instead of wrapping in
                    # ``OperationalError("timed out")``: the
                    # type-of-truth is the recovered exception, and a
                    # wrap here misleads caller-side type-based
                    # dispatch (``except IntegrityError:`` no longer
                    # matches a constraint violation; ``except
                    # OperationalError:`` triggers a redundant retry
                    # against an autocommit DML that already
                    # persisted server-side, double-writing).
                    #
                    # The original ``TimeoutError`` is still reachable
                    # via ``__context__`` — Python sets it
                    # automatically when raising inside an except —
                    # so callers that need the timeout signal for
                    # diagnostics can walk the chain. The trade-off
                    # against the legacy "sync-timeout always
                    # surfaces as OperationalError" contract is
                    # deliberate: faithful exception class wins over
                    # contract preservation.
                    #
                    # ``noqa: B904`` — bare ``raise recovered_error``
                    # is intentional. ``from e`` would force-set
                    # ``__cause__`` to the TimeoutError, mis-stating
                    # causality (the server-side error wasn't caused
                    # by the calling-thread timer); ``from None``
                    # would suppress the chain entirely, hiding the
                    # timeout signal that callers may need.
                    raise recovered_error  # noqa: B904
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
                # Synchronously null ``self._async_conn`` from the
                # calling thread so the next sync op gets a fresh-
                # connect path regardless of whether the loop thread
                # has drained yet. ``self._async_conn = None`` is a
                # single STORE_ATTR (GIL-atomic on CPython); the
                # still-running loop-thread coroutine holds its own
                # local reference to the dying conn and will reap its
                # own transport via the scheduled ``_invalidate``.
                # Without this null-out, a slow read on the loop can
                # keep ``_in_use=True`` for up to the read deadline,
                # wedging the next sync op with "another operation
                # is in progress" until the old coroutine yields.
                dying = self._async_conn
                self._async_conn = None
                if dying is not None:
                    with contextlib.suppress(RuntimeError):
                        loop.call_soon_threadsafe(
                            dying._invalidate,
                            InterfaceError("operation interrupted"),
                        )
                # Narrow suppress: a SECOND KI/SystemExit landing
                # inside the 1-second bounded wait must propagate so
                # the user's Ctrl-C escalation reaches the process.
                # The original (first) KI is still re-raised by the
                # trailing ``raise``. CancelledError/TimeoutError
                # are absorbed (cancellation acknowledged); other
                # ``Exception`` from the cancelled coroutine is
                # DEBUG-logged so a programming bug in cleanup is
                # observable. Mirrors the timeout arm's narrow shape.
                try:
                    future.result(timeout=1.0)
                except (
                    concurrent.futures.CancelledError,
                    concurrent.futures.TimeoutError,
                ):
                    pass
                except Exception:
                    logger.debug(
                        "sync KI/SystemExit cleanup: unexpected error during bounded cancel-wait",
                        exc_info=True,
                    )
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
            raise InterfaceError(f"Connection is closed (id={id(self)})")

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
        # PEP 249 §6.4: ``Connection.messages`` is "cleared by all
        # standard methods". ``connect()`` is a dqlite extension
        # (not in PEP 249), but the project-wide invariant — every
        # public Connection method clears messages first — covers
        # this method too. Without the clear, a stale entry from
        # any prior path would survive an eager-connect call,
        # breaking the uniform "method-call resets messages"
        # contract that ``cursor`` / ``commit`` / ``rollback`` /
        # ``close`` already follow.
        del self.messages[:]
        self._check_thread()
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
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
        # PEP 249 §6.1.1: Connection.messages should be cleared on
        # any standard Connection method invocation. The sibling
        # commit/rollback/cursor paths already clear; align close()
        # so the contract is uniform across the four required
        # methods.
        del self.messages[:]
        # Fork-after-init: the inherited connection FDs are shared
        # with the parent, the inherited daemon loop thread did not
        # survive fork (only the calling thread crosses), and
        # ``self._loop`` references a defunct loop. Calling
        # ``_close_async`` would deadlock or send FIN on sockets the
        # parent still uses. Flip the closed flags so the child can
        # GC its references quietly without touching the wire or the
        # dead loop, and skip the loop teardown. The pid-aware
        # ``_check_thread`` would also raise here, but close() is
        # documented as PEP 249 idempotent and silent on already-
        # closed inputs — quiet no-op preserves that contract for
        # the GC / atexit path that commonly drives close in a
        # forked worker.
        if _client_conn_mod._current_pid != self._creator_pid:
            self._closed = True
            self._closed_flag[0] = True
            # Cascade to tracked cursors so buffered fetches on them
            # stop silently answering from stale in-memory rows in
            # the child. stdlib sqlite3.Connection.close() does the
            # same; the non-fork branch below mirrors this loop.
            # Without it, a cursor inherited across fork retains
            # _closed=False and the parent's stale rows / description.
            try:
                for cur in list(self._cursors):
                    cur._closed = True
                    cur._rows = []
                    cur._description = None
                    cur._rowcount = -1
                    cur._lastrowid = None
                    cur._row_index = 0
                    # Mirror ``Cursor.close()``'s back-ref proxy
                    # so a cascade-closed cursor does not pin the
                    # (now-closing) parent connection.
                    with contextlib.suppress(TypeError):
                        cur._connection = weakref.proxy(cur._connection)
            finally:
                self._cursors.clear()
            if self._finalizer is not None:
                self._finalizer.detach()
                self._finalizer = None
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
                # PEP 249 §6.4: Cursor.messages "should be cleared
                # automatically by all standard cursor methods"; the
                # close-cascade should also leave each cursor's list
                # empty so a post-cascade ``cur.messages`` access
                # doesn't see stale entries from before the parent
                # connection closed.
                del cur.messages[:]
                # Mirror ``Cursor.close()``'s strong-ref release; see
                # the fork-branch sibling above for full rationale.
                with contextlib.suppress(TypeError):
                    cur._connection = weakref.proxy(cur._connection)
        finally:
            self._cursors.clear()
        # Detach the finalizer — it's about to do nothing useful, and
        # keeping it registered would double-stop the loop.
        if self._finalizer is not None:
            self._finalizer.detach()
            self._finalizer = None
        try:
            if self._loop is not None and not self._loop.is_closed():
                # Same-thread re-entry detection: if a signal handler
                # (SIGTERM / SIGINT) ran ``close()`` on the creator
                # thread while a prior ``_run_sync`` was still parked
                # in ``Future.result(timeout=...)``, ``_op_lock`` is
                # already held by this same thread. The bounded
                # acquire inside ``_run_sync`` would block for
                # ``self._timeout`` and time out — correct, but
                # operator-hostile (a SIGTERM handler that calls
                # ``close()`` should not pause for the configured
                # query timeout).
                #
                # Skip the bounded acquire entirely on detected same-
                # thread re-entry. The underlying ``_async_conn``'s
                # transport reap is best-effort even on the happy
                # path (the existing ``suppress(Exception)`` wrap
                # acknowledges this); the loop teardown in the
                # ``finally`` below still runs unconditionally so the
                # daemon thread is reaped and the OS socket FDs are
                # released by the loop's stop-and-close. Close the
                # un-awaited coroutine explicitly so it does not emit
                # ``coroutine 'Connection._close_async' was never
                # awaited`` at gc time.
                if self._op_lock.locked() and threading.get_ident() == self._creator_thread:
                    coro = self._close_async()
                    coro.close()
                else:
                    with contextlib.suppress(Exception):
                        self._run_sync(self._close_async())
        finally:
            with self._loop_lock:
                # Mirror ``AsyncConnection.close()``'s ``finally``-
                # clause discipline (``aio/connection.py``: every
                # exit path nulls ``self._async_conn``). The two
                # ``Exception``-suppressing arms above (same-thread
                # KI re-entry's ``coro.close()`` and the wedged-loop
                # ``contextlib.suppress(Exception)``) skip
                # ``_close_async``'s own finally — leaving
                # ``self._async_conn`` pointing at a live
                # ``DqliteConnection`` whose writer transport's FD
                # is reaped only at GC, AFTER the loop teardown
                # below stops the selector. ``connection_lost`` then
                # cannot fire and the FD lingers until the dbapi
                # instance itself is GC'd, surfacing as a
                # ``ResourceWarning("unclosed transport")``.
                #
                # Best-effort writer.close() drives FIN to the peer
                # synchronously instead of waiting on the
                # ``_SelectorSocketTransport``'s deferred ``__del__``.
                # Placed BEFORE ``self._loop.close()`` below so the
                # writer.close runs while the selector still exists
                # to dispatch the close event.
                if self._async_conn is not None:
                    inner = self._async_conn
                    proto = getattr(inner, "_protocol", None)
                    writer = getattr(proto, "_writer", None) if proto is not None else None
                    if writer is not None:
                        with contextlib.suppress(Exception):
                            writer.close()
                    self._async_conn = None
                if self._loop is not None and not self._loop.is_closed():
                    # ``is_closed()`` is a TOCTOU check — the loop
                    # could be closed by a concurrent finalizer /
                    # interpreter-shutdown sweep between the check
                    # and the ``call_soon_threadsafe`` call, raising
                    # ``RuntimeError("Event loop is closed")``. The
                    # finalizer at ``_cleanup_loop_thread`` already
                    # wraps the same call in ``suppress(RuntimeError)``;
                    # mirror the discipline here so ``Connection.close``
                    # cannot leak a bare ``RuntimeError`` past the
                    # PEP 249 ``Error`` hierarchy on the race.
                    with contextlib.suppress(RuntimeError):
                        self._loop.call_soon_threadsafe(self._loop.stop)
                    if self._thread is not None:
                        self._thread.join(timeout=_LOOP_THREAD_JOIN_TIMEOUT_SECONDS)
                    # ``loop.close()`` raises
                    # ``RuntimeError("Cannot close a running event loop")``
                    # if ``thread.join`` returned with the loop still
                    # alive (a wire read longer than the join budget
                    # leaves the loop spinning). The finalizer wraps
                    # the same call defensively (see
                    # ``_cleanup_loop_thread``); mirror that here so
                    # ``Connection.close()`` cannot leak a bare
                    # ``RuntimeError`` past the PEP 249 ``Error``
                    # hierarchy. Drop the local refs unconditionally
                    # so a retry close re-runs through the
                    # finalizer's reaping path on next GC.
                    try:
                        self._loop.close()
                    except RuntimeError:
                        logger.debug(
                            "Connection.close: loop.close raised RuntimeError "
                            "(loop thread did not exit within %s s); refs cleared",
                            _LOOP_THREAD_JOIN_TIMEOUT_SECONDS,
                            exc_info=True,
                        )
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

    @property
    def autocommit(self) -> bool:
        """``True`` — dqlite operates in autocommit-by-default mode.

        Mirrors the surface stdlib ``sqlite3`` added in Python 3.12 and
        the long-standing ``psycopg.Connection.autocommit`` accessor.
        Every statement commits at the server unless the caller issued
        an explicit ``BEGIN``. See class docstring for the contract.

        The bare dbapi exposes ``True`` here because the underlying
        wire protocol is genuinely autocommit-by-default. The
        SQLAlchemy adapter (``sqlalchemy-dqlite``) deliberately exposes
        ``False`` because SA wraps the connection with explicit
        BEGIN/COMMIT control — both are accurate for their respective
        layer.

        Setting to ``True`` is a no-op (acknowledges the existing
        mode); setting to ``False`` raises ``NotSupportedError``
        because the autocommit mode is fixed by the dqlite server
        and cannot be toggled at the dbapi level.
        """
        return True

    @autocommit.setter
    def autocommit(self, value: object) -> None:
        # Accept ``True`` (acknowledges the existing mode) and the
        # stdlib sentinel ``sqlite3.LEGACY_TRANSACTION_CONTROL``
        # (numerically ``-1``) — stdlib's 3.12+ surface uses the
        # sentinel as the "do not change isolation" signal that
        # cross-driver code passes through. Any other value
        # (including ``False`` / ``0`` / ``1`` / truthy non-bool)
        # raises ``NotSupportedError`` — stdlib itself enforces a
        # similarly strict gate (no PyObject_IsTrue coercion).
        if value is True or value == -1:
            return
        raise NotSupportedError(
            "dqlite operates in autocommit-by-default mode; the autocommit "
            "flag cannot be turned off at the dbapi level. Wrap your "
            "statements in explicit BEGIN/COMMIT (issued via cursor.execute) "
            "to control transaction boundaries instead."
        )

    @property
    def isolation_level(self) -> None:
        """stdlib pre-3.12 ``sqlite3.Connection.isolation_level``-
        parity surface. Returns ``None`` — stdlib's autocommit
        sentinel; truthful for dqlite's autocommit-by-default mode
        (the bijection ``autocommit=True`` ↔ ``isolation_level=None``).

        Setter accepts ``None`` (acknowledges the existing mode);
        the implicit-transaction values (``""``, ``"DEFERRED"``,
        ``"IMMEDIATE"``, ``"EXCLUSIVE"``) raise
        ``NotSupportedError`` because the wire protocol does not
        surface server-side implicit-transaction semantics.

        Without this property, ``conn.isolation_level = None``
        succeeded silently (Python allows arbitrary instance
        attribute writes without ``__slots__``); the user's
        attempt to express "use autocommit" had no effect on the
        driver. The property closes the silent-write footgun.
        """
        return None

    @isolation_level.setter
    def isolation_level(self, value: object) -> None:
        if value is None:
            return
        raise NotSupportedError(
            "dqlite operates in autocommit-by-default mode and does not "
            "support stdlib sqlite3 implicit-transaction isolation_level "
            "values; use explicit BEGIN/COMMIT via cursor.execute or set "
            "isolation_level=None to acknowledge the existing mode."
        )

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
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if self._async_conn is None:
            return
        # Local short-circuit when no transaction is active. Mirrors
        # stdlib ``sqlite3.Connection.commit`` which uses
        # ``sqlite3_get_autocommit`` to skip the wire round-trip.
        # ``in_transaction`` already ORs in the
        # ``_has_untracked_savepoint`` flag at the client layer, so the
        # property covers the autobegun-via-quoted-SAVEPOINT case
        # without the dbapi having to peek at the private attribute.
        # ``getattr`` keeps mock tolerance: stripped-down test stubs
        # without the property short-circuit (no wire round-trip) the
        # same way a fresh connection would.
        if not getattr(self._async_conn, "in_transaction", False):
            return
        self._run_sync(self._commit_async())

    async def _commit_async(self) -> None:
        """Async implementation of commit."""
        if self._async_conn is None:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Clear ``messages`` under the lock so the PEP 249 contract
        # "messages cleared by every method call" is atomic with the
        # operation. ``_run_sync`` holds ``_op_lock`` across this
        # coroutine; the pre-lock clear in ``commit()`` leaves a
        # window where a sibling thread could write directly to
        # ``messages`` between that clear and the lock acquire.
        # Mirror the async sibling's defense-in-depth shape.
        del self.messages[:]
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
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        if self._async_conn is None:
            return
        # See commit() — same local short-circuit applies. Saves a
        # wire round-trip on the autocommit-by-default common case.
        if not getattr(self._async_conn, "in_transaction", False):
            return
        self._run_sync(self._rollback_async())

    async def _rollback_async(self) -> None:
        """Async implementation of rollback."""
        if self._async_conn is None:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # In-lock messages clear; see ``_commit_async`` for the
        # rationale.
        del self.messages[:]
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
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        cur = Cursor(self)
        self._cursors.add(cur)
        return cur

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
    ) -> Cursor:
        """PEP 249 optional extension — open a cursor, run ``execute``,
        return the cursor.

        Parity with stdlib ``sqlite3.Connection.execute`` and with the
        async-side ``AsyncAdaptedConnection.execute``. SA-internal code
        paths and the ``connect``-event listener idiom call
        ``dbapi_connection.execute(...)`` directly; without this method,
        sync dialect users hit ``AttributeError`` on the first checkout
        of a ``dqlite://`` engine that registers a ``connect`` listener
        — an opaque diagnostic that escapes the ``dbapi.Error``
        hierarchy.

        On a synchronous failure of ``cur.execute(...)`` close the
        freshly-opened cursor before re-raising so the caller's
        exception path doesn't leak an unowned cursor. Mirrors the
        async adapter's cleanup-on-raise discipline.
        """
        cur = self.cursor()
        try:
            if parameters is None:
                cur.execute(operation)
            else:
                cur.execute(operation, parameters)
        except BaseException:
            with contextlib.suppress(Exception):
                cur.close()
            raise
        return cur

    def executemany(
        self,
        operation: str,
        seq_of_parameters: Iterable[Sequence[Any]],
    ) -> Cursor:
        """PEP 249 optional extension — open a cursor, run
        ``executemany``, return the cursor.

        Parity with stdlib ``sqlite3.Connection.executemany`` and with
        the async-side ``AsyncConnection.executemany``. Cross-driver
        code (aiosqlite / psycopg / asyncpg) reaches for this shortcut
        on both sync and async sides; without it, sync callers hit
        ``AttributeError`` — an opaque diagnostic that escapes the
        ``dbapi.Error`` hierarchy.

        Mirrors the cleanup-on-raise discipline of ``execute``: close
        the freshly-opened cursor on synchronous failure before
        re-raising so the caller's exception path doesn't leak an
        unowned cursor.
        """
        cur = self.cursor()
        try:
            cur.executemany(operation, seq_of_parameters)
        except BaseException:
            with contextlib.suppress(Exception):
                cur.close()
            raise
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

        Mirrors the psycopg / asyncpg convention so callers porting
        idempotent-close patterns (``if not conn.closed: conn.close()``)
        do not hit ``AttributeError``. PEP 249 does not mandate this
        property; stdlib ``sqlite3`` famously omits it. The underlying
        flag is already maintained by every method that mutates
        closed-ness.
        """
        return self._closed

    @property
    def row_factory(self) -> None:
        """stdlib ``sqlite3.Connection.row_factory``-parity stub.

        dqlitedbapi does not support custom row construction; rows
        are always returned as plain tuples per PEP 249. The
        property exists so cross-driver code that READS
        ``conn.row_factory`` does not hit ``AttributeError``; the
        SETTER rejects with ``NotSupportedError`` so an attempted
        write does not silently succeed (which today would just
        add a regular attribute to the instance and have no effect
        on row construction)."""
        return None

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        if value is None:
            return
        raise NotSupportedError(
            "dqlitedbapi does not support row_factory; rows are always "
            "returned as plain tuples per PEP 249"
        )

    @property
    def text_factory(self) -> type[str]:
        """stdlib ``sqlite3.Connection.text_factory``-parity stub.

        dqlitedbapi always returns TEXT cells as ``str`` (UTF-8
        decoded at the wire layer); custom text-factory routing is
        not supported. Setter rejects non-``str`` values with
        ``NotSupportedError`` so a silent write cannot happen."""
        return str

    @text_factory.setter
    def text_factory(self, value: object) -> None:
        if value is str:
            return
        raise NotSupportedError(
            "dqlitedbapi does not support text_factory; TEXT cells are "
            "always returned as str (UTF-8 decoded at the wire layer)"
        )

    # PEP 249 §7 (TPC extension) and stdlib sqlite3 parity stubs.
    # PEP 249 says drivers without TPC support MUST raise
    # NotSupportedError on the TPC methods rather than letting
    # AttributeError leak (which escapes the dbapi.Error hierarchy).
    # The stdlib-sqlite3 helpers (load_extension, backup, iterdump,
    # create_function/aggregate/collation) similarly should surface
    # via NotSupportedError so cross-driver code that calls them
    # inside ``except sqlite3.Error:`` catches uniformly. dqlite-
    # server does not implement any of these.
    #
    # **Note for cross-driver code porting from stdlib ``sqlite3``:**
    # ``hasattr(conn, "tpc_begin")`` returns ``True`` on this driver
    # because the stub IS defined (it just unconditionally raises).
    # Stdlib ``sqlite3`` has no ``tpc_*`` methods at all, so
    # ``hasattr`` returns ``False`` there. Code that feature-detects
    # via ``hasattr`` will mistakenly take the "supported" branch
    # against dqlitedbapi and then surface ``NotSupportedError``
    # from inside the call. To portably test for support, use a
    # ``try: conn.tpc_begin(xid); except dbapi.NotSupportedError:``
    # block instead of ``hasattr``. The same caveat applies to
    # ``callproc`` / ``nextset`` / ``scroll`` on the cursor side.

    def tpc_begin(self, xid: object) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    def tpc_prepare(self) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    def tpc_commit(self, xid: object | None = None) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    def tpc_rollback(self, xid: object | None = None) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    def tpc_recover(self) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    def xid(self, format_id: int, global_transaction_id: str, branch_qualifier: str) -> NoReturn:
        raise NotSupportedError("dqlite does not support two-phase commit")

    def executescript(self, sql_script: str) -> NoReturn:
        """stdlib ``sqlite3``-parity stub. dqlite has no
        multi-statement-script primitive on the wire (each statement
        requires a separate Prepare → Exec / Query round-trip), so
        this raises ``NotSupportedError`` rather than escaping
        ``dbapi.Error`` as ``AttributeError``. Callers should split
        the script and ``execute`` each statement individually."""
        raise NotSupportedError(
            "dqlite does not support stdlib sqlite3 executescript; "
            "split the script and execute each statement individually"
        )

    def interrupt(self) -> NoReturn:
        """stdlib ``sqlite3``-parity stub. dqlite's wire-level
        interrupt primitive is not surfaced at the dbapi layer in
        this driver. Callers needing cross-thread cancellation
        should wrap calls in ``asyncio.timeout`` (async surface)
        or rely on the configured per-RPC timeout."""
        raise NotSupportedError(
            "dqlite does not surface interrupt() at the dbapi layer; "
            "use asyncio.timeout(...) on the async surface or rely "
            "on the per-RPC timeout"
        )

    # stdlib ``sqlite3.Connection``-parity stubs for VDBE-callback
    # / db-status / db-config / serialize / blob-open primitives.
    # None are wire-feasible (the VDBE / pager runs server-side; no
    # client-callable hook). Stub with ``NotSupportedError`` so the
    # rejection stays inside the ``dbapi.Error`` hierarchy instead
    # of leaking ``AttributeError``. Same family as the existing
    # ``load_extension`` / ``backup`` / ``iterdump`` / ``create_*``
    # stubs.

    def set_authorizer(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError(
            "dqlite-server does not expose a per-prepare authorization callback"
        )

    def set_progress_handler(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError("dqlite-server does not expose a VDBE progress callback")

    def set_trace_callback(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError("dqlite-server does not expose a per-statement trace callback")

    @property
    def total_changes(self) -> NoReturn:
        raise NotSupportedError(
            "dqlite-server does not surface a total_changes counter on the wire"
        )

    def getlimit(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError(
            "dqlite-server does not expose sqlite3_db_status getlimit/setlimit on the wire"
        )

    def setlimit(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError(
            "dqlite-server does not expose sqlite3_db_status getlimit/setlimit on the wire"
        )

    def getconfig(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError(
            "dqlite-server does not expose sqlite3_db_config getconfig/setconfig on the wire"
        )

    def setconfig(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError(
            "dqlite-server does not expose sqlite3_db_config getconfig/setconfig on the wire"
        )

    def serialize(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError(
            "dqlite does not support sqlite3_serialize; conflicts with the distributed Raft model"
        )

    def deserialize(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError(
            "dqlite does not support sqlite3_deserialize; conflicts with the distributed Raft model"
        )

    def blobopen(self, *args: object, **kwargs: object) -> NoReturn:
        raise NotSupportedError("dqlite does not expose sqlite3_blob_open on the wire")

    def enable_load_extension(self, enabled: bool) -> NoReturn:
        raise NotSupportedError("dqlite-server does not support runtime extension loading")

    def load_extension(self, path: str, *, entrypoint: str | None = None) -> NoReturn:
        raise NotSupportedError("dqlite-server does not support runtime extension loading")

    def backup(self, *args: object, **kwargs: object) -> NoReturn:
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
        return f"<Connection address={self._address!r} database={self._database!r} {state}>"

    def __reduce__(self) -> NoReturn:
        # Connections own a live socket, an event-loop thread, and a
        # weakref-finalizer cycle — none of which survives pickling.
        # Without this guard the default pickle walks the attribute
        # graph and surfaces a confusing ``cannot pickle '_thread.lock'``
        # message that buries the driver-level intent. Stdlib
        # ``sqlite3.Connection`` raises an explicit driver-level
        # TypeError; mirror that shape.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — driver "
            "connections own a live socket and an event-loop thread; "
            "use a connection pool or recreate the connection in the "
            "consumer process instead"
        )

    def __enter__(self) -> Self:
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
