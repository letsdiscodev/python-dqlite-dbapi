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
from collections.abc import Coroutine, Iterable, Iterator, Sequence
from types import TracebackType
from typing import Any, Final, NoReturn, Self

import dqliteclient.exceptions as _client_exc
from dqliteclient import CLOSE_TIMEOUT_FLOOR as _client_close_timeout_floor
from dqliteclient import (
    DEFAULT_CLOSE_TIMEOUT_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    ClusterClient,
    DialFunc,
    DqliteConnection,
    MemoryNodeStore,
    get_current_pid,
    validate_positive_int_or_none,
)
from dqliteclient import parse_address as _client_parse_address
from dqlitedbapi import exceptions as _exc
from dqlitedbapi.cursor import Cursor, _call_client, _validate_executemany_seq_shape
from dqlitedbapi.exceptions import (
    AmbiguousCommitError,
    DatabaseError,
    DataError,
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
    NO_TRANSACTION_MESSAGE_SUBSTRINGS,
    WIRE_DECODE_FAILED_PREFIX,
    primary_sqlite_code,
    sanitize_for_log,
)
from dqlitewire import (
    EncodeError as _WireEncodeError,
)

__all__ = ["Connection"]

logger = logging.getLogger(__name__)

# SQLite codes for a stray COMMIT/ROLLBACK with no active transaction.
# Deliberately excludes 21 (SQLITE_MISUSE, unrelated VFS path) and 0
# (empty-statement failure, which must surface as a normal error). The
# numeric gate runs before the substring filter so a hostile server
# cannot silence unrelated errors via a crafted message.
_NO_TX_PRIMARY_CODES: Final[frozenset[int]] = frozenset({1})
# Members must be primary codes (< 256); the lookup site masks via
# primary_sqlite_code() before set membership, so an extended code
# (e.g. 513) would never match. ``if __debug__:`` makes the -O strip
# explicit; runtime enforcement is test_no_tx_primary_codes_invariant.
if __debug__:
    assert all(0 <= c < 256 for c in _NO_TX_PRIMARY_CODES), (
        "_NO_TX_PRIMARY_CODES must hold primary SQLite codes (< 256); "
        "use primary_sqlite_code(extended) at the lookup site instead."
    )
# Shared with the client-layer recogniser so a server/SQLite wording
# drift cannot make one layer suppress while the other raises.
_NO_TX_SUBSTRINGS: Final[tuple[str, ...]] = NO_TRANSACTION_MESSAGE_SUBSTRINGS

# Floor for the background-loop-thread join on teardown. close_timeout
# can be as low as 0.01 s, too tight for the queued ``loop.stop`` to
# land and the daemon thread to exit on a non-stuck loop.
_LOOP_THREAD_JOIN_MIN_SECONDS: Final[float] = 0.1

# Shortened join budget when the finalizer / force-close runs on a
# thread that itself hosts a running loop (mixed deployments): a
# full-budget ``thread.join`` would park the user's loop for up to
# close_timeout. ~20 ms instead; the daemon loop drains independently
# and ``daemon=True`` bounds any residual lifetime to interpreter exit.
_LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS: Final[float] = 0.02


def _join_budget_for_current_thread(
    close_timeout: float,
    *,
    _asyncio: Any = asyncio,
    _join_min: float = _LOOP_THREAD_JOIN_MIN_SECONDS,
    _foreign_floor: float = _LOOP_THREAD_JOIN_FOREIGN_FLOOR_SECONDS,
) -> float:
    """Join budget for the finalizer / force-close path: foreign-floor
    when the calling thread hosts a loop, else max(close_timeout, min).

    ``asyncio`` and the floor constants are captured as kwarg defaults
    so a ``Py_FinalizeEx`` phase-3 globals-None-set cannot turn the
    loop probe into an unraisable-hook traceback (the call sites'
    RuntimeError suppression would not catch the AttributeError).
    """
    try:
        if _asyncio is None:
            on_loop_thread = False
        else:
            _asyncio.get_running_loop()
            on_loop_thread = True
    except RuntimeError:
        on_loop_thread = False
    except Exception:
        # Phase-3 teardown can leave ``_asyncio`` partially cleared;
        # fall back to the off-loop budget.
        on_loop_thread = False
    if on_loop_thread:
        return _foreign_floor
    return max(close_timeout, _join_min)


# ``self._timeout`` is a PER-PHASE budget; the async surface wraps
# each RPC in its own ``asyncio.timeout(timeout)``, so a single sync
# call (bridged via one ``Future.result(timeout=...)``) must absorb
# all phases or it fails on benign latency the async surface tolerates.
# N=4 covers the worst case: handshake + open_database + send +
# read+drain. Steady-state bottoms out at N=2.
_SYNC_PHASES_MULTIPLIER: Final[int] = 4

# Fallback join budget for ``_cleanup_loop_thread`` when the captured
# ``close_timeout`` is missing or invalid.
_LOOP_THREAD_JOIN_FALLBACK_SECONDS: Final[float] = 5.0


def _validate_timeout(timeout: float) -> None:
    """Raise ProgrammingError if ``timeout`` is not a positive finite number."""
    from dqliteclient import validate_timeout as _client_validate_timeout

    try:
        _client_validate_timeout(timeout)
    except (TypeError, ValueError) as e:
        raise ProgrammingError(str(e)) from e


# Mirrors the SA URL/connect_args cap (same 10x factor) so direct
# dbapi callers reject the same above-cap typos — defence against
# granting a hostile/misconfigured server a huge decode budget.
MAX_CONTINUATION_FRAMES_UPPER_BOUND: Final[int] = _DEFAULT_MAX_CONTINUATION_FRAMES * 10


# Stdlib ``isolation_level`` pre-3.12 accept-set (less ``None``).
# dqlite is fixed-mode autocommit so these collapse to no-ops;
# accepting them preserves the cross-driver round-trip idiom.
_STDLIB_IMPLICIT_TX_VALUES: Final[frozenset[str]] = frozenset(
    {"", "DEFERRED", "IMMEDIATE", "EXCLUSIVE"}
)


def _wrap_positive_int(
    value: int | None,
    name: str,
    *,
    upper: int | None = None,
) -> int | None:
    """Wrap ``validate_positive_int_or_none``'s errors as ProgrammingError.

    ``upper`` (if given) caps the accepted value, mirroring the SA
    URL/connect_args bound for direct dbapi callers.
    """
    try:
        validated = validate_positive_int_or_none(value, name)
    except (TypeError, ValueError) as e:
        raise ProgrammingError(str(e)) from e
    if validated is not None and upper is not None and validated > upper:
        raise ProgrammingError(
            f"{name}={validated} exceeds the upper bound of {upper}; "
            f"a value above this cap is almost certainly a typo and "
            f"would grant a hostile or misconfigured server an "
            f"unreasonable Python-side decode budget. Reduce the value "
            f"or omit the kwarg to inherit the wire-layer default."
        )
    return validated


# SSOT lives in ``dqliteclient.CLOSE_TIMEOUT_FLOOR``; re-exported here
# under the established private name to avoid reference churn.
_CLOSE_TIMEOUT_FLOOR: Final[float] = _client_close_timeout_floor


# Prefix on every connect-time OperationalError wrap. SA's is_disconnect
# matches a lowercase truncation of it; dbapi tests match it verbatim.
# Keep as SSOT so a wording change updates both in lockstep.
FAILED_TO_CONNECT_PREFIX: Final[str] = "Failed to connect: "


# Shared with the cursor-side rewrap site (avoids a circular import).
from dqlitedbapi._constants import (  # noqa: E402
    _is_int_not_bool,
    cluster_policy_rejection_message,
)


def _validate_close_timeout(close_timeout: float) -> None:
    """Raise ProgrammingError if close_timeout is not a positive finite number >= floor."""
    from dqliteclient import CLOSE_TIMEOUT_FLOOR_RATIONALE
    from dqliteclient import validate_timeout as _client_validate_timeout

    try:
        _client_validate_timeout(
            close_timeout,
            name="close_timeout",
            min_value=_CLOSE_TIMEOUT_FLOOR,
            min_value_rationale=CLOSE_TIMEOUT_FLOOR_RATIONALE,
        )
    except (TypeError, ValueError) as e:
        raise ProgrammingError(str(e)) from e


# Process-wide ``ClusterClient`` cache for the leader-discovery probe,
# keyed by the full config tuple. Without it every connect()/pool
# warm-up builds a fresh client, discarding the single-flight slot map
# and ``_last_known_leader`` fast-path (N parallel sweeps after a flip
# where one would do). Invalidated wholesale on fork (parent-loop-bound
# Lock/Task refs cannot progress in the child). Strong dict so the
# client outlives a single ``find_leader`` call; cap bounds the worst
# case.
_RESOLVE_LEADER_CACHE: dict[tuple[object, ...], ClusterClient] = {}
_RESOLVE_LEADER_CACHE_PID: int = os.getpid()
_RESOLVE_LEADER_CACHE_MAX: Final[int] = 32
# Serialises the read-check-construct-insert composite (the individual
# dict ops are GIL-atomic but the composite is not). Held across
# ``ClusterClient.__init__``, which must NOT do async I/O (it doesn't
# today); if that changes, reshape to construct outside the lock.
# Not Final: the after-fork hook replaces it so a child inheriting a
# held lock cannot deadlock (the sync layer starts a daemon thread per
# Connection, so a forking process is multi-threaded by definition).
_RESOLVE_LEADER_CACHE_LOCK: threading.Lock = threading.Lock()


def _at_fork_replace_resolve_leader_cache_lock() -> None:
    """Replace the cache lock in the child so an inherited held lock
    cannot deadlock first cache access (the pid-mismatch clear inside
    ``_get_resolve_leader_cluster`` needs the lock grabbable first)."""
    global _RESOLVE_LEADER_CACHE_LOCK
    _RESOLVE_LEADER_CACHE_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_at_fork_replace_resolve_leader_cache_lock)


def _get_resolve_leader_cluster(
    *,
    address: str,
    timeout: float,
    max_total_rows: int | None,
    max_continuation_frames: int | None,
    trust_server_heartbeat: bool,
    dial_func: DialFunc | None = None,
    max_message_size: int | None = None,
) -> ClusterClient:
    """Process-shared :class:`ClusterClient` for the leader probe, keyed
    by ``(loop_id, address, governors, dial_func)``.

    Keyed by ``id(running_loop)`` so a worker-thread loop never reuses a
    client whose ``_find_leader_tasks`` were created on another loop
    (``asyncio.shield`` of a foreign-loop task raises RuntimeError,
    escaping the dbapi.Error tree). LRU-evicted at cap so closed loops
    don't leak; id-recycle after GC is bounded by the cap.

    Async-only: raises RuntimeError if there's no running loop (fail
    loud rather than cache against a sentinel loop_id).
    """
    global _RESOLVE_LEADER_CACHE_PID
    try:
        loop_id = id(asyncio.get_running_loop())
    except RuntimeError as e:
        raise RuntimeError(
            "_get_resolve_leader_cluster must be invoked from within a "
            "running event loop (via _run_sync)."
        ) from e

    # Distinct dialers MUST NOT share a client (different transport
    # contracts: TLS/plaintext, AF_UNIX/TCP, etc.). The callable itself
    # goes in the key (not ``id(dial_func)``) so it hashes by identity
    # and is pinned for the entry's lifetime — closes the id-recycle
    # window where a fresh lambda lands at the same address.
    key: tuple[object, ...] = (
        loop_id,
        address,
        timeout,
        max_total_rows,
        max_continuation_frames,
        max_message_size,
        trust_server_heartbeat,
        dial_func,
    )

    # Double-checked init: look up under the lock, construct outside it,
    # retake to register. Holding the lock across ``__init__`` would
    # deadlock if a future refactor adds an await/I/O there.
    with _RESOLVE_LEADER_CACHE_LOCK:
        pid = get_current_pid()
        if pid != _RESOLVE_LEADER_CACHE_PID:
            _RESOLVE_LEADER_CACHE.clear()
            _RESOLVE_LEADER_CACHE_PID = pid
        cluster = _RESOLVE_LEADER_CACHE.get(key)
        if cluster is not None:
            return cluster

    # ``max_message_size`` is intentionally NOT forwarded: ClusterClient
    # lacks the ctor kwarg, and the leader-probe LeaderResponse is well
    # below the wire default. Still in the cache key so connect()
    # variants stay independent if ClusterClient ever grows the kwarg.
    new_cluster = ClusterClient(
        MemoryNodeStore([address]),
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        trust_server_heartbeat=trust_server_heartbeat,
        dial_func=dial_func,
    )

    with _RESOLVE_LEADER_CACHE_LOCK:
        # Recheck: a concurrent caller may have inserted while we
        # constructed (first writer wins). Re-validate PID in case a
        # fork happened during the unlocked construction.
        pid = get_current_pid()
        if pid != _RESOLVE_LEADER_CACHE_PID:
            _RESOLVE_LEADER_CACHE.clear()
            _RESOLVE_LEADER_CACHE_PID = pid
        existing = _RESOLVE_LEADER_CACHE.get(key)
        if existing is not None:
            return existing
        if len(_RESOLVE_LEADER_CACHE) >= _RESOLVE_LEADER_CACHE_MAX:
            # FIFO eviction of the oldest entry. Costs at most one
            # wasted sweep per evicted key with concurrent demand;
            # self-heals on the next resolve.
            _RESOLVE_LEADER_CACHE.pop(next(iter(_RESOLVE_LEADER_CACHE)))
        _RESOLVE_LEADER_CACHE[key] = new_cluster
        return new_cluster


async def _resolve_leader(
    address: str,
    *,
    timeout: float,
    max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
    max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
    max_message_size: int | None = None,
    trust_server_heartbeat: bool = False,
    dial_func: DialFunc | None = None,
) -> str:
    """Follow the leader-redirect chain from a seed address; return the
    leader's address.

    Without this, connecting to a demoted-leader address surfaces
    SQLITE_IOERR_NOT_LEADER even though a healthy leader exists
    elsewhere. The governors and ``dial_func`` are threaded so the
    probe (the FIRST round-trip) runs with the same config as the data
    session — notably so a TLS/AF_UNIX dialer isn't bypassed by a
    plaintext probe socket.
    """
    cluster = _get_resolve_leader_cluster(
        address=address,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        max_message_size=max_message_size,
        trust_server_heartbeat=trust_server_heartbeat,
        dial_func=dial_func,
    )
    return await cluster.find_leader()


async def _build_and_connect(
    address: str,
    *,
    database: str,
    timeout: float,
    max_total_rows: int | None,
    max_continuation_frames: int | None,
    trust_server_heartbeat: bool,
    close_timeout: float,
    dial_timeout: float | None = None,
    attempt_timeout: float | None = None,
    dial_func: DialFunc | None = None,
    max_message_size: int | None = None,
    session_mode: str = "immediate",
) -> DqliteConnection:
    """Resolve the leader, then construct and connect a DqliteConnection
    against it.

    The "Failed to connect: ..." OperationalError phrasing is verbatim
    so prefix-matching tests keep passing.
    """
    try:
        leader_address = await _resolve_leader(
            address,
            timeout=timeout,
            max_total_rows=max_total_rows,
            max_continuation_frames=max_continuation_frames,
            max_message_size=max_message_size,
            trust_server_heartbeat=trust_server_heartbeat,
            dial_func=dial_func,
        )
    except _client_exc.ClusterPolicyError as e:
        # Allowlist rejected a redirect target; symmetric with the
        # post-construct ClusterPolicyError arm below.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(
            cluster_policy_rejection_message("during leader discovery", str(e)),
            code=None,
            raw_message=raw_msg,
        ) from e
    except _client_exc.ClusterError as e:
        # No leader / all nodes unreachable. OperationalError for the
        # SA pool retry loop; distinct prefix from the post-construct
        # arm so logs tell "no leader" from "found but couldn't connect".
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(
            f"Failed to find leader from {address}: {e}",
            code=None,
            raw_message=raw_msg,
        ) from e
    except OSError as e:
        # Unreached on the happy path (find_leader aggregates per-probe
        # OSErrors into ClusterError); catches custom NodeStore OSError,
        # future gaierror, or a leaked TimeoutError. PEP 249 §7 surface.
        raise OperationalError(
            f"Failed to find leader from {address}: {e}",
            code=None,
            raw_message=str(e),
        ) from e
    except BaseExceptionGroup as eg:
        # BaseExceptionGroup bypasses every per-class arm (not an
        # Exception). PEP 654 cancel-class split re-raises cancel/KI/
        # SystemExit children rather than wrapping them. See
        # cursor.py::_call_client for the full rationale.
        cancel_group, remainder = eg.split(
            lambda e: isinstance(e, (asyncio.CancelledError, KeyboardInterrupt, SystemExit))
        )
        if cancel_group is not None:
            raise cancel_group from None
        # ``if`` not ``assert`` so the .exceptions access below doesn't
        # raise AttributeError under -O. Unreachable per the split contract.
        if remainder is None:
            raise eg
        child_classes = {type(c).__name__ for c in remainder.exceptions}
        raise OperationalError(
            f"Failed to find leader from {address}: aggregate "
            f"{type(remainder).__name__} with {len(remainder.exceptions)} child(ren) "
            f"of class(es) {sorted(child_classes)}",
            code=None,
            raw_message=str(remainder),
        ) from remainder

    conn = DqliteConnection(
        leader_address,
        database=database,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
        max_message_size=max_message_size,
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
        dial_timeout=dial_timeout,
        attempt_timeout=attempt_timeout,
        dial_func=dial_func,
    )
    try:
        await conn.connect()
    except _client_exc.OperationalError as e:
        # Route through the cursor-path primary-code classifier so
        # connect-time CORRUPT/NOTADB/etc. surface as the right PEP 249
        # subclass and leader-change codes carry through for
        # is_disconnect. The prefix goes on ``message`` only, never on
        # raw_message (which is verbatim server text).
        from dqlitedbapi.cursor import _classify_operational

        exc_cls = _classify_operational(e.code)
        if issubclass(exc_cls, DatabaseError) or issubclass(exc_cls, InterfaceError):
            raise exc_cls(
                f"{FAILED_TO_CONNECT_PREFIX}{e.message}",
                code=e.code,
                raw_message=e.raw_message,
            ) from e
        # Fallback if a future class lands outside both umbrellas.
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}{e.message}",
            code=e.code,
            raw_message=e.raw_message,
        ) from e
    except _client_exc.ClusterPolicyError as e:
        # Permanent config mismatch. InterfaceError with the "Cluster
        # policy rejection;" prefix so SA's is_disconnect does NOT
        # retry it (it narrows InterfaceError to closed-conn/cursor).
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(
            cluster_policy_rejection_message(None, str(e)),
            code=None,
            raw_message=raw_msg,
        ) from e
    except _client_exc.DqliteConnectionError as e:
        # Transport/handshake failure. Thread code+raw_message so a
        # leader-change rewrap carries the wire signal is_disconnect
        # expects, matching the query path.
        code = getattr(e, "code", None)
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}{e}", code=code, raw_message=raw_msg
        ) from e
    except _client_exc.ClusterError as e:
        # Transient discovery failure (no leader yet / all unreachable).
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}{e}", code=None, raw_message=raw_msg
        ) from e
    except _client_exc.ProtocolError as e:
        # Handshake wire desync. Use WIRE_DECODE_FAILED_PREFIX so SA's
        # substring scan recognises it.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(
            f"{WIRE_DECODE_FAILED_PREFIX}: {e}", code=None, raw_message=raw_msg
        ) from e
    except _client_exc.DataError as e:
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise DataError(str(e), code=None, raw_message=raw_msg) from e
    except _WireEncodeError as e:
        # Raw wire EncodeError on the open request (NUL/oversize/
        # surrogate db name). NOT a subclass of client ProtocolError,
        # so without this arm it escapes every ``except dbapi.Error:``.
        raise DataError(f"wire encode failed: {e}", code=None, raw_message=str(e)) from e
    except _client_exc.InterfaceError as e:
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(str(e), code=None, raw_message=raw_msg) from e
    except _client_exc.DqliteError as e:
        # Catch-all for future DqliteError subclasses. DatabaseError
        # (not InterfaceError) so server-sourced errors classify right.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise DatabaseError(
            f"unrecognized client error ({type(e).__name__}): {e}",
            code=None,
            raw_message=raw_msg,
        ) from e
    except OSError as e:
        # Transport error escaping the client's wrap discipline.
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}{e}", code=None, raw_message=str(e)
        ) from e
    except BaseExceptionGroup as eg:
        # See the sibling arm above the construct; cancel-class split
        # runs first, remainder wrapped as OperationalError.
        cancel_group, remainder = eg.split(
            lambda e: isinstance(e, (asyncio.CancelledError, KeyboardInterrupt, SystemExit))
        )
        if cancel_group is not None:
            raise cancel_group from None
        # ``if`` not ``assert`` so the .exceptions access below doesn't
        # raise AttributeError under -O. Unreachable per the split contract.
        if remainder is None:
            raise eg
        child_classes = {type(c).__name__ for c in remainder.exceptions}
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}aggregate {type(remainder).__name__} with "
            f"{len(remainder.exceptions)} child(ren) of class(es) {sorted(child_classes)}",
            code=None,
            raw_message=str(remainder),
        ) from remainder
    # Read-only session: emit PRAGMA query_only=1 on the inner conn
    # (bypasses the Cursor op_lock). Re-emitted naturally on every
    # rebuild since reconnect re-enters here. query_only is NOT in
    # dqlite-server's PRAGMA deny-list (vfs.c), so it can't be rejected.
    if session_mode == "read_only":
        try:
            await conn.execute("PRAGMA query_only = 1")
        except BaseException:
            # Shield the close against a second cancel. Hoist into an
            # explicit Task with a done-callback observer first so an
            # outer cancel doesn't orphan the implicit shield Task
            # ("Task exception was never retrieved" at GC).
            from dqliteclient.cluster import _observe_drain_exception

            close_task = asyncio.ensure_future(conn.close())
            close_task.add_done_callback(_observe_drain_exception)
            with contextlib.suppress(Exception):
                await asyncio.shield(close_task)
            raise
    return conn


def _is_no_transaction_error(exc: Exception) -> bool:
    """True if ``exc`` is a genuine "no active transaction" server reply.

    Gates the silent swallow on BOTH the SQLite code and the canonical
    wording. ``code=None`` (how the dbapi wraps connection/cluster/
    protocol errors) never matches — those must surface, not swallow.

    The substring filter is load-bearing despite the code gate:
    DQLITE_ERROR shares low-byte 1 with SQLITE_ERROR, and upstream
    emits DQLITE_ERROR for "leadership transfer failed" (a path the
    client doesn't invoke today, so the collision is latent). Drop the
    filter only if the wire gains a namespace-discriminator byte.
    """
    code = getattr(exc, "code", None)
    if code is None:
        return False
    # Mask to the primary code (mirrors _classify_operational) so an
    # extended variant of code 1/21 can't slip past the whitelist.
    if primary_sqlite_code(code) not in _NO_TX_PRIMARY_CODES:
        return False
    # Match the un-truncated raw_message: a long message with the no-tx
    # clause past the truncation cap would miss against str(exc).
    raw = getattr(exc, "raw_message", None) or str(exc)
    lowered = raw.lower()
    return any(s in lowered for s in _NO_TX_SUBSTRINGS)


def _safe_writer_close(writer: asyncio.StreamWriter) -> None:
    """Scheduled via call_soon_threadsafe to drive FIN out of a transport
    without awaiting drain; swallows a connection_lost-race exception."""
    try:
        writer.close()
    except Exception:  # noqa: BLE001 - last-resort cleanup
        logger.debug(
            "Connection.force_close_transport: writer.close() raised; ignoring",
            exc_info=True,
        )


def _cleanup_loop_thread(
    loop: asyncio.AbstractEventLoop,
    thread: threading.Thread,
    closed_flag: list[bool],
    address: str,
    creator_pid: int,
    close_timeout: float = _LOOP_THREAD_JOIN_FALLBACK_SECONDS,
    inner_handle: list[Any] | None = None,
    *,
    # Capture pure-module globals as kwarg defaults so a Py_FinalizeEx
    # phase-3 globals-None-set can't replace the names this body
    # dereferences between definition and finalizer invocation.
    # ``get_current_pid`` is deliberately NOT captured (tests patch it
    # to simulate a fork); its deref below is try-wrapped instead.
    _warnings: Any = warnings,
    _logger: Any = logger,
    _contextlib: Any = contextlib,
    _sanitize_for_log: Any = sanitize_for_log,
) -> None:
    """Stop the background event loop and join its thread (from a
    ``weakref.finalize``, so it must not reference the Connection).

    ``closed_flag`` is a 1-element list the Connection flips on close
    (decides the ResourceWarning); ``inner_handle`` is a late-published
    box holding a ``weakref.ref`` to the inner DqliteConnection. Both
    use the box idiom to avoid pinning the outer/inner into the
    finalize args (which would create a GC-preventing cycle). Only the
    leaked-outer-GC path reaches here with the box populated.

    Fork-safe (pid-mismatch -> no-op; the captured loop/thread are
    parent-owned). ``close_timeout`` mirrors the operator's knob,
    floored at _LOOP_THREAD_JOIN_MIN_SECONDS.
    """
    # Deref at call time so the test patch is observed; broad except so
    # a phase-3 ``get_current_pid=None`` is a silent no-op, not an
    # unraisable-hook traceback.
    try:
        current_pid = get_current_pid()
    except Exception:
        return
    if current_pid != creator_pid:
        # Forked child: captured loop/thread belong to the parent.
        return
    # Resolve the inner via weakref; None if it was reclaimed in the
    # same GC pass (its own finalizer handles that case).
    inner: Any = None
    if inner_handle:
        inner_ref = inner_handle[0]
        if inner_ref is not None:
            inner_obj = inner_ref() if callable(inner_ref) else None
            if inner_obj is not None:
                inner = inner_obj
    # try/finally so the loop/thread teardown ALWAYS runs even if the
    # warning emission raises (e.g. ResourceWarning -> raise under
    # ``-W error::ResourceWarning``); otherwise the daemon thread would
    # linger, amplifying the very leak the warning surfaces.
    try:
        # User never called close() -> leak warning (stdlib parity).
        # Narrow RuntimeError suppress for the warnings-module-teardown
        # race; the None guards cover the interpreter-reload path.
        if closed_flag[0] is False and _warnings is not None and _contextlib is not None:
            with _contextlib.suppress(RuntimeError):
                # Sanitise the address so a dial_func that bypassed
                # parse_address can't split a journald record via LF.
                _warnings.warn(
                    f"Connection(address={_sanitize_for_log(str(address))!r}) "
                    f"was garbage-collected without close(); cleaning up "
                    f"event-loop thread. Call Connection.close() explicitly "
                    f"to avoid this warning.",
                    ResourceWarning,
                    stacklevel=2,
                )
    finally:
        # Disarm the inner's ResourceWarning finalizer before teardown
        # so the same GC sweep doesn't surface a second misleading
        # warning for the same socket.
        if inner is not None and _contextlib is not None:
            inner_closed_flag = getattr(inner, "_closed_flag", None)
            if isinstance(inner_closed_flag, list) and inner_closed_flag:
                inner_closed_flag[0] = True
            inner_finalizer = getattr(inner, "_finalizer", None)
            if inner_finalizer is not None:
                with _contextlib.suppress(Exception):
                    inner_finalizer.detach()
                with _contextlib.suppress(Exception):
                    inner._finalizer = None
            # Reap any pending invalidation-drain task before loop.stop
            # (FIFO ready queue): loop.close() does NOT cancel pending
            # tasks, so otherwise Task.__del__ writes "Task was
            # destroyed but it is pending" to stderr. Bounded
            # re-snapshot closes the snapshot-vs-fresh-publish race.
            if not loop.is_closed():
                resnapshot_cap = 3
                for _attempt in range(resnapshot_cap):
                    pending = getattr(inner, "_pending_drain", None)
                    with _contextlib.suppress(Exception):
                        inner._pending_drain = None
                    if pending is None or pending.done():
                        break

                    def _cancel_and_observe(target: asyncio.Task[Any]) -> None:
                        target.cancel()

                        def _observe(t: asyncio.Task[Any]) -> None:
                            if not t.cancelled():
                                with _contextlib.suppress(BaseException):
                                    t.exception()

                        target.add_done_callback(_observe)

                    with _contextlib.suppress(RuntimeError):
                        loop.call_soon_threadsafe(_cancel_and_observe, pending)
                else:
                    # Cap exhausted (pathological _invalidate feedback
                    # loop): final null-out + operator-visible warning.
                    with _contextlib.suppress(Exception):
                        inner._pending_drain = None
                    if _logger is not None:
                        _logger.warning(
                            "Connection._cleanup_loop_thread: inner._pending_drain still "
                            "set after %d re-snapshot iterations; cancelling residual task "
                            "to avoid 'Task was destroyed but it is pending' at GC.",
                            resnapshot_cap,
                        )
            # Close the inner writer before loop.stop (FIFO ready queue)
            # so FIN flushes orderly; otherwise StreamWriter.__del__
            # fires against a dead loop ("unclosed transport" warnings).
            # _safe_writer_close is idempotent.
            if not loop.is_closed():
                proto = getattr(inner, "_protocol", None)
                writer = getattr(proto, "_writer", None)
                if writer is not None:
                    with _contextlib.suppress(RuntimeError):
                        loop.call_soon_threadsafe(_safe_writer_close, writer)
        # Narrow suppression so a refactor-introduced bug still surfaces.
        try:
            if not loop.is_closed():
                loop.call_soon_threadsafe(loop.stop)
        except RuntimeError:  # pragma: no cover - race: loop closed mid-call
            if _logger is not None:
                _logger.debug(
                    "Connection._cleanup_loop_thread: loop.call_soon_threadsafe "
                    "raised RuntimeError (loop likely closed mid-call)",
                    exc_info=True,
                )
        if _contextlib is not None:
            with _contextlib.suppress(RuntimeError):
                thread.join(timeout=_join_budget_for_current_thread(close_timeout))
        try:
            if not loop.is_closed():
                loop.close()
        except RuntimeError:  # pragma: no cover - race: loop restarted mid-finalize
            if _logger is not None:
                _logger.debug(
                    "Connection._cleanup_loop_thread: loop.close() raised "
                    "RuntimeError (loop likely restarted mid-finalize)",
                    exc_info=True,
                )


_OWNER_INTERNAL_BUSY: object = object()
"""Parked in ``_transaction_owner`` during the ctxmgr's COMMIT/ROLLBACK
RTT: keeps the slot non-None so a sibling thread can't reserve it
mid-round-trip, while ``_tx_owner == current_ident`` reads in commit()/
rollback() return False against it (bypassing the stray-commit reject)."""


class Connection:
    """PEP 249 compliant database connection.

    Autocommit-by-default: every statement commits at the server unless
    wrapped in an explicit ``BEGIN`` (diverges from PEP 249 §6 / stdlib
    sqlite3; see the README). This also applies to ``executemany`` — a
    mid-batch cancel without a surrounding BEGIN persists the completed
    iterations.

    Thread-affinity: ``threadsafety`` is 2 (threads may share the module
    and connections, but not cursors). ``check_same_thread=True``
    (default) confines a connection to its creating thread — foreign-thread
    calls raise ProgrammingError; ``check_same_thread=False`` relaxes the
    cross-thread check (the wire is already serialised by ``_op_lock``).
    Under it, the contract matches stdlib sqlite3: **share connections
    across threads; create one cursor per thread** (per-cursor result
    state is unlocked — sharing a cursor gives torn reads). Pattern:

        conn = dqlitedbapi.connect(addr, check_same_thread=False)
        def worker():
            cur = conn.cursor()  # each thread gets its own cursor
            cur.execute("SELECT 1")
            return cur.fetchall()

    ``close()`` keeps the cross-thread check unconditionally (it tears
    down the daemon loop thread synchronously); use
    ``force_close_transport()`` from non-creator threads. The fork
    check (``_creator_pid``) is NEVER relaxed — cross-process use is a
    hard InterfaceError in both modes. Under check_same_thread=False,
    ``messages`` is best-effort (interleaved clears) and BUSY retries
    release the wire lock between attempts (wrap in a transaction to
    keep a retry atomic).
    """

    # PEP 249 optional extension: expose exception classes as class
    # attrs so generic code can write ``except conn.Error:``.
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
    # dqlite-specific OperationalError subclass; mirrored here for
    # introspection symmetry with the PEP 249 names.
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
        check_same_thread: bool = True,
        session_mode: str | None = None,
    ) -> None:
        """Initialize connection (does not connect yet).

        Args:
            address: Node address in "host:port" format.
            database: Database name to open.
            timeout: Per-RPC-phase budget (seconds); a call can take
                up to N x this end-to-end. Wrap in asyncio.timeout for
                a wall-clock deadline.
            max_total_rows: Cumulative row cap across continuation
                frames per query; ``None`` disables.
            max_continuation_frames: Per-query frame cap, bounding
                decode work a hostile server can inflict via 1-row
                frames.
            trust_server_heartbeat: Widen the per-read deadline to the
                server heartbeat (300 s hard cap). Default False.
            close_timeout: Transport-drain budget for ``close()``.
            dial_timeout: Per-TCP-connect budget; ``None`` collapses
                onto ``timeout``. Smaller value fast-fails dead peers.
            attempt_timeout: Per-attempt dial+handshake+first-RPC
                envelope; ``None`` collapses onto ``timeout``. Smaller
                bounds the leader-sweep against slow-handshaking peers.
            dial_func: Async dialer replacing the default TCP path
                (TLS, unix-socket, custom KEEPALIVE). See
                :data:`dqliteclient.DialFunc`.
            busy_timeout: Cumulative seconds retrying BUSY before
                raising (SQLite curve). Default 5.0 = stdlib parity;
                ``0`` disables. ``PRAGMA busy_timeout`` writes the same
                field (intercepted at the Cursor layer).
            check_same_thread: ``True`` (default) confines calls to the
                creator thread; ``False`` shares across threads (one
                cursor per thread). ``close()`` and the fork check are
                never relaxed. See the class docstring.
        """
        _validate_timeout(timeout)
        _validate_close_timeout(close_timeout)
        if dial_timeout is not None:
            _validate_timeout(dial_timeout)
        if attempt_timeout is not None:
            _validate_timeout(attempt_timeout)
        # busy_timeout: non-negative finite; reject bool (would coerce
        # True->1.0). Allows zero (stdlib parity for "no retry").
        if isinstance(busy_timeout, bool) or not isinstance(busy_timeout, (int, float)):
            raise TypeError(
                f"busy_timeout must be a number (seconds); got {type(busy_timeout).__name__}"
            )
        if not math.isfinite(busy_timeout) or busy_timeout < 0:
            raise ValueError(
                f"busy_timeout must be a non-negative finite number; got {busy_timeout}"
            )
        # Strict bool: reject 0/1 (isinstance(True, int) is True).
        if not isinstance(check_same_thread, bool):
            raise ProgrammingError(
                f"check_same_thread must be bool; got {type(check_same_thread).__name__}"
            )
        # Validate at config-load site (mirrors DqliteConnection);
        # map to InterfaceError per PEP 249.
        if not isinstance(address, str):
            raise InterfaceError(
                f"address must be a 'host:port' string, got {type(address).__name__}"
            )
        if not isinstance(database, str):
            raise InterfaceError(f"database must be a str, got {type(database).__name__}")
        if not database:
            raise InterfaceError("database must be a non-empty string")
        if database != database.strip():
            # Surrounding whitespace has implementation-defined OPEN
            # semantics server-side (may create a distinct db or
            # silently mismatch a later open); canonicalise here.
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
        # ``None`` -> wire default (64 MiB); wire layer revalidates, so
        # the dbapi just stores and forwards (single source of truth).
        self._max_message_size = max_message_size
        self._trust_server_heartbeat = trust_server_heartbeat
        self._close_timeout = close_timeout
        self._dial_timeout = dial_timeout
        self._attempt_timeout = attempt_timeout
        self._dial_func = dial_func
        # Float seconds; shares the backing field with the PRAGMA setter.
        self._busy_timeout: float = float(busy_timeout)
        self._check_same_thread: bool = check_same_thread
        # session_mode: immediate (default; rewrites bare BEGIN to
        # BEGIN IMMEDIATE to dodge SQLITE_BUSY_SNAPSHOT) / deferred /
        # exclusive / read_only (DEFERRED + PRAGMA query_only=1 at
        # connect). ``None`` consults DQLITE_SESSION_MODE, else
        # immediate. Two slots: ``_dqlite_session_mode`` is the live
        # value (SA may override per-checkout);
        # ``_dqlite_session_mode_default`` is the construct-time default
        # used to restore on pool checkin (never reassigned).
        from dqlitedbapi._pragma_intercept import (
            session_mode_default_from_env,
            validate_session_mode,
        )

        if session_mode is None:
            resolved_session_mode = session_mode_default_from_env()
        else:
            resolved_session_mode = validate_session_mode(session_mode)
        self._dqlite_session_mode: str = resolved_session_mode
        self._dqlite_session_mode_default: str = resolved_session_mode
        self._async_conn: DqliteConnection | None = None
        self._closed = False
        # None means plain tuples; new cursors inherit this.
        self._row_factory: RowFactory | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._loop_lock = threading.Lock()
        self._op_lock = threading.Lock()
        # Owner-thread of the current _op_lock holder (set under the
        # lock in _run_sync) so close()'s same-thread re-entry bypass
        # distinguishes "WE hold it" from a sibling thread holding it
        # under check_same_thread=False — the bare .locked() probe
        # would release the sibling's lock and corrupt protocol state.
        self._op_lock_owner: int | None = None
        self._connect_lock: asyncio.Lock | None = None
        self._creator_thread = threading.get_ident()
        # Fork-after-init is unsupported (shared socket, dead daemon
        # thread, parent-loop-bound primitives); store the pid so
        # cross-fork use raises InterfaceError instead of corrupting.
        self._creator_pid = os.getpid()
        # PEP 249 §13: values are Exception instances, not strings.
        self.messages: list[tuple[type[Exception], Exception]] = []
        # OS thread id of the active ``transaction()`` body owner;
        # commit/rollback reject from inside so the ctxmgr keeps
        # boundary control. Async sibling stores a Task instead.
        self._transaction_owner: int | None = None
        # Guards the transaction() owner-check+reserve composite under
        # check_same_thread=False so two threads can't both enter.
        # Outermost/brief; never held while acquiring _op_lock.
        self._state_lock = threading.Lock()
        # Mutable flag the finalizer reads; a list avoids the finalizer
        # closing over ``self`` and preventing GC.
        self._closed_flag: list[bool] = [False]
        # Box for late-publishing the inner handle into the finalizer's
        # captured args (mutated by _get_async_connection to a
        # weakref.ref). Not cleared on explicit close (finalizer is
        # detached first); the ``if inner is not None`` guard handles
        # a resolved-to-None ref.
        self._inner_finalize_handle: list[Any] = []
        self._finalizer: weakref.finalize[Any, Any] | None = None
        # Tracked weakly so close() can scrub cursor state (else
        # buffered fetches answer from stale in-memory rows).
        self._cursors: weakref.WeakSet[Cursor] = weakref.WeakSet()

    def _check_thread(self) -> None:
        """InterfaceError on fork (always); ProgrammingError on
        cross-thread use when check_same_thread=True. The fork check is
        never relaxed (inherited socket, dead loop thread)."""
        if get_current_pid() != self._creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {self._creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        # Default True so __new__-built fixtures fall through to strict.
        if not getattr(self, "_check_same_thread", True):
            return
        current = threading.get_ident()
        if current != self._creator_thread:
            raise ProgrammingError(
                f"Connection objects created in a thread can only be used in that "
                f"same thread. The object was created in thread id "
                f"{self._creator_thread} and this is thread id {current}. "
                f"Pass check_same_thread=False at connect() time to allow "
                f"cross-thread use."
            )

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Ensure a dedicated event loop runs in a background thread, so
        sync methods work even inside a running async context.

        Registers a finalizer on first loop creation so a GC'd
        Connection still reaps its thread. Carries a defence-in-depth
        fork guard so a caller bypassing _check_thread doesn't run
        against the parent's undrained loop in a child.
        """
        if get_current_pid() != self._creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {self._creator_pid}, "
                f"current pid {get_current_pid()})"
            )
        # Snapshot before the second access so a concurrent close()
        # nulling _loop can't make .is_closed() raise AttributeError.
        snapshot = self._loop
        if snapshot is not None and not snapshot.is_closed():
            return snapshot
        with self._loop_lock:
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
                self._thread.start()
                # Capture primitives only — closing over self would
                # keep the Connection alive. _inner_finalize_handle is
                # populated later with a weakref.ref(inner).
                self._finalizer = weakref.finalize(
                    self,
                    _cleanup_loop_thread,
                    self._loop,
                    self._thread,
                    self._closed_flag,
                    self._address,
                    self._creator_pid,
                    self._close_timeout,
                    self._inner_finalize_handle,
                )
        return self._loop

    def _run_sync[T](self, coro: Coroutine[Any, Any, T]) -> T:
        """Run an async coroutine from sync code on the background loop,
        serialised by _op_lock (one wire op at a time).

        On sync-side timeout, cancels the future AND invalidates the
        connection (the coro may have half-written the socket, so the
        next op must reconnect). Lock acquire is bounded by
        ``self._timeout`` so a same-thread signal-handler re-entry
        raises cleanly instead of deadlocking the non-reentrant lock.
        """
        # The acquire, the owner-stamp, and the body all sit under one
        # try/finally so a SIGINT-delivered KeyboardInterrupt/SystemExit
        # raised anywhere after the lock is held — including in the
        # owner-stamp gap — cannot escape with the lock latched. `acquired`
        # is bound first so the finally never NameErrors on the inner arm
        # below (which re-raises before `acquired` is assigned).
        acquired = False
        try:
            # acquire(timeout=...) is SIGINT-interruptible: a KI/SystemExit
            # can land between acquire returning True and the STORE_FAST,
            # leaving the lock held with `acquired` still False — so the
            # inner arm best-effort releases it (the outer finally skips
            # release when `acquired` is False). A prior op in flight owns
            # _in_use, wedging the connection; schedule a defensive
            # _invalidate, gated on _in_use so a KI on a quiet acquire
            # doesn't invalidate gratuitously.
            try:
                acquired = self._op_lock.acquire(timeout=self._timeout)
            except (KeyboardInterrupt, SystemExit):
                with contextlib.suppress(RuntimeError):
                    self._op_lock_owner = None
                    self._op_lock.release()
                # Close the never-scheduled coro to avoid a warning.
                coro.close()
                # Synchronously null _async_conn so a retry from the signal
                # handler doesn't hit "another operation is in progress" on
                # the stale in-use conn (the queued _invalidate only lands
                # when the slow read yields). GIL-atomic STORE_ATTR; the
                # loop coro keeps its own ref and reaps via _invalidate.
                dying = self._async_conn
                if dying is not None and self._loop is not None and dying._in_use:
                    self._async_conn = None
                    with contextlib.suppress(RuntimeError):
                        self._loop.call_soon_threadsafe(
                            dying._invalidate,
                            InterfaceError("operation interrupted during op-lock acquire"),
                        )
                raise
            # Stamp owner on the acquired arm so the close() bypass probe
            # can read it. GIL-atomic.
            if acquired:
                self._op_lock_owner = threading.get_ident()
            if not acquired:
                coro.close()
                # OperationalError (not InterfaceError) so SA's
                # is_disconnect recycles the contended slot rather than
                # treating it as a programmer bug. Canonical prefix.
                raise OperationalError(
                    f"op_lock acquire timed out after {self._timeout}s waiting "
                    "for another operation on this connection to release "
                    f"(id={id(self)}; may indicate re-entry from a signal handler "
                    "or concurrent use from another thread). Treat as a transient "
                    "condition and retry on a fresh connection.",
                    code=None,
                )
            # Close coro if _ensure_loop raises before scheduling (OS
            # thread-start / FD-exhaustion) so it doesn't warn at GC.
            # Distinct from the closed-loop arm below so an OS-resource
            # error isn't misrouted through the "event loop closed" remap.
            try:
                loop = self._ensure_loop()
            except BaseException:
                coro.close()
                raise
            # Per-phase budget x N: the async surface wraps each RPC in
            # its own asyncio.timeout with no cross-RPC ceiling, so the
            # single sync window must absorb all phases.
            sync_timeout = _SYNC_PHASES_MULTIPLIER * self._timeout
            # future=None sentinel so the KI cleanup arm is deterministic
            # whether the schedule raised or landed before the result wait.
            future: concurrent.futures.Future[T] | None = None
            try:
                try:
                    future = asyncio.run_coroutine_threadsafe(coro, loop)
                except RuntimeError as e:
                    # Loop closed between _ensure_loop and the schedule
                    # (sibling do_terminate / SIGTERM shutdown). Close
                    # the coro and remap the closed-loop case to
                    # OperationalError; let other RuntimeErrors (e.g.
                    # the non-thread-safe programmer bug) propagate.
                    coro.close()
                    if "Event loop is closed" not in str(e):
                        raise
                    raise OperationalError(
                        f"event loop closed before coroutine could be scheduled: {e}"
                    ) from e
                # Future.result() is a happens-before barrier; loop-thread
                # writes are visible here.
                return future.result(timeout=sync_timeout)
            except TimeoutError as e:
                # Only future.result can raise this, so future is bound;
                # assert is for mypy.
                assert future is not None
                # The coro may have completed between result() raising
                # and our cancel landing. If so the op persisted —
                # honour it instead of raising (a retry would duplicate
                # a non-idempotent write).
                recovered_error: BaseException | None = None
                if (
                    future.done() and not future.cancelled()
                ):  # pragma: no cover - race: future completes mid-timeout
                    try:
                        return future.result(timeout=0)
                    except BaseException as recovered:
                        # Coro completed with its own error; chain it via
                        # __cause__ so the user sees the real failure, not
                        # an opaque "timed out".
                        recovered_error = recovered
                # Coro completed -> connection is healthy (_in_use already
                # cleared); re-raise immediately, skipping invalidate to
                # avoid a reconnect storm under tight tuning.
                if recovered_error is not None:
                    if isinstance(recovered_error, asyncio.CancelledError):
                        raise OperationalError(
                            "Operation cancelled in async context (no meaning in sync caller)"
                        ) from recovered_error
                    # bare raise preserves causality vs the timer (see
                    # the trailing arm's noqa rationale)
                    raise recovered_error  # noqa: B904
                future.cancel()
                # Synchronously null _async_conn so the caller's retry
                # doesn't hit "another operation is in progress" while
                # the slow read keeps _in_use latched. Reached only on a
                # genuine timeout (the recovered branch above re-raised).
                dying = self._async_conn
                self._async_conn = None
                # Poison the wire (the coro may have half-written a
                # request); fire-and-forget on the loop thread.
                if dying is not None:
                    with contextlib.suppress(
                        RuntimeError
                    ):  # pragma: no cover - race: loop closing mid-schedule
                        loop.call_soon_threadsafe(
                            dying._invalidate,
                            OperationalError(
                                f"sync timeout after {sync_timeout}s "
                                f"({_SYNC_PHASES_MULTIPLIER} × per-phase budget "
                                f"of {self._timeout}s)"
                            ),
                        )
                # Bounded wait for the cancelled coro to unwind so the
                # next sync call doesn't race its _in_use; _invalidate
                # above is the safety net for a stuck coro.
                try:
                    future.result(timeout=1.0)
                except (
                    concurrent.futures.CancelledError,
                    concurrent.futures.TimeoutError,
                ):
                    pass
                except Exception:
                    # Cancelled coro died with an unexpected error
                    # (cleanup bug); DEBUG-log it, outer error still wins.
                    logger.debug(
                        "sync timeout: unexpected error during bounded cancel-wait",
                        exc_info=True,
                    )
                raise OperationalError(
                    f"Operation timed out after {sync_timeout} seconds "
                    f"({_SYNC_PHASES_MULTIPLIER} × per-phase budget of "
                    f"{self._timeout}s)"
                ) from e
            except (KeyboardInterrupt, SystemExit):
                # Signal raised while blocked on Future.result; the coro
                # still runs and owns _in_use, wedging the connection.
                # Cancel + _invalidate + bounded-wait, then re-raise the
                # signal (no ``from``). Narrowed to KI/SystemExit: normal
                # Exceptions re-raised by Future.result propagate the
                # standard path and must NOT invalidate.
                #
                # future may be None if the signal landed before the
                # schedule returned; close the unscheduled coro and
                # re-raise (else future.done() below raises AttributeError).
                if future is None:
                    coro.close()
                    raise
                # Race-recovery (mirror of the timeout arm): if the coro
                # already resolved, there's nothing wedged — drain the
                # captured exception (avoid "never retrieved") and re-raise.
                if future.done() and not future.cancelled():
                    future.cancel()
                    with contextlib.suppress(BaseException):
                        future.result(timeout=0)
                    raise
                future.cancel()
                # Null _async_conn so the next op reconnects regardless of
                # loop drain state (else a slow read keeps _in_use latched).
                dying = self._async_conn
                self._async_conn = None
                if dying is not None:
                    with contextlib.suppress(RuntimeError):
                        loop.call_soon_threadsafe(
                            dying._invalidate,
                            InterfaceError("operation interrupted"),
                        )
                # Narrow suppress so a SECOND KI inside the bounded wait
                # still escalates; the first KI re-raises below.
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
            if acquired:
                # Clear owner before release so a concurrent close bypass
                # probe can't see "owner == me" after release.
                self._op_lock_owner = None
                # Suppress RuntimeError: the same-thread close() bypass
                # may have already released the lock from a signal
                # handler, and a double-release would replace the
                # in-flight KI/SystemExit with the wrong-layer error.
                with contextlib.suppress(RuntimeError):
                    self._op_lock.release()

    async def _get_async_connection(self) -> DqliteConnection:
        """Get or create the underlying async connection."""
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")

        if self._async_conn is not None:
            return self._async_conn

        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()

        # Snapshot before the async-with: a foreign-thread
        # force_close_transport nulling _connect_lock would otherwise
        # raise AttributeError outside the dbapi.Error tree.
        connect_lock = self._connect_lock
        if connect_lock is None:
            raise InterfaceError(f"Connection is closed (id={id(self)})")

        async with connect_lock:
            if self._async_conn is not None:  # pragma: no cover - race: peer built conn mid-lock
                return self._async_conn

            self._async_conn = await _build_and_connect(
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
            # Late-publish the inner handle into the finalizer's captured
            # args (registered before the inner existed). weakref.ref
            # avoids an outer<->inner cycle that would block GC.
            with contextlib.suppress(Exception):
                self._inner_finalize_handle[:] = [weakref.ref(self._async_conn)]
            # Snapshot under the lock so a foreign-thread close nulling
            # _async_conn can't return None to the caller.
            inner = self._async_conn

        if inner is None:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        return inner

    def connect(self) -> None:
        """Eagerly establish the TCP session (optional — the connection
        is lazy). Use to fail-fast when the cluster is unreachable."""
        # Thread check before the messages clear so a cross-thread caller
        # doesn't scrub the owner thread's list before the diagnostic.
        self._check_thread()
        del self.messages[:]
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._run_sync(self._get_async_connection())

    def _cascade_cursors(self) -> None:
        """Cascade close-state to every tracked cursor (stdlib parity),
        including the PEP 249 §6.4 messages clear. ``weakref.proxy`` swap
        is suppress(TypeError) so a double-cascade is a no-op."""
        # Snapshot under the lock, scrub outside it (the scrub doesn't
        # re-enter the WeakSet); WeakSet ops aren't PEP 703 thread-safe.
        _state_lock = getattr(self, "_state_lock", None) or threading.Lock()
        with _state_lock:
            snapshot = list(self._cursors)
        try:
            for cur in snapshot:
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
            with _state_lock:
                self._cursors.clear()

    def close(self) -> None:
        """Close the connection."""
        # Idempotent (PEP 249 §6.1): closed-check before the thread guard
        # so a re-close from a non-creator thread is a no-op, not a raise.
        if self._closed:
            return
        # Fork-after-init: the inherited loop/socket are the parent's;
        # _close_async would deadlock or FIN the parent's sockets. Flip
        # the flags + drop parent-loop-bound refs (else the dead daemon
        # Thread pins them in threading._active) and skip teardown.
        if get_current_pid() != self._creator_pid:
            self._closed = True
            self._closed_flag[0] = True
            self._cascade_cursors()
            if self._finalizer is not None:
                self._finalizer.detach()
                self._finalizer = None
            self._async_conn = None
            self._loop = None
            self._thread = None
            self._connect_lock = None
            self._transaction_owner = None
            return
        # Cross-thread check is STRICT even under check_same_thread=False
        # (close() tears down the daemon loop thread synchronously);
        # foreign threads use force_close_transport(). Inline the bare
        # check since _check_thread() would relax under the flag.
        current = threading.get_ident()
        if current != self._creator_thread:
            raise ProgrammingError(
                f"Connection.close() must be called from the creator "
                f"thread (id={self._creator_thread}); got thread "
                f"id={current}. This applies even under "
                f"check_same_thread=False because close() tears down "
                f"the daemon loop thread synchronously. Use "
                f"force_close_transport() from non-creator threads "
                f"(SA's pool recycle path uses this)."
            )
        # Clear messages after the thread check (so a foreign-thread
        # first-close doesn't scrub the owner's list pre-diagnostic).
        del self.messages[:]
        self._closed = True
        # Flag the finalizer reads to suppress its ResourceWarning.
        self._closed_flag[0] = True
        self._cascade_cursors()
        # Detach the finalizer (keeping it would double-stop the loop).
        if self._finalizer is not None:
            self._finalizer.detach()
            self._finalizer = None
        try:
            if self._loop is not None and not self._loop.is_closed():
                # Same-thread re-entry (signal handler ran close() while
                # a prior _run_sync is parked holding _op_lock): skip the
                # bounded acquire (it would block self._timeout) and just
                # close the un-awaited coro. The finally below still
                # reaps the loop/thread. Probe _op_lock_owner, not
                # .locked(), so a sibling's lock under tier-2 isn't
                # mistaken for ours and released (corrupting the wire).
                if getattr(self, "_op_lock_owner", None) == threading.get_ident():
                    # A SIGINT may have left the lock latched (KI before
                    # release ran); best-effort release so the next
                    # _run_sync doesn't deadlock.
                    with contextlib.suppress(RuntimeError):
                        self._op_lock_owner = None
                        self._op_lock.release()
                    coro = self._close_async()
                    coro.close()
                else:
                    # Re-raise the op_lock-acquire-timeout (matches the
                    # async sibling) so SA's pool sees the recycle event;
                    # genuine drain faults stay swallowed (close is
                    # best-effort, the conn is closed by the finally).
                    try:
                        self._run_sync(self._close_async())
                    except OperationalError:
                        raise
                    except Exception:
                        pass
        finally:
            with self._loop_lock:
                # Always null _async_conn (the suppress arms above skip
                # _close_async's own finally). Best-effort writer.close
                # drives FIN synchronously before loop.close, else the
                # transport FD lingers to GC ("unclosed transport").
                if self._async_conn is not None:
                    inner = self._async_conn
                    proto = getattr(inner, "_protocol", None)
                    writer = getattr(proto, "_writer", None) if proto is not None else None
                    if writer is not None and self._loop is not None and not self._loop.is_closed():
                        # writer.close() isn't thread-safe; schedule on
                        # the loop (FIFO before the loop.stop below).
                        with contextlib.suppress(RuntimeError):
                            self._loop.call_soon_threadsafe(_safe_writer_close, writer)
                    elif writer is not None:
                        # Loop gone — sync close is the best we can do.
                        with contextlib.suppress(Exception):
                            writer.close()
                    self._async_conn = None
                if self._loop is not None and not self._loop.is_closed():
                    # is_closed() is TOCTOU vs a concurrent finalizer;
                    # suppress the closed-loop RuntimeError.
                    with contextlib.suppress(RuntimeError):
                        self._loop.call_soon_threadsafe(self._loop.stop)
                    if self._thread is not None:
                        # Honour close_timeout, floored so a tight value
                        # still lets loop.stop land on a non-stuck loop.
                        self._thread.join(
                            timeout=max(self._close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS)
                        )
                    # loop.close() raises if the loop is still alive
                    # (read outlasted the join); swallow + drop refs so
                    # the next GC re-runs the finalizer's reap.
                    try:
                        self._loop.close()
                    except RuntimeError:
                        logger.debug(
                            "Connection.close: loop.close raised RuntimeError "
                            "(loop thread did not exit within %s s); refs cleared",
                            max(self._close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS),
                            exc_info=True,
                        )
                    self._loop = None
                    self._thread = None
                # Drop the loop-bound asyncio.Lock; _get_async_connection
                # rebuilds it against the next loop.
                self._connect_lock = None

    def force_close_transport(self) -> None:
        """Force-close the socket transport without awaiting any
        in-flight RPC. Synchronous, bounded by ``close_timeout``,
        idempotent.

        For last-resort shutdown where close() would block on a stuck
        read (SA do_terminate under partition+SIGTERM). Unlike close():
        skips the _run_sync await, joins on close_timeout, and runs with
        no _check_thread / no _op_lock so it works from finalize threads
        and signal handlers.
        """
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        if self._closed:
            return
        self._closed = True
        self._closed_flag[0] = True
        # Fork-after-init: same shape as close()'s pid guard.
        if get_current_pid() != self._creator_pid:
            self._cascade_cursors()
            if self._finalizer is not None:
                self._finalizer.detach()
                self._finalizer = None
            self._async_conn = None
            self._loop = None
            self._thread = None
            self._connect_lock = None
            self._transaction_owner = None
            return
        self._cascade_cursors()
        if self._finalizer is not None:
            self._finalizer.detach()
            self._finalizer = None
        with self._loop_lock:
            inner = self._async_conn
            self._async_conn = None
            loop = self._loop
            # Disarm the inner's ResourceWarning finalizer before the
            # loop closes the writer, else it warns "GC'd without close"
            # on the conn we're explicitly closing.
            if inner is not None:
                inner_closed_flag = getattr(inner, "_closed_flag", None)
                if isinstance(inner_closed_flag, list) and inner_closed_flag:
                    inner_closed_flag[0] = True
                inner_finalizer = getattr(inner, "_finalizer", None)
                if inner_finalizer is not None:
                    with contextlib.suppress(Exception):
                        inner_finalizer.detach()
                    inner._finalizer = None
            if loop is not None and not loop.is_closed():
                if inner is not None:
                    # Reap any pending invalidation-drain task before
                    # loop.stop (FIFO): else Task.__del__ emits "Task was
                    # destroyed but it is pending". Bounded re-snapshot
                    # because a loop coro can publish a fresh drain task
                    # between our snapshot and null.
                    resnapshot_cap = 3
                    for _attempt in range(resnapshot_cap):
                        pending = getattr(inner, "_pending_drain", None)
                        with contextlib.suppress(Exception):
                            inner._pending_drain = None
                        if pending is None or pending.done():
                            break

                        def _cancel_and_observe(target: asyncio.Task[Any]) -> None:
                            target.cancel()

                            def _observe(t: asyncio.Task[Any]) -> None:
                                if not t.cancelled():
                                    with contextlib.suppress(BaseException):
                                        t.exception()

                            target.add_done_callback(_observe)

                        with contextlib.suppress(RuntimeError):
                            loop.call_soon_threadsafe(_cancel_and_observe, pending)
                    else:
                        # Cap exhausted (racing _invalidate keeps
                        # republishing): final null-out + warning.
                        with contextlib.suppress(Exception):
                            inner._pending_drain = None
                        logger.warning(
                            "Connection.force_close_transport: inner._pending_drain still "
                            "set after %d re-snapshot iterations; cancelling residual task "
                            "to avoid 'Task was destroyed but it is pending' at GC. This "
                            "indicates a pathological _invalidate feedback loop on inner "
                            "conn.",
                            resnapshot_cap,
                        )
                    proto = getattr(inner, "_protocol", None)
                    writer = getattr(proto, "_writer", None) if proto is not None else None
                    if writer is not None:
                        # writer.close() isn't thread-safe; schedule it
                        # (FIFO before the loop.stop below).
                        with contextlib.suppress(RuntimeError):
                            loop.call_soon_threadsafe(_safe_writer_close, writer)
                with contextlib.suppress(RuntimeError):
                    loop.call_soon_threadsafe(loop.stop)
                # Shortened budget when running on a thread that hosts a
                # loop (SA do_terminate via greenlet); full budget else.
                join_budget = _join_budget_for_current_thread(self._close_timeout)
                if self._thread is not None:
                    self._thread.join(timeout=join_budget)
                try:
                    loop.close()
                except RuntimeError:
                    logger.debug(
                        "Connection.force_close_transport: loop.close raised "
                        "RuntimeError (loop thread did not exit within %s s); "
                        "refs cleared",
                        join_budget,
                        exc_info=True,
                    )
                self._loop = None
                self._thread = None
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
        """Whether an explicit transaction is open.

        Diverges from stdlib: returns False on a closed/never-connected
        connection instead of raising, so it's safe in shutdown paths.
        The closed short-circuit runs before the thread check; under
        check_same_thread=True a cross-thread live read raises, under
        False it returns the value (the fork check is unconditional).
        """
        # Snapshot so a concurrent close() nulling _async_conn can't land
        # between the None-check and the read.
        conn = self._async_conn
        if conn is None or self._closed:
            return False
        self._check_thread()
        return bool(conn.in_transaction)

    @property
    def autocommit(self) -> "bool | int":
        """``True`` — dqlite is autocommit-by-default (stdlib 3.12+ /
        psycopg parity). Setter stores and round-trips ``True`` or the
        ``-1`` LEGACY_TRANSACTION_CONTROL sentinel (both no-op the wire);
        any other value raises NotSupportedError. Raises InterfaceError
        on a closed connection or from a forked child.
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
        # Thread check before the messages clear (no-op path is still an
        # attempt). suppress(AttributeError) for __new__-built fixtures.
        self._check_thread()
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        with contextlib.suppress(AttributeError):
            if self._closed:
                raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Accept True and the -1 LEGACY_TRANSACTION_CONTROL sentinel
        # (both no-op the wire; stored so the getter round-trips). The
        # inner _async_conn slot is intentionally not mirrored (pinned
        # by test_property_setters_do_not_mirror_to_inner_async_conn).
        if value is True:
            self._autocommit_value: bool | int = value
            return
        # Tight exact-int -1 gate (stdlib discipline): loose == -1 would
        # accept -1.0 / Decimal and break isinstance(autocommit, int).
        # Canonicalise-store as int(-1).
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
        """stdlib pre-3.12 isolation_level-parity surface. Setter stores
        and round-trips None / "" / DEFERRED / IMMEDIATE / EXCLUSIVE
        (all no-op the wire), preserving the cross-driver
        ``dst.isolation_level = src.isolation_level`` idiom. Default
        None. Raises InterfaceError on a closed connection or forked
        child.
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
        # Thread check first; suppress(AttributeError) for fixtures.
        self._check_thread()
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        with contextlib.suppress(AttributeError):
            if self._closed:
                raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Accept the stdlib pre-3.12 set (None/""/DEFERRED/IMMEDIATE/
        # EXCLUSIVE) as no-ops; store so the getter round-trips. Inner
        # _async_conn slot is intentionally not mirrored (pinned by
        # test_property_setters_do_not_mirror_to_inner_async_conn).
        # Invalid values raise ProgrammingError (caller misuse), not
        # NotSupportedError.
        if value is None:
            self._isolation_level_value: str | None = value
            return
        if isinstance(value, str) and value.upper() in _STDLIB_IMPLICIT_TX_VALUES:
            # Normalize case like stdlib's uppercased read-back ('deferred' -> 'DEFERRED').
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

    def commit(self) -> None:
        """Commit any pending transaction.

        Silent no-op if never used or if the server reports "no active
        transaction" (the common case here, since every statement
        auto-commits unless an explicit BEGIN ran). Caveat: a leader
        flip mid-COMMIT raises with a LEADER_ERROR_CODES code and the
        write may or may not have persisted — use idempotent DML before
        retrying.
        """
        # Thread check before the messages clear (scopes to the owner).
        self._check_thread()
        del self.messages[:]
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Reject stray commit() inside ``with conn.transaction():`` —
        # the ctxmgr owns boundaries. getattr for __new__-built fixtures.
        _tx_owner = getattr(self, "_transaction_owner", None)
        if _tx_owner is not None and _tx_owner == threading.get_ident():
            raise InterfaceError(
                "commit() cannot be issued inside conn.transaction(); "
                "the context manager owns transaction boundaries — "
                "exit the ``with`` block first."
            )
        # Snapshot _async_conn so a foreign-thread force_close racing the
        # reads below can't produce a silent no-op against an
        # invalidated connection.
        inner = self._async_conn
        if inner is None:
            return
        # Cancel-after-invalidate: a prior cancelled commit invalidates
        # the conn and clears in_transaction, so raise BEFORE the
        # in_transaction short-circuit (else partial-commit ambiguity
        # is hidden as a silent return).
        if getattr(inner, "_protocol", "_sentinel") is None:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}); reconnect before "
                "retrying commit / rollback. The prior call may have "
                "reached the leader before cancel landed; server-side "
                "transaction state is ambiguous."
            )
        # Skip the wire round-trip when no transaction is active (stdlib
        # parity); in_transaction also covers the untracked-SAVEPOINT case.
        if not getattr(inner, "in_transaction", False):
            return
        # Busy-retry: COMMIT is SQL too, so a contended COMMIT survives
        # like an INSERT.
        from dqlitedbapi._busy_retry import _resolve_busy_timeout_seconds, retry_sync_on_busy

        retry_sync_on_busy(
            _resolve_busy_timeout_seconds(self),
            self._run_sync,
            self._commit_async,
        )

    async def _commit_async(self) -> None:
        if self._async_conn is None:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # In-lock clear (atomic with the op) closes the window where a
        # sibling could write messages after commit()'s pre-lock clear.
        del self.messages[:]
        try:
            # Via _call_client so client errors surface as PEP 249 Errors.
            await _call_client(self._async_conn.execute("COMMIT"))
        except OperationalError as e:
            if _is_no_transaction_error(e):
                return
            # Leader flip mid-COMMIT -> in-doubt Raft entry.
            if e.code in _LEADER_ERROR_CODES:
                raise AmbiguousCommitError(
                    "ambiguous commit: leader flipped during COMMIT; "
                    "the write may or may not have been persisted. "
                    f"Original: {e}",
                    code=e.code,
                    raw_message=getattr(e, "raw_message", None),
                ) from e
            raise

    def rollback(self) -> None:
        """Roll back any pending transaction. Same silent-success
        contract as :meth:`commit` for "no active transaction" / never-
        used connections."""
        self._check_thread()
        del self.messages[:]
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Reject stray rollback() inside transaction(); getattr for fixtures.
        _tx_owner = getattr(self, "_transaction_owner", None)
        if _tx_owner is not None and _tx_owner == threading.get_ident():
            raise InterfaceError(
                "rollback() cannot be issued inside conn.transaction(); "
                "the context manager owns transaction boundaries — "
                "raise from inside the ``with`` block to trigger "
                "rollback-at-exit, or exit the block first."
            )
        # Snapshot _async_conn — see commit() for the race rationale.
        inner = self._async_conn
        if inner is None:
            return
        # Cancel-after-invalidate guard — see commit().
        if getattr(inner, "_protocol", "_sentinel") is None:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}); reconnect before "
                "retrying commit / rollback. The prior call may have "
                "reached the leader before cancel landed; server-side "
                "transaction state is ambiguous."
            )
        # See commit() — same short-circuit saves a round-trip.
        if not getattr(inner, "in_transaction", False):
            return
        # Busy-retry (ROLLBACK is SQL too). Safe vs cancel-after-
        # invalidate: retry fires only on SQLITE_BUSY, which cancel
        # never produces.
        from dqlitedbapi._busy_retry import _resolve_busy_timeout_seconds, retry_sync_on_busy

        retry_sync_on_busy(
            _resolve_busy_timeout_seconds(self),
            self._run_sync,
            self._rollback_async,
        )

    async def _rollback_async(self) -> None:
        if self._async_conn is None:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        # In-lock clear; see _commit_async.
        del self.messages[:]
        try:
            await _call_client(self._async_conn.execute("ROLLBACK"))
        except OperationalError as e:
            if not _is_no_transaction_error(e):
                raise

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Context manager: BEGIN on enter, COMMIT on clean exit,
        ROLLBACK on exception.

        Stray commit()/rollback() inside the body, nesting, and a closed
        connection all raise InterfaceError. Emits the statements
        through a cursor rather than the client's task-scoped async
        ctxmgr (whose task-affinity guard would reject the sequential
        _run_sync calls).
        """
        del self.messages[:]
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._check_thread()
        token = threading.get_ident()
        # getattr/fresh-Lock fallback for __new__-built fixtures.
        _state_lock = getattr(self, "_state_lock", None) or threading.Lock()
        cursor = self.cursor()
        # Owner slot set INSIDE the try so a KI at the assignment is
        # caught by the outer finally (pinned by
        # test_sync_transaction_owner_assignment_inside_try_frame).
        try:
            # Atomic read-check-RESERVE under _state_lock so two threads
            # under check_same_thread=False can't both BEGIN. Lock
            # released before the wire round-trip; the outer finally
            # clears the slot on failure (token-guarded).
            with _state_lock:
                if self._transaction_owner is not None:
                    raise InterfaceError(
                        f"Nested conn.transaction() not supported (id={id(self)}); "
                        f"exit the outer block before opening a new one. "
                        f"(owner thread id={self._transaction_owner})"
                    )
                self._transaction_owner = token
            cursor.execute("BEGIN")
            try:
                yield
            except BaseException:
                # Best-effort ROLLBACK with the owner slot parked at a
                # non-None sentinel (keeps the nested-tx reject firing
                # for siblings during the RTT). Suppress a ROLLBACK
                # failure so the caller's exception propagates; on
                # failure force-close so the slot doesn't return to the
                # SA pool with an open server-side transaction.
                with _state_lock:
                    self._transaction_owner = _OWNER_INTERNAL_BUSY  # type: ignore[assignment]
                try:
                    try:
                        cursor.execute("ROLLBACK")
                    except Exception:
                        with contextlib.suppress(Exception):
                            self.force_close_transport()
                finally:
                    with _state_lock:
                        self._transaction_owner = token
                raise
            else:
                # Park the owner slot at the non-None sentinel before
                # COMMIT so a sibling can't reserve in the RTT window
                # (the sentinel reads as not-owner so COMMIT passes).
                with _state_lock:
                    self._transaction_owner = _OWNER_INTERNAL_BUSY  # type: ignore[assignment]
                try:
                    cursor.execute("COMMIT")
                finally:
                    with _state_lock:
                        self._transaction_owner = token
        finally:
            # Clear only if we still own the slot (== because the token
            # is a thread-id int, not an interned-guaranteed object).
            if self._transaction_owner == token:
                self._transaction_owner = None
            with contextlib.suppress(Exception):
                cursor.close()

    def cursor(self, **unknown_kwargs: object) -> Cursor:
        """Return a new Cursor. Unknown kwargs (e.g. stdlib's factory=)
        raise NotSupportedError, not a bare TypeError, so cross-driver
        ``except dbapi.Error:`` catches them."""
        del self.messages[:]
        # Precedence: closed -> thread -> kwarg-shape, so a foreign-thread
        # factory= caller sees the thread diagnostic, not two errors.
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._check_thread()
        if unknown_kwargs:
            raise NotSupportedError(
                f"dqlitedbapi cursor() rejects stdlib sqlite3 kwargs not "
                f"supported by this driver: {sorted(unknown_kwargs)}. "
                f"(stdlib's factory= is not honoured here — Cursor "
                f"subclassing is not supported.)"
            )
        cur = Cursor(self)
        # WeakSet.add isn't documented thread-safe; lock briefly.
        _state_lock = getattr(self, "_state_lock", None) or threading.Lock()
        with _state_lock:
            self._cursors.add(cur)
        # Re-check _closed after add: a foreign-thread force_close can
        # cascade between the prelude check and the add, leaving this
        # fresh cursor unscrubbed. Apply the cascade scrub + discard so
        # _cursors matches the post-close postcondition.
        if self._closed:
            cur._closed = True
            cur._rows = []
            cur._description = None
            cur._rowcount = -1
            cur._lastrowid = None
            cur._row_index = 0
            del cur.messages[:]
            # Keep cur._connection as a strong ref (not a proxy): the
            # cursor is already scrubbed, so the ref is metadata-only,
            # and a strong ref preserves identity + hashability.
            with _state_lock:
                self._cursors.discard(cur)
        return cur

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
        /,
    ) -> Cursor:
        """Stdlib convenience extension (not PEP 249): open a cursor,
        execute, return it. Without it SA's connect-listener idiom hits
        AttributeError outside the dbapi.Error tree. Closes the cursor
        on synchronous failure before re-raising.
        """
        del self.messages[:]
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
        /,
    ) -> Cursor:
        """Stdlib convenience extension (not PEP 249): open a cursor,
        executemany, return it. Closes the cursor on synchronous
        failure before re-raising.
        """
        del self.messages[:]
        # Via cursor() so a closed connection raises InterfaceError
        # before the shape check (cross-driver hooks expect that class).
        cur = self.cursor()
        # Reject shapes that iterate keys/chars or in nondeterministic
        # order; shared helper keeps one contract with Cursor.executemany.
        try:
            _validate_executemany_seq_shape(seq_of_parameters)
        except ProgrammingError:
            with contextlib.suppress(Exception):
                cur.close()
            raise
        try:
            cur.executemany(operation, seq_of_parameters)
        except BaseException:
            with contextlib.suppress(Exception):
                cur.close()
            raise
        return cur

    @property
    def address(self) -> str:
        """Node address this connection was opened against (read-only)."""
        return self._address

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` ran OR the inner connection was
        invalidated (cancel-mid-execute, leader-flip, transport reset).
        psycopg/asyncpg parity; use :attr:`invalidated` to distinguish
        the two states."""
        return self._closed or self.invalidated

    @property
    def invalidated(self) -> bool:
        """``True`` if the inner connection was invalidated (cancel /
        leader-flip / reset) but :meth:`close` hasn't been called.
        False if explicitly closed, never-connected, or healthy."""
        if self._closed:
            return False
        inner = self._async_conn
        if inner is None:
            return False
        return getattr(inner, "_protocol", "_sentinel") is None

    @property
    def row_factory(self) -> RowFactory | None:
        """stdlib row_factory parity: a ``factory(cursor, row)`` callable
        wrapping each fetched tuple; ``None`` returns plain tuples. New
        cursors inherit it. ``sqlite3.Row`` does NOT work (its ctor
        type-checks for a real sqlite3.Cursor) — use plain callables."""
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        # State-mutating setter -> closed-then-thread checks (the getter
        # bypasses them). suppress(AttributeError) for fixtures.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        with contextlib.suppress(AttributeError):
            if self._closed:
                raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._check_thread()
        if value is not None and not callable(value):
            raise ProgrammingError(
                f"row_factory must be callable or None, got {type(value).__name__}"
            )
        self._row_factory = value

    @property
    def text_factory(self) -> type[str]:
        """stdlib text_factory stub: TEXT is always returned as str
        (UTF-8 at the wire); the setter rejects non-str with
        NotSupportedError."""
        return str

    @text_factory.setter
    def text_factory(self, value: object) -> None:
        # suppress(AttributeError) for __new__-built fixtures.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        with contextlib.suppress(AttributeError):
            if self._closed:
                raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._check_thread()
        if value is str:
            return
        raise NotSupportedError(
            "dqlitedbapi does not support text_factory; TEXT cells are "
            "always returned as str (UTF-8 decoded at the wire layer)"
        )

    # TPC + stdlib-sqlite3 parity stubs: dqlite-server implements none,
    # so they raise NotSupportedError (inside dbapi.Error) rather than
    # leaking AttributeError. CAVEAT: hasattr() returns True here (the
    # stub exists), unlike stdlib — feature-detect via try/except
    # NotSupportedError, not hasattr.

    # *args/**kwargs so any caller signature reaches _stub_unsupported
    # (a tight signature would leak bare TypeError outside dbapi.Error).
    def tpc_begin(self, *args: object, **kwargs: object) -> NoReturn:
        """PEP 249 two-phase-commit stub. Always raises ``NotSupportedError`` —
        dqlite does not support two-phase commit (single-leader Raft)."""
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_prepare(self, *args: object, **kwargs: object) -> NoReturn:
        """PEP 249 two-phase-commit stub. Always raises ``NotSupportedError`` —
        dqlite does not support two-phase commit (single-leader Raft)."""
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_commit(self, *args: object, **kwargs: object) -> NoReturn:
        """PEP 249 two-phase-commit stub. Always raises ``NotSupportedError`` —
        dqlite does not support two-phase commit (single-leader Raft)."""
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_rollback(self, *args: object, **kwargs: object) -> NoReturn:
        """PEP 249 two-phase-commit stub. Always raises ``NotSupportedError`` —
        dqlite does not support two-phase commit (single-leader Raft)."""
        self._stub_unsupported("dqlite does not support two-phase commit")

    def tpc_recover(self, *args: object, **kwargs: object) -> NoReturn:
        """PEP 249 two-phase-commit stub. Always raises ``NotSupportedError`` —
        dqlite does not support two-phase commit (single-leader Raft)."""
        self._stub_unsupported("dqlite does not support two-phase commit")

    def xid(self, *args: object, **kwargs: object) -> NoReturn:
        """PEP 249 two-phase-commit stub. Always raises ``NotSupportedError`` —
        dqlite does not support two-phase commit (single-leader Raft)."""
        self._stub_unsupported("dqlite does not support two-phase commit")

    def _stub_unsupported(self, msg: str) -> NoReturn:
        """Shared NotSupportedError stub: clear messages, then check
        fork (InterfaceError, BEFORE the closed-check so a forked child
        gets the retry-classifiable diagnostic) and closed-state, then
        raise. No _check_thread (these are unsupported in every state).
        suppress(AttributeError) for __new__-built fixtures."""
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
        requires a separate Prepare → Exec / Query round-trip), so
        this raises ``NotSupportedError`` rather than escaping
        ``dbapi.Error`` as ``AttributeError``. Callers should split
        the script and ``execute`` each statement individually."""
        self._stub_unsupported(
            "dqlite does not support stdlib sqlite3 executescript; "
            "split the script and execute each statement individually"
        )

    def interrupt(self) -> NoReturn:
        """stdlib ``sqlite3``-parity stub. dqlite's wire-level
        interrupt primitive is not surfaced at the dbapi layer in
        this driver. Callers needing cross-thread cancellation
        should wrap calls in ``asyncio.timeout`` (async surface)
        or rely on the configured per-RPC timeout."""
        self._stub_unsupported(
            "dqlite does not surface interrupt() at the dbapi layer; "
            "use asyncio.timeout(...) on the async surface or rely "
            "on the per-RPC timeout"
        )

    # More stdlib parity stubs (VDBE-callback / db-status / db-config /
    # serialize / blob-open): none wire-feasible, all raise via
    # _stub_unsupported.

    def set_authorizer(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.set_authorizer`` parity stub. Always
        raises ``NotSupportedError`` — dqlite-server does not expose a
        per-prepare authorization callback."""
        self._stub_unsupported("dqlite-server does not expose a per-prepare authorization callback")

    def set_progress_handler(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.set_progress_handler`` parity stub.
        Always raises ``NotSupportedError`` — dqlite-server does not expose
        a VDBE progress callback."""
        self._stub_unsupported("dqlite-server does not expose a VDBE progress callback")

    def set_trace_callback(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.set_trace_callback`` parity stub. Always
        raises ``NotSupportedError`` — dqlite-server does not expose a
        per-statement trace callback."""
        self._stub_unsupported("dqlite-server does not expose a per-statement trace callback")

    def total_changes(self, *args: object, **kwargs: object) -> NoReturn:
        """No total_changes counter on the wire. Exposed as a callable
        stub (not a property) so hasattr() stays safe — a property
        getter would propagate NotSupportedError. Pinned by
        test_total_changes_hasattr_safe / test_pep249_stub_hasattr_divergence."""
        self._stub_unsupported("dqlite-server does not surface a total_changes counter on the wire")

    def getlimit(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.getlimit`` parity stub. Always raises
        ``NotSupportedError`` — dqlite-server does not expose
        ``sqlite3_db_status`` getlimit/setlimit on the wire."""
        self._stub_unsupported(
            "dqlite-server does not expose sqlite3_db_status getlimit/setlimit on the wire"
        )

    def setlimit(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.setlimit`` parity stub. Always raises
        ``NotSupportedError`` — dqlite-server does not expose
        ``sqlite3_db_status`` getlimit/setlimit on the wire."""
        self._stub_unsupported(
            "dqlite-server does not expose sqlite3_db_status getlimit/setlimit on the wire"
        )

    def getconfig(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.getconfig`` parity stub. Always raises
        ``NotSupportedError`` — dqlite-server does not expose
        ``sqlite3_db_config`` getconfig/setconfig on the wire."""
        self._stub_unsupported(
            "dqlite-server does not expose sqlite3_db_config getconfig/setconfig on the wire"
        )

    def setconfig(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.setconfig`` parity stub. Always raises
        ``NotSupportedError`` — dqlite-server does not expose
        ``sqlite3_db_config`` getconfig/setconfig on the wire."""
        self._stub_unsupported(
            "dqlite-server does not expose sqlite3_db_config getconfig/setconfig on the wire"
        )

    def serialize(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.serialize`` parity stub. Always raises
        ``NotSupportedError`` — dqlite does not support
        ``sqlite3_serialize`` (conflicts with the distributed Raft model)."""
        self._stub_unsupported(
            "dqlite does not support sqlite3_serialize; conflicts with the distributed Raft model"
        )

    def deserialize(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.deserialize`` parity stub. Always raises
        ``NotSupportedError`` — dqlite does not support
        ``sqlite3_deserialize`` (conflicts with the distributed Raft
        model)."""
        self._stub_unsupported(
            "dqlite does not support sqlite3_deserialize; conflicts with the distributed Raft model"
        )

    def blobopen(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.blobopen`` parity stub. Always raises
        ``NotSupportedError`` — dqlite does not expose ``sqlite3_blob_open``
        on the wire."""
        self._stub_unsupported("dqlite does not expose sqlite3_blob_open on the wire")

    def enable_load_extension(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.enable_load_extension`` parity stub.
        Always raises ``NotSupportedError`` — dqlite-server does not support
        runtime extension loading."""
        # *args/**kwargs so any signature reaches _stub_unsupported
        # (a tight signature would leak bare TypeError outside dbapi.Error).
        self._stub_unsupported("dqlite-server does not support runtime extension loading")

    def load_extension(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.load_extension`` parity stub. Always
        raises ``NotSupportedError`` — dqlite-server does not support
        runtime extension loading."""
        # See ``enable_load_extension`` rationale.
        self._stub_unsupported("dqlite-server does not support runtime extension loading")

    def backup(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.backup`` parity stub. Always raises
        ``NotSupportedError`` — dqlite does not support the stdlib sqlite3
        online backup API; use the dqlite-server dump/restore mechanism
        instead."""
        self._stub_unsupported(
            "dqlite does not support the stdlib sqlite3 online backup API; "
            "use the dqlite-server dump/restore mechanism instead"
        )

    def iterdump(self, *, filter: str | None = None, **kwargs: object) -> NoReturn:
        # Explicit ``filter=`` matches the stdlib-3.13 (*, filter=None)
        # shape for API-surface tooling; **kwargs absorbs future
        # additions; keyword-only mirrors stdlib's positional rejection.
        del filter  # accepted for signature parity; dqlite has no dump surface
        self._stub_unsupported(
            "dqlite does not support stdlib sqlite3 iterdump; "
            "use the dqlite-server dump/restore mechanism instead"
        )

    def create_function(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.create_function`` parity stub. Always
        raises ``NotSupportedError`` — dqlite-server does not support
        user-defined SQL functions."""
        self._stub_unsupported("dqlite-server does not support user-defined SQL functions")

    def create_aggregate(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.create_aggregate`` parity stub. Always
        raises ``NotSupportedError`` — dqlite-server does not support
        user-defined SQL aggregates."""
        self._stub_unsupported("dqlite-server does not support user-defined SQL aggregates")

    def create_collation(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.create_collation`` parity stub. Always
        raises ``NotSupportedError`` — dqlite-server does not support
        user-defined SQL collations."""
        self._stub_unsupported("dqlite-server does not support user-defined SQL collations")

    def create_window_function(self, *args: object, **kwargs: object) -> NoReturn:
        """Stdlib ``sqlite3.Connection.create_window_function`` parity stub.
        Always raises ``NotSupportedError`` — dqlite-server does not support
        user-defined SQL window functions."""
        self._stub_unsupported("dqlite-server does not support user-defined SQL window functions")

    def __repr__(self) -> str:
        state = "closed" if self._closed else ("connected" if self._async_conn else "unused")
        # Sanitise the address before !r so an attacker-influenced
        # address renders with the same ? substitution used elsewhere.
        safe_addr = sanitize_for_log(str(self._address))
        return f"<Connection address={safe_addr!r} database={self._database!r} {state}>"

    def __reduce__(self) -> NoReturn:
        # A live socket / loop thread / finalizer cycle can't be pickled;
        # raise an explicit driver TypeError (stdlib parity) instead of
        # the confusing default "cannot pickle '_thread.lock'".
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — driver "
            "connections own a live socket and an event-loop thread; "
            "use a connection pool or recreate the connection in the "
            "consumer process instead"
        )

    def __enter__(self) -> Self:
        """Eager-connect and return self. __exit__ commits/rolls back but does
        NOT close (like stdlib sqlite3) — the socket + loop thread stay alive for
        reuse; close() or a pool owns teardown. Because the driver is
        autocommit-by-default, ``with conn:`` does NOT implicitly begin a
        transaction (statements autocommit individually); use
        ``conn.transaction()`` or an explicit BEGIN for atomic grouping."""
        # Eager connect so ``with`` fails at the line, not in the body.
        try:
            self.connect()
        except BaseException:
            # __exit__ isn't called when __enter__ raises; clean up here
            # (close() is idempotent and tolerates never-connected).
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
        """Commit on clean exit, rollback on exception, do NOT close
        (stdlib parity — the connection stays reusable). KI/SystemExit
        mid-COMMIT/ROLLBACK is partial-ambiguous but still propagates
        (with a DEBUG breadcrumb).
        """
        # Nothing to finish if never used; also short-circuit if a
        # foreign-thread force_close already closed it (else commit()'s
        # closed-state guard would supplant the body exception).
        if self._closed or self._async_conn is None:
            return
        if exc_type is None:
            # Clean exit: commit; let exceptions propagate (silent data
            # loss is worse). DEBUG breadcrumb on signal interruption.
            try:
                self.commit()
            except (KeyboardInterrupt, SystemExit):
                logger.debug(
                    "Connection.__exit__ (address=%s, id=%s): "
                    "clean-exit commit interrupted by signal; "
                    "transaction state may be ambiguous (commit-or-not "
                    "on the leader)",
                    sanitize_for_log(str(self._address)),
                    id(self),
                    exc_info=True,
                )
                raise
        else:
            # Body raised; attempt rollback without masking it (narrow
            # except so bugs surface, DEBUG-log the rollback failure).
            try:
                self.rollback()
            except (KeyboardInterrupt, SystemExit):
                # Signal supersedes the body exception; breadcrumb + raise.
                logger.debug(
                    "Connection.__exit__ (address=%s, id=%s): "
                    "rollback interrupted by signal after body raised",
                    sanitize_for_log(str(self._address)),
                    id(self),
                    exc_info=True,
                )
                raise
            except Exception:
                logger.debug(
                    "Connection.__exit__ (address=%s, id=%s): "
                    "rollback failed; propagating original body exception",
                    sanitize_for_log(str(self._address)),
                    id(self),
                    exc_info=True,
                )
        # Deliberately no close (stdlib/psycopg2 parity) — SA-style usage
        # outlives a single ``with`` block.


# Expose Cursor for ``isinstance(cur, conn.Cursor)`` without importing
# dqlitedbapi.cursor. Assigned outside the class body to avoid shadowing
# the Cursor annotation name; not a cursor_factory hook.
Connection.Cursor = Cursor  # type: ignore[attr-defined]
