"""PEP 249 Connection implementation for dqlite."""

import asyncio
import concurrent.futures
import contextlib
import logging
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
    NO_TRANSACTION_MESSAGE_SUBSTRINGS,
    WIRE_DECODE_FAILED_PREFIX,
    primary_sqlite_code,
    sanitize_for_log,
)

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
_NO_TX_PRIMARY_CODES: Final[frozenset[int]] = frozenset({1})
# Type guard: all members must be primary SQLite codes (< 256)
# because the lookup site below masks the incoming code with
# ``primary_sqlite_code(...)`` before set membership. Adding an
# extended code (e.g. a hypothetical ``SQLITE_ERROR_RETRY = 513``)
# directly here would silently never match — ``513 & 0xFF == 1``,
# set holds ``513``, no match.
#
# Wrapped in ``if __debug__:`` so the strip-under-``-O`` posture is
# explicit to the reader (bare ``assert`` strips silently). The
# runtime enforcement is the ride-along test
# ``tests/test_no_tx_primary_codes_invariant.py`` which asserts the
# same invariant under any Python invocation; this guard is
# documentation for contributors editing the constant.
if __debug__:
    assert all(0 <= c < 256 for c in _NO_TX_PRIMARY_CODES), (
        "_NO_TX_PRIMARY_CODES must hold primary SQLite codes (< 256); "
        "use primary_sqlite_code(extended) at the lookup site instead."
    )
# Substrings that mark a benign "no transaction was active" reply.
# Imported from ``dqlitewire`` so the dbapi recogniser and the
# client-layer ``_is_no_tx_rollback_error`` share one source of
# truth — a wording drift in the server (or in the embedded SQLite
# version) that drops one of these clauses cannot produce silent
# layer divergence (client suppressing while the dbapi raises).
_NO_TX_SUBSTRINGS: Final[tuple[str, ...]] = NO_TRANSACTION_MESSAGE_SUBSTRINGS

# Minimum bound (in seconds) for joining the background event-loop
# thread on teardown. Both ``Connection.close()`` and
# ``force_close_transport()`` consult the operator's
# ``self._close_timeout`` for the join, but ``_close_timeout`` has a
# 0.01 s floor (``_CLOSE_TIMEOUT_FLOOR``) — too tight in practice for
# the queued ``loop.stop`` callback to land and the daemon thread to
# observe the stop and exit cleanly on a non-stuck loop. Floor the
# join at 0.1 s so the operator's tight close budgets do not race the
# scheduling latency of the stop dispatch itself; a stuck loop still
# bottoms out at this floor (matching the prior hard-coded behaviour
# under tight tuning) while WAN-tuned operators set
# ``close_timeout >> 0.1`` and get the full configured window.
_LOOP_THREAD_JOIN_MIN_SECONDS: Final[float] = 0.1

# Maximum number of per-RPC phases a single high-level sync call can
# stack end-to-end. ``self._timeout`` is documented as a PER-PHASE
# budget — the async surface honours this by wrapping each individual
# RPC in ``asyncio.timeout(self._timeout)``, so a single
# ``await execute(...)`` can legitimately take up to N × ``timeout``
# wall-clock without any phase exceeding its budget. The sync wrapper
# bridges the same async coroutine via ``Future.result(timeout=...)``
# from a calling thread, so its single timeout window must absorb all
# N phases — otherwise the sync surface fails on benign latency the
# async surface tolerates (silent sync/async contract drift; see the
# ``DqliteConnection.__init__`` and ``_operation_deadline`` docstrings
# for the client-layer source of the per-phase contract).
#
# N = 4 covers the worst-case first-call-after-connect:
#   1. handshake (Raft endpoint version negotiation),
#   2. open_database (database-id allocation),
#   3. query_sql send (request frame),
#   4. read+drain (response + any continuation frames).
# Steady-state calls (handshake + open already amortised) bottom out
# at N = 2 (send + read+drain), so the multiplier is conservative for
# the common case and authoritative for the worst case. Document each
# phase here so a future protocol change that adds a phase updates
# the multiplier; the per-phase budget itself stays the operator's
# ``timeout`` knob.
_SYNC_PHASES_MULTIPLIER: Final[int] = 4

# Fallback bound (in seconds) used by the ``weakref.finalize``-backed
# cleanup path (``_cleanup_loop_thread``) when invoked without an
# explicit close_timeout. The finalizer is captured at loop-creation
# time and ordinarily receives the operator's ``self._close_timeout``
# as a positional argument so its budget matches the graceful close
# path; this constant is the conservative cap retained for the rare
# case where the captured value is missing or invalid.
_LOOP_THREAD_JOIN_FALLBACK_SECONDS: Final[float] = 5.0


def _validate_timeout(timeout: float) -> None:
    """Raise ProgrammingError if ``timeout`` is not a positive finite number.

    Delegates to the client layer's public ``validate_timeout`` (the
    source of truth for the bool/finite/positive predicate) and
    translates its ``TypeError`` / ``ValueError`` to PEP 249
    ``ProgrammingError``. Sibling pattern to ``_wrap_positive_int``
    below — both wrap client-layer validators that deliberately use
    Python-convention exceptions for the client-only path.

    Previously this function re-implemented the predicate, which
    risked silent drift from the client layer (e.g. accepting
    ``Decimal`` in one but not the other). The shared validator
    keeps the contract single-source-of-truth.
    """
    from dqliteclient import validate_timeout as _client_validate_timeout

    try:
        _client_validate_timeout(timeout)
    except (TypeError, ValueError) as e:
        # Preserve the established error wording for tests that
        # match on ``"timeout must be a positive finite number"``.
        raise ProgrammingError(str(e)) from e


# Upper bound for ``max_continuation_frames`` enforced at the dbapi
# boundary. Mirror of the SA URL/connect_args cap (which uses the same
# 10x factor over the wire-default ``_DEFAULT_MAX_CONTINUATION_FRAMES``)
# so the same input that SA rejects above-cap is also rejected by
# direct dbapi callers — defence-in-depth against operator typos that
# would otherwise propagate to the wire layer and grant an attacker /
# misconfiguration ten-million-frame continuation budgets. Tightly
# paired with the SA-side ``_CONNECT_KWARG_ALLOWED`` cap that uses
# the same factor.
MAX_CONTINUATION_FRAMES_UPPER_BOUND: Final[int] = _DEFAULT_MAX_CONTINUATION_FRAMES * 10


# Stdlib ``sqlite3.Connection.isolation_level`` pre-3.12 accept-set
# (less ``None``). The empty string ``""`` is the stdlib DEFAULT value
# of the property; cross-driver round-trip code (``dst.isolation_level
# = src.isolation_level`` against a stdlib ``sqlite3.Connection``)
# silently broke on this driver because the setter only accepted
# ``None``. dqlite is fixed-mode autocommit at the wire layer; these
# five values collapse to the same behaviour, so accepting them as
# no-ops preserves cross-driver portability without changing wire
# behaviour.
_STDLIB_IMPLICIT_TX_VALUES: Final[frozenset[str]] = frozenset(
    {"", "DEFERRED", "IMMEDIATE", "EXCLUSIVE"}
)


def _wrap_positive_int(
    value: int | None,
    name: str,
    *,
    upper: int | None = None,
) -> int | None:
    """Wrap the client-layer ``validate_positive_int_or_none``'s
    ``TypeError`` / ``ValueError`` into PEP 249 ``ProgrammingError``.

    PEP 249 §7 requires every error originating from the driver to be
    a subclass of ``Error``. The client-layer validator deliberately
    raises Python-convention exceptions — correct for client-only
    consumers; the dbapi entry points are the boundary
    that translates to PEP 249 shapes. Sibling pattern to the
    ``_client_parse_address`` ``ValueError → InterfaceError`` wrap and
    to ``_validate_timeout``'s direct ``ProgrammingError``.

    ``upper`` (if given) caps the accepted value. Mirrors the SA URL/
    connect_args validator's upper bound so direct dbapi callers get
    the same defence-in-depth that SA already provides for the
    ``?max_continuation_frames=N`` URL form and the
    ``connect_args={"max_continuation_frames": N}`` form.
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


# Re-export the client-layer public constant under the established
# underscore-private name so existing references (the validator below,
# test fixtures, and the docstring at the top of this module) keep
# working without churn. The single-source-of-truth lives in
# ``dqliteclient.CLOSE_TIMEOUT_FLOOR``; a future tuning is a one-place
# change in the client layer.
_CLOSE_TIMEOUT_FLOOR: Final[float] = _client_close_timeout_floor


# Canonical prefix for the wrap-as-OperationalError diagnostic emitted
# by every connect-time arm in this module — eight raises across
# ``_build_and_connect``'s post-construct exception branches. The SA
# dialect's ``is_disconnect`` substring matcher reads the lowercase
# truncation of this prefix; the dbapi-side assertion tests read the
# full prefix verbatim. Single-source-of-truth so a future wording
# change is a one-place edit and the SA matcher updates in lockstep.
FAILED_TO_CONNECT_PREFIX: Final[str] = "Failed to connect: "


# Re-export the cluster-policy-rejection prefix + helper from the
# shared ``_constants`` module so the cursor-side rewrap site can
# share the SSOT without a circular import.
from dqlitedbapi._constants import (  # noqa: E402
    cluster_policy_rejection_message,
)


def _validate_close_timeout(close_timeout: float) -> None:
    """Raise ProgrammingError if ``close_timeout`` is not a positive finite number ≥ 0.01.

    Delegates to the client layer's public ``validate_timeout`` with
    ``min_value=_CLOSE_TIMEOUT_FLOOR`` so the floor is enforced
    uniformly across direct dqliteclient callers, the dbapi entry
    points, and the SA URL parser. Translates the client's
    ``TypeError`` / ``ValueError`` to PEP 249 ``ProgrammingError``.

    Forwards the close-timeout-specific FIN-flush rationale to the
    validator so dbapi-layer / SA-URL operators see the same
    operator-facing explanation as direct ``DqliteConnection`` /
    ``ConnectionPool`` callers when the floor trips.
    """
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


# Process-wide ``ClusterClient`` cache for the leader-discovery probe.
# Keyed by the full ``(address, governor)`` tuple so two configurations
# never share state. Without the cache, every dbapi ``connect()`` /
# every SA pool slot warm-up constructs a fresh ``ClusterClient`` —
# discarding both the single-flight ``_find_leader_tasks`` slot map
# AND the ``_last_known_leader`` fast-path cache. Under N concurrent
# SA pool checkouts after a leader flip, the cluster sees N
# independent leader-discovery sweeps where one would suffice.
#
# Fork-safety: the cache is wholesale-invalidated on fork via the same
# ``_current_pid`` token that ``DqliteConnection`` uses (see
# ``dqliteclient.connection`` lines 45-78). The first ``_resolve_leader``
# call in a child process observes the pid mismatch and clears the
# inherited cache; the parent's ``ClusterClient`` instances would
# otherwise carry parent-allocated ``asyncio.Lock`` / ``asyncio.Task``
# references that the child's event loop cannot make progress on.
#
# Strong-reference (``dict``, not ``WeakValueDictionary``): the
# ClusterClient must outlive a single ``find_leader`` call so the
# fast-path cache survives across calls; nothing else holds a
# reference. The cap (``_RESOLVE_LEADER_CACHE_MAX``) bounds the worst
# case to a single distinct configuration per dbapi ``connect()`` call;
# typical SA deployments use one config per Engine, so the cap is
# only reached by adversarial / highly-fragmented usage.
_RESOLVE_LEADER_CACHE: dict[tuple[object, ...], ClusterClient] = {}
_RESOLVE_LEADER_CACHE_PID: int = os.getpid()
_RESOLVE_LEADER_CACHE_MAX: Final[int] = 32
# Module-level lock serialising the read-check-construct-insert
# composite. Each individual dict op is GIL-atomic on CPython, but
# the composite is not — without serialisation, two threads that
# both observe ``cluster is None`` for the same key construct
# distinct ClusterClient instances and race on the dict insert,
# orphaning whichever loses (and defeating the single-flight
# collapse the cache is for). The lock is held across
# ``ClusterClient.__init__``; that constructor must NOT block on
# async I/O (it doesn't today — wire I/O happens lazily inside
# ``find_leader``). If a future change adds async work to the
# ``__init__`` path, the lock-while-awaiting becomes a deadlock
# risk and this gate must be reshaped (e.g. construct outside the
# lock, then check-and-insert under the lock).
#
# Not ``Final`` because the after-fork hook below replaces this with
# a fresh lock so a child that inherited the lock in a held state
# (e.g. parent forked while another thread held it) cannot deadlock.
# A multi-threaded parent forking is uncommon (and discouraged), but
# the dbapi sync layer DOES start a daemon ``_loop_thread`` per
# Connection — so any process that opens a sync connection then
# forks is multi-threaded by definition.
_RESOLVE_LEADER_CACHE_LOCK: threading.Lock = threading.Lock()


def _at_fork_replace_resolve_leader_cache_lock() -> None:
    """Replace the module-level cache lock with a fresh instance in
    the child process so a parent that forked while a thread held
    the lock does not leave the child with a permanently-held
    inherited lock (deadlock on first cache access).

    The cache itself is cleared inside the lock-protected composite
    on a pid mismatch (see ``_get_resolve_leader_cluster``); this
    callback complements that path by ensuring the lock is grabbable
    in the first place. Without this hook, a child that inherits a
    held lock would block forever on ``acquire`` and the pid
    mismatch path would never run.
    """
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
) -> ClusterClient:
    """Return a process-shared :class:`ClusterClient` for the
    leader-discovery probe, keyed by the (loop, address, governor)
    tuple.

    The single-flight collapse and ``_last_known_leader`` fast-path
    inside ``ClusterClient`` only amortise across callers of the
    *same* instance. Constructing a fresh client per ``connect()``
    defeats both. A process-wide cache restores the invariant.

    **Loop isolation**: keyed additionally by ``id(running_loop)``
    so a sync ``Connection`` running on a worker-thread loop does
    not reuse a ``ClusterClient`` whose ``_find_leader_tasks`` were
    created on a different loop. ``await asyncio.shield(<foreign-
    loop task>)`` raises ``RuntimeError`` which escapes the
    ``dbapi.Error`` hierarchy and SA's ``is_disconnect``
    classifier; without per-loop keying, multi-thread sync dbapi
    callers (Flask/Django handlers each opening their own
    Connection) would non-deterministically hit that path. The
    ``id(loop)`` key is paired with LRU eviction at cap so closed
    loops do not leak entries indefinitely; under churn, stale
    entries get evicted naturally. ``id`` recycling after loop GC
    is a residual risk bounded by the 32-slot cap — the recycled
    loop's predecessor's tasks would already be GC-eligible by
    that point.

    Cleared wholesale on fork: ``ClusterClient`` instances inherit
    parent ``asyncio.Lock`` / pending ``asyncio.Task`` references
    that are bound to the parent's event loop and cannot make
    progress in the child. The pid check is cheap (Python int
    equality) and runs only on the cache-lookup path.

    Async-only: must be called from inside a running event loop.
    The function raises ``InterfaceError`` if no running loop is
    found — fail loud rather than silently caching against a
    sentinel ``loop_id``. Today the only callers are
    ``_resolve_leader`` (async) and the cross-loop test fixtures.
    """
    global _RESOLVE_LEADER_CACHE_PID
    try:
        loop_id = id(asyncio.get_running_loop())
    except RuntimeError as e:
        raise InterfaceError(
            "_get_resolve_leader_cluster called outside a running event loop; "
            "the cache key requires a loop identity to keep ClusterClient "
            "tasks from leaking across loops."
        ) from e

    with _RESOLVE_LEADER_CACHE_LOCK:
        pid = get_current_pid()
        if pid != _RESOLVE_LEADER_CACHE_PID:
            _RESOLVE_LEADER_CACHE.clear()
            _RESOLVE_LEADER_CACHE_PID = pid

        # ``dial_func`` is keyed by callable identity — operators
        # constructing a single module-level dialer get a single cache
        # entry (the common case); operators constructing a fresh
        # lambda per request degrade to one cache entry per identity
        # (wasteful but correct, bounded by ``_RESOLVE_LEADER_CACHE_MAX``).
        # Distinct dialers MUST NOT share a ``ClusterClient`` because
        # they carry different transport contracts (TLS vs plaintext,
        # AF_UNIX vs TCP, custom KEEPALIVE, etc.) — sharing would let
        # the first dialer's connection serve a request that should
        # have used the second's.
        #
        # The callable itself goes in the key (NOT ``id(dial_func)``):
        # CPython's ``id()`` is the memory address of the object and is
        # recycled as soon as the object is GC'd. Function / lambda /
        # ``functools.partial`` / bound-method objects all hash by
        # identity and compare equal only to themselves, so using the
        # callable directly yields the same effective key while pinning
        # the dial_func for the lifetime of the cache entry — eliminating
        # the post-eviction id-recycle window where a freshly-allocated
        # lambda with a different transport contract could land at the
        # same memory address. Cost: the cache pins the dial_func until
        # the entry evicts (bounded by ``_RESOLVE_LEADER_CACHE_MAX=32``).
        # The ``id(loop)`` pairing above is the analogous bounded-
        # acceptable hazard documented at lines 398-403.
        key: tuple[object, ...] = (
            loop_id,
            address,
            timeout,
            max_total_rows,
            max_continuation_frames,
            trust_server_heartbeat,
            dial_func,
        )
        cluster = _RESOLVE_LEADER_CACHE.get(key)
        if cluster is None:
            if len(_RESOLVE_LEADER_CACHE) >= _RESOLVE_LEADER_CACHE_MAX:
                # FIFO eviction: drop the oldest entry. Two effects,
                # both acceptable for this cache's intended use:
                # 1. Fast-path lookup for that key is lost — the next
                #    ``find_leader`` against the evicted key
                #    rediscovers in one sweep.
                # 2. Single-flight collapse is temporarily violated:
                #    if a concurrent caller arrives on the evicted
                #    key while the prior awaiter still holds a
                #    reference to the in-flight task, the new caller
                #    constructs a brand-new ClusterClient (fresh
                #    ``_find_leader_tasks`` slot map) and runs ITS
                #    own parallel sweep against the same cluster.
                # Both effects self-heal — the cache backfills on the
                # next successful resolve, and the original in-flight
                # sweep completes independently. Cost: one wasted
                # sweep per evicted key with concurrent demand. The
                # cache size cap (_RESOLVE_LEADER_CACHE_MAX) bounds
                # the per-loop memory pressure.
                _RESOLVE_LEADER_CACHE.pop(next(iter(_RESOLVE_LEADER_CACHE)))
            cluster = ClusterClient(
                MemoryNodeStore([address]),
                timeout=timeout,
                max_total_rows=max_total_rows,
                max_continuation_frames=max_continuation_frames,
                trust_server_heartbeat=trust_server_heartbeat,
                dial_func=dial_func,
            )
            _RESOLVE_LEADER_CACHE[key] = cluster
        return cluster


async def _resolve_leader(
    address: str,
    *,
    timeout: float,
    max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
    max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
    trust_server_heartbeat: bool = False,
    dial_func: DialFunc | None = None,
) -> str:
    """Resolve the cluster's current leader address from a seed.

    Bootstraps from the user-supplied ``address`` (the URL host:port
    in the SA dialect's case) and uses :class:`ClusterClient` to
    follow the leader-redirect chain — same pattern go-dqlite's
    ``database/sql`` driver implements via
    ``client.NewLeaderConnector(store)``. Without this step,
    connecting to a demoted-leader address surfaces
    ``SQLITE_IOERR_NOT_LEADER`` from the server even though the
    cluster has a healthy leader at a different address; the SA
    pool's reconnect-after-pre-ping path cannot recover.

    Threads the governor set used by the subsequent
    :class:`DqliteConnection` so the leader-discovery probe runs
    with the same configuration as the eventual data session. Without
    forwarding, an operator who set ``trust_server_heartbeat=True``
    finds the *first* round-trip — leader discovery — running with
    the default opt-out, defeating the very setting they enabled.
    Likewise ``max_total_rows`` / ``max_continuation_frames`` matter
    for admin paths (``cluster_info`` / ``dump``) reachable through
    the resolved client.

    ``dial_func`` is threaded for the same reason: an operator
    requiring a TLS/AF_UNIX/custom-KEEPALIVE dialer must see it
    honoured on the leader-discovery probe (the FIRST round-trip),
    not just the post-resolve data session. Without forwarding, a
    TLS-required deployment opens a plaintext leader-probe socket
    against the seed — either failing the TLS-only listener with an
    unhelpful "connection reset" diagnostic or (worse) succeeding
    against a TLS-terminating proxy that tolerates plaintext, making
    the first round-trip silently unencrypted.

    Wraps the seed in a single-node :class:`MemoryNodeStore` and
    delegates to :meth:`ClusterClient.find_leader`. Returns the
    leader's address on success; raises the underlying
    ``ClusterError`` / ``ClusterPolicyError`` for the surrounding
    error-translation arms in :func:`_build_and_connect` to handle.
    """
    cluster = _get_resolve_leader_cluster(
        address=address,
        timeout=timeout,
        max_total_rows=max_total_rows,
        max_continuation_frames=max_continuation_frames,
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
) -> DqliteConnection:
    """Build a DqliteConnection with the given governors and connect it.

    Performs the dqlite production-grade connect sequence:

    1. Resolve the current leader via :func:`_resolve_leader` (one
       round-trip against the seed; if the seed is the leader, the
       leader-info reply is its own address).
    2. Construct + connect a :class:`DqliteConnection` against the
       leader address.

    Wraps the sequence that both the sync and async Connection
    flavours execute under their respective locks. The
    ``OperationalError`` message phrasing ("Failed to connect: ...")
    is intentionally verbatim so test assertions that match on the
    prefix continue to pass.

    Mirrors the canonical go-dqlite/driver layering — applications
    should not need to special-case leader-flips between
    connections; the dbapi handles the redirect transparently.
    """
    try:
        leader_address = await _resolve_leader(
            address,
            timeout=timeout,
            max_total_rows=max_total_rows,
            max_continuation_frames=max_continuation_frames,
            trust_server_heartbeat=trust_server_heartbeat,
            dial_func=dial_func,
        )
    except _client_exc.ClusterPolicyError as e:
        # Operator allowlist rejected a redirect target. Surface as
        # InterfaceError with the canonical prefix — symmetric with
        # the post-construct ClusterPolicyError arm below.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(
            cluster_policy_rejection_message("during leader discovery", str(e)),
            code=None,
            raw_message=raw_msg,
        ) from e
    except _client_exc.ClusterError as e:
        # All nodes in the seed's resolved store rejected the leader
        # query (no node is currently leader, all unreachable, etc.).
        # Surface as OperationalError so the SA pool's retry loop
        # classifies it correctly. Different from the post-construct
        # ClusterError arm only in the message prefix — operators
        # reading logs need to tell "couldn't find leader" from
        # "found leader but couldn't connect".
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(
            f"Failed to find leader from {address}: {e}",
            code=None,
            raw_message=raw_msg,
        ) from e
    except OSError as e:
        # Defence-in-depth, symmetric with the post-construct
        # ``conn.connect()`` block's ``OSError`` arm. The in-tree
        # ``find_leader`` wraps every per-probe ``OSError`` in
        # ``_ProbeMiss`` and aggregates them into ``ClusterError`` —
        # so on the happy path this arm is unreached. The arm exists
        # for: (a) custom ``NodeStore``s that raise ``OSError`` from
        # ``get_nodes()`` (e.g. a file-backed YAML store with a
        # missing file); (b) ``socket.gaierror`` from future DNS
        # paths; (c) ``TimeoutError`` (an ``OSError`` subclass since
        # Python 3.11) leaked from a misconfigured ``asyncio.wait_for``
        # inside a third-party ``cluster_factory``. PEP 249 §7
        # requires Error-class surface; ``OperationalError`` is the
        # right shape for transport-class faults.
        raise OperationalError(
            f"Failed to find leader from {address}: {e}",
            code=None,
            raw_message=str(e),
        ) from e
    except BaseExceptionGroup as eg:
        # PEP 249 §7 mandates Error-class surface. ``BaseExceptionGroup``
        # does not inherit from ``Exception`` so ``except Exception:``
        # blocks miss it; no client class matches it either. The
        # in-tree primary raise path is ``ConnectionPool.initialize``;
        # a future dbapi-side pool or third-party retry middleware
        # wrapping the connect coro could route a group here. Mirror
        # the ``_call_client`` discipline. See
        # ``cursor.py::_call_client`` for the full rationale —
        # including the PEP 654 cancel-class split that re-raises any
        # ``CancelledError`` / ``KeyboardInterrupt`` / ``SystemExit``
        # children rather than silently wrapping them as
        # ``OperationalError``.
        cancel_group, remainder = eg.split(
            lambda e: isinstance(e, (asyncio.CancelledError, KeyboardInterrupt, SystemExit))
        )
        if cancel_group is not None:
            raise cancel_group from None
        assert remainder is not None
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
        trust_server_heartbeat=trust_server_heartbeat,
        close_timeout=close_timeout,
        dial_timeout=dial_timeout,
        attempt_timeout=attempt_timeout,
        dial_func=dial_func,
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
            cluster_policy_rejection_message(None, str(e)),
            code=None,
            raw_message=raw_msg,
        ) from e
    except _client_exc.DqliteConnectionError as e:
        # Transport / handshake failure at the connect layer (TCP
        # refused, DNS failure, server-reset, leader-change rewrap).
        # The cursor-path classifier maps DqliteConnectionError to
        # OperationalError; mirror it on the connect path so SA's
        # pool retry loop sees the right shape and the substring
        # scan can classify it. Thread the optional ``code`` and
        # ``raw_message`` through so a leader-change rewrap (the
        # client's ``connect()`` LEADER_ERROR_CODES branch surfaces
        # ``DqliteConnectionError(..., code=10250, raw_message=...)``)
        # carries the wire-level signal that SA's is_disconnect's
        # code-based classifier expects — matching the query path.
        code = getattr(e, "code", None)
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}{e}", code=code, raw_message=raw_msg
        ) from e
    except _client_exc.ClusterError as e:
        # Non-policy ClusterError — transient at the cluster discovery
        # layer (no leader yet, all nodes unreachable). Surface as
        # OperationalError so the SA pool's retry loop classifies it
        # correctly, with raw_message preserved.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}{e}", code=None, raw_message=raw_msg
        ) from e
    except _client_exc.ProtocolError as e:
        # Wire-level desync during handshake (very rare). Match the
        # cursor-path classifier's wording so SA's substring scan sees
        # the canonical ``WIRE_DECODE_FAILED_PREFIX``.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(
            f"{WIRE_DECODE_FAILED_PREFIX}: {e}", code=None, raw_message=raw_msg
        ) from e
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
        # above. PEP 249 §7: errors that occur during the operation
        # of the database are wrapped in DatabaseError or its
        # subclasses; an InterfaceError wrap would mis-classify a
        # server-sourced error as a driver-misuse error. Use
        # DatabaseError as the conservative wrap class so cross-
        # driver code using ``except DatabaseError:`` catches future
        # error classes correctly. Mirrors the cursor-path classifier
        # catch-all at the end of ``_call_client``.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise DatabaseError(
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
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}{e}", code=None, raw_message=str(e)
        ) from e
    except BaseExceptionGroup as eg:
        # See the sibling arm above the ``DqliteConnection(...)``
        # construction for the rationale. ``BaseExceptionGroup``
        # bypasses every per-class arm; wrap as ``OperationalError``
        # (transport flavour, since this block surrounds the actual
        # connect) with the remainder on ``__cause__``. PEP 654
        # cancel-class split runs first so any
        # ``CancelledError`` / ``KeyboardInterrupt`` / ``SystemExit``
        # children are re-raised rather than silently wrapped.
        cancel_group, remainder = eg.split(
            lambda e: isinstance(e, (asyncio.CancelledError, KeyboardInterrupt, SystemExit))
        )
        if cancel_group is not None:
            raise cancel_group from None
        assert remainder is not None
        child_classes = {type(c).__name__ for c in remainder.exceptions}
        raise OperationalError(
            f"{FAILED_TO_CONNECT_PREFIX}aggregate {type(remainder).__name__} with "
            f"{len(remainder.exceptions)} child(ren) of class(es) {sorted(child_classes)}",
            code=None,
            raw_message=str(remainder),
        ) from remainder
    return conn


def _is_no_transaction_error(exc: Exception) -> bool:
    """True if ``exc`` is a genuine "no active transaction" server reply.

    Gates the silent swallow on the SQLite result code in addition to
    the English wording. A disk-full / constraint / IO error whose
    message happens to include the magic substring will not be
    swallowed.

    Substring fragility: the matched text is the SQLite engine's
    canonical wording (``"no transaction is active"``), pinned by
    ``NO_TRANSACTION_MESSAGE_SUBSTRINGS`` in
    ``dqlitewire.constants`` and exercised by the integration test
    ``tests/integration/test_no_transaction_error_wording.py`` against
    a live cluster. A future SQLite version that rephrases the message
    would surface this as a real ``OperationalError`` from
    ``commit()`` / ``rollback()`` rather than a silent swallow — the
    integration test would catch it before users see the regression.
    Server-side rephrasings are extremely rare (the canonical wording
    has been stable across many SQLite releases); the substring guard
    is the best available signal short of a stable extended-error-code
    pin, which dqlite does not currently emit for this case.

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
    if primary_sqlite_code(code) not in _NO_TX_PRIMARY_CODES:
        return False
    # Match against the un-truncated server text (raw_message) rather
    # than ``str(exc)`` (truncated). A long server message that has
    # the no-tx clause beyond the truncation cap would otherwise miss
    # the substring and surface the no-tx as a real error.
    raw = getattr(exc, "raw_message", None) or str(exc)
    lowered = raw.lower()
    return any(s in lowered for s in _NO_TX_SUBSTRINGS)


def _safe_writer_close(writer: asyncio.StreamWriter) -> None:
    """``StreamWriter.close()`` last-resort: callable scheduled on the
    owning loop via ``call_soon_threadsafe`` to drive FIN out of a
    transport without awaiting the protocol-level drain.

    Used by :meth:`Connection.force_close_transport` so terminate paths
    don't crash the loop with a stray exception (e.g. transport already
    closed by a connection_lost race).
    """
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
    # Bind PURE-MODULE globals (``warnings`` / ``logger`` /
    # ``contextlib``) as keyword-only default args so the
    # ``Py_FinalizeEx`` phase-3 module-globals-None-set teardown
    # (documented in ``Lib/weakref.py::_exitfunc`` /
    # ``Python/pylifecycle.c::Py_FinalizeEx``) cannot replace the
    # names this body dereferences with ``None`` between function
    # definition and finalizer invocation. Stdlib precedent:
    # ``Lib/tempfile.py::_TemporaryFileWrapper.close`` captures
    # ``closer`` the same way; ``multiprocessing.util.Finalize`` is
    # the same pattern. Names captured at definition time — if the
    # module re-binds any of them after definition (none do today;
    # not even test fixtures should), the captured value is stale.
    #
    # ``get_current_pid`` is INTENTIONALLY NOT captured: tests
    # (``test_cleanup_loop_thread_finalizer_fork_safe.py``) patch
    # the module-level name via ``unittest.mock.patch`` to simulate
    # a forked child, and a kwarg-default capture would freeze the
    # production value past the patch. The runtime dereference
    # below is wrapped in a ``try`` block that catches the
    # shutdown-time ``TypeError`` ('NoneType' is not callable) so
    # the shutdown-safety goal is still met for ``get_current_pid``.
    _warnings: Any = warnings,
    _logger: Any = logger,
    _contextlib: Any = contextlib,
) -> None:
    """Stop the background event loop and join its thread.

    Called from a ``weakref.finalize`` so it must not reference the
    ``Connection`` instance. ``closed_flag`` is a 1-element list that
    the Connection mutates when ``close()`` is called — we use that
    rather than a direct reference to self to decide whether to emit
    a ``ResourceWarning``.

    ``inner_handle`` is a 0-or-1-element list mutated in
    ``Connection._get_async_connection`` (inline ``[:]=``) once
    ``self._async_conn`` is built. When populated, the single element
    is a ``weakref.ref`` to the inner ``DqliteConnection``. The box
    indirection is the canonical idiom for late-publishing a value
    into a ``weakref.finalize``'s captured args (the finalize captures
    args by reference at registration time; mutating a captured list
    is observed at call time). A ``weakref.ref`` avoids strong-pinning
    the inner from the finalize's args (which would create a
    reference cycle: outer → ``_async_conn`` → inner; finalize args →
    inner directly; cycle through the outer's ``__dict__``).

    The box is NOT cleared on explicit close paths: explicit close
    detaches the finalizer (``self._finalizer.detach()``) before
    nulling ``_async_conn``, so the cleanup callback never observes
    a populated box on that arm. The only path that reaches this
    callback with the box populated is the leaked-outer-GC path —
    where the inner's ``weakref.ref`` may resolve to ``None`` if the
    inner was reclaimed in the same GC pass (handled by the
    ``if inner is not None`` guard below).

    ``close_timeout`` mirrors the operator's ``Connection._close_timeout``
    so the finalizer's join budget matches the graceful ``close()`` and
    ``force_close_transport()`` paths. Captured positionally at finalize
    registration so the finalizer does not retain a reference to the
    ``Connection`` instance. Floored at
    ``_LOOP_THREAD_JOIN_MIN_SECONDS`` so a tight ``close_timeout``
    (down to the ``_CLOSE_TIMEOUT_FLOOR=0.01`` minimum) still leaves
    enough slack for the queued ``loop.stop`` callback to land and the
    daemon thread to exit on a non-stuck loop.

    Fork-safety: ``creator_pid`` is the pid of the process that
    constructed the Connection; the finalizer fires in BOTH parent
    and child after ``os.fork`` (each frees the inherited
    Connection independently). In the child the captured ``loop`` /
    ``thread`` are parent-owned — calling ``loop.close()`` would
    close inherited selector FDs the parent still uses;
    ``thread.join`` blocks for up to the configured budget on a
    non-existent OS thread (only the calling thread crosses
    ``fork``); ``ResourceWarning`` based on the parent's frozen
    ``closed_flag`` is a false positive (the parent may close after
    fork). Mirror the discipline of ``Connection._check_thread`` /
    ``DqliteConnection.close`` / ``Pool.close``: pid-mismatch →
    no-op.

    Shutdown-safety: when CPython's ``Py_FinalizeEx`` reaches phase 3
    (cycle-collect after ``atexit``), ``PyImport_Cleanup`` walks
    ``sys.modules`` and sets every module's globals to ``None``. A
    finalize that dereferences imported names by NAME would then see
    ``None`` for ``get_current_pid`` / ``warnings`` / ``logger`` /
    ``contextlib`` and raise ``TypeError`` / ``AttributeError`` —
    emitting an unraisable-hook traceback that buries whatever
    actually caused the shutdown. Names are captured as kwarg
    defaults at definition time to dodge this teardown phase. If
    any of the captured names ends up ``None`` at call time anyway
    (exotic reload paths), the body short-circuits silently.
    """
    # Read ``get_current_pid`` from module globals at call time so
    # the test fixture's ``patch("dqlitedbapi.connection."
    # "get_current_pid", ...)`` is observed. Wrap in a broad except
    # so the ``Py_FinalizeEx`` phase-3 ``get_current_pid = None``
    # teardown surfaces as a silent no-op (not an unraisable-hook
    # ``TypeError: 'NoneType' object is not callable`` traceback).
    try:
        current_pid = get_current_pid()
    except Exception:
        # Module global ``get_current_pid`` may be ``None`` under
        # interpreter shutdown; return silently rather than emit an
        # unraisable-hook traceback that buries whatever caused the
        # shutdown.
        return
    if current_pid != creator_pid:
        # Forked child. The captured loop/thread/closed_flag belong
        # to the parent process. Skip cleanup entirely — both the
        # warning emission and the loop/thread teardown.
        return
    # Resolve the inner ``DqliteConnection`` if the late-publish box
    # has been populated. Use a weakref to avoid strong-pinning. If
    # the inner has already been GC'd (the leaked-outer path: the
    # outer's ``__dict__["_async_conn"]`` held the only strong ref
    # to the inner, so the inner is reclaimed in the same collection
    # pass that triggers this finalizer), ``inner_ref()`` returns
    # ``None`` and we skip the disarm / drain reap entirely — the
    # inner's own ``weakref.finalize`` will have fired in the same
    # pass and emitted its own diagnostic if applicable.
    inner: Any = None
    if inner_handle:
        inner_ref = inner_handle[0]
        if inner_ref is not None:
            inner_obj = inner_ref() if callable(inner_ref) else None
            # Skip the inner-targeted disarm if the inner is already
            # closed (``_closed_flag[0] is True``): no false-positive
            # warning to suppress and no pending drain to reap.
            if inner_obj is not None:
                inner = inner_obj
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
        # User never called close() → leak warning (matches stdlib
        # sqlite3). The narrow ``RuntimeError`` suppression here is
        # for the specific interpreter-shutdown race where the
        # warnings module's own finalization is mid-teardown; any
        # other exception (including ResourceWarning being
        # converted to a raise under -W error) is allowed to
        # propagate through the surrounding finally so the
        # finalizer's reporter (sys.unraisablehook) still surfaces
        # it while the cleanup completes. The
        # ``_warnings is not None and _contextlib is not None``
        # guard handles the rare interpreter-reload path where the
        # kwarg-default capture itself sees ``None`` mid-shutdown.
        if closed_flag[0] is False and _warnings is not None and _contextlib is not None:
            with _contextlib.suppress(RuntimeError):
                _warnings.warn(
                    f"Connection(address={address!r}) was garbage-collected "
                    f"without close(); cleaning up event-loop thread. Call "
                    f"Connection.close() explicitly to avoid this warning.",
                    ResourceWarning,
                    stacklevel=2,
                )
    finally:
        # Disarm the inner client's ``_connection_unclosed_warning``
        # finalizer BEFORE the loop teardown, mirroring the discipline
        # at ``force_close_transport`` lines 2148-2155. Without this,
        # the same GC sweep that fired this finalize would also
        # eventually fire the inner's finalizer, emitting a misleading
        # second ResourceWarning ("DqliteConnection ... was garbage-
        # collected without await close()") for the SAME socket — one
        # leak surfacing as two stderr lines. Mirrors what the explicit
        # close paths already do at close.py / force_close_transport.
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
            # Reap any pending invalidation-drain task on the inner
            # BEFORE ``loop.stop`` lands, mirroring the bounded-
            # resnapshot block in ``force_close_transport`` at
            # ``connection.py:2156-2219``. Without this reap, the
            # task survives ``loop.close()`` (CPython
            # ``BaseEventLoop.close`` does NOT cancel pending tasks),
            # ``Task.__del__`` fires with state PENDING, and asyncio
            # writes "Task was destroyed but it is pending" to stderr
            # via its default exception handler — bypassing
            # ``warnings.catch_warnings`` and surfacing as a third
            # stderr line per GC-leaked sync ``Connection``. FIFO of
            # the ``call_soon_threadsafe`` ready queue ensures the
            # cancel callbacks run before the queued ``loop.stop``.
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
                    # Cap exhausted: final defensive null-out. Mirrors
                    # the ``force_close_transport`` cap-exhausted
                    # branch. Operator-visible warning only on the
                    # pathological feedback-loop case.
                    with _contextlib.suppress(Exception):
                        inner._pending_drain = None
                    if _logger is not None:
                        _logger.warning(
                            "Connection._cleanup_loop_thread: inner._pending_drain still "
                            "set after %d re-snapshot iterations; cancelling residual task "
                            "to avoid 'Task was destroyed but it is pending' at GC.",
                            resnapshot_cap,
                        )
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
            if _logger is not None:
                _logger.debug(
                    "Connection._cleanup_loop_thread: loop.call_soon_threadsafe "
                    "raised RuntimeError (loop likely closed mid-call)",
                    exc_info=True,
                )
        if _contextlib is not None:
            with _contextlib.suppress(RuntimeError):
                thread.join(timeout=max(close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS))
        try:
            if not loop.is_closed():
                loop.close()
        except RuntimeError:  # pragma: no cover - race: loop restarted mid-finalize
            # Raised if the loop was somehow restarted mid-finalization.
            # Same operator-visibility rationale as above.
            if _logger is not None:
                _logger.debug(
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

    Thread-affinity: every public method enforces the
    ``threadsafety=1`` contract — sync side via ``_check_thread()``,
    async side via ``_check_loop_binding()`` / ``_check_loop_only()``
    (the asymmetry exists because the sync class is thread-bound and
    the async class is loop-bound). Calls from a foreign OS thread
    (sync) or foreign event loop (async) raise ``ProgrammingError``.
    Read-only property reads (``closed``, ``address``, ``autocommit``,
    ``isolation_level``, ``row_factory``) bypass the affinity check
    and are GIL-atomic at the CPython level — safe to read from any
    thread / loop, but may still raise ``InterfaceError`` on a closed
    connection (``autocommit`` / ``isolation_level`` also raise
    ``InterfaceError`` if read from a forked child). The
    ``in_transaction`` property is the exception: it retains the
    affinity check (``_check_thread()`` on the sync class,
    ``_check_loop_only()`` on the async class) for shipped-API
    compatibility (callers depend on the cross-thread / cross-loop
    raise; removing it would be a behavioural change).
    """

    # PEP 249 optional extension ("Attributes from Module Exceptions"):
    # expose the module-level exception classes as class attributes so
    # cross-driver generic code can write ``except conn.Error:`` without
    # importing the driver module. Stdlib ``sqlite3.Connection`` and
    # every mainstream driver (psycopg2, asyncpg, aiosqlite) do the same.
    # Class attrs (not instance attrs) to keep ``type(conn).Error``
    # identity.
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

    def __init__(
        self,
        address: str,
        *,
        database: str = "default",
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        max_total_rows: int | None = _DEFAULT_MAX_TOTAL_ROWS,
        max_continuation_frames: int | None = _DEFAULT_MAX_CONTINUATION_FRAMES,
        trust_server_heartbeat: bool = False,
        close_timeout: float = DEFAULT_CLOSE_TIMEOUT_SECONDS,
        dial_timeout: float | None = None,
        attempt_timeout: float | None = None,
        dial_func: DialFunc | None = None,
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
            dial_timeout: Per-TCP-connect budget (seconds) — mirrors
                go-dqlite's ``Config.DialTimeout``. ``None`` (default)
                collapses onto ``timeout``. Set a smaller value than
                ``timeout`` to fast-fail on a DNS-typo / firewalled
                peer rather than paying the full per-RPC budget at the
                dial stage. Forwarded to the underlying
                :class:`DqliteConnection`.
            attempt_timeout: Per-attempt envelope (seconds) covering
                dial + handshake + first RPC — mirrors go-dqlite's
                ``Config.AttemptTimeout``. ``None`` (default) collapses
                onto ``timeout``. Smaller-than-``timeout`` values
                bound the parallel leader-sweep against slow-
                handshaking peers (TLS-terminating proxies with stuck
                welcomes, partial-restart nodes). Forwarded to the
                underlying :class:`DqliteConnection`.
            dial_func: Caller-supplied async dialer replacing the
                default TCP path — mirrors go-dqlite's
                ``WithDialFunc``. Use cases: TLS, unix-socket
                transport, custom SO_KEEPALIVE policy, out-of-band
                health probes. ``None`` (default) uses the standard
                ``asyncio.open_connection`` path. Forwarded to the
                underlying :class:`DqliteConnection`. See
                :data:`dqliteclient.DialFunc` for the protocol.
        """
        _validate_timeout(timeout)
        _validate_close_timeout(close_timeout)
        if dial_timeout is not None:
            _validate_timeout(dial_timeout)
        if attempt_timeout is not None:
            _validate_timeout(attempt_timeout)
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
        if not isinstance(database, str):
            raise InterfaceError(f"database must be a str, got {type(database).__name__}")
        if not database:
            raise InterfaceError("database must be a non-empty string")
        if database != database.strip():
            # Reject any leading/trailing whitespace. dqlite-server's
            # ``OPEN(name=whitespace)`` has implementation-defined
            # semantics: it may create a database literally named
            # ``" "`` / ``" default"``, fail with a SQL-level filename
            # error, or silently mismatch a future open of the same
            # logical name written without surrounding whitespace.
            # The dbapi layer is the right place to canonicalise —
            # same discipline as ``_client_parse_address``.
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
        self._trust_server_heartbeat = trust_server_heartbeat
        self._close_timeout = close_timeout
        self._dial_timeout = dial_timeout
        self._attempt_timeout = attempt_timeout
        self._dial_func = dial_func
        self._async_conn: DqliteConnection | None = None
        self._closed = False
        # stdlib ``sqlite3.Connection.row_factory`` parity. None means
        # "return plain tuples". New cursors inherit this default.
        self._row_factory: RowFactory | None = None
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
        # ``transaction()`` context-manager owner sentinel. Stores the
        # OS thread id of the body owner while a ``with conn.transaction()``
        # block is active; ``commit`` / ``rollback`` reject from inside
        # the body so the ctxmgr keeps boundary control. Mirrors the
        # async sibling's ``_transaction_owner`` (which stores
        # ``asyncio.Task``); thread id is the sync equivalent identity.
        self._transaction_owner: int | None = None
        # 1-element list (mutable, captured by the finalizer) that
        # close() flips to True. Using a list avoids the finalizer
        # closing over ``self`` and preventing GC.
        self._closed_flag: list[bool] = [False]
        # Box for late-publishing the inner ``DqliteConnection``
        # handle into ``_cleanup_loop_thread``'s captured args. The
        # finalize captures THIS list by reference at registration
        # time (inside ``_ensure_loop``, before the inner is built);
        # ``_get_async_connection`` mutates the slot to
        # ``weakref.ref(inner)`` once the inner is built so the
        # finalize body can reach it without strong-pinning.
        #
        # The box is intentionally NOT cleared on explicit close
        # paths: those detach the finalizer before nulling
        # ``_async_conn``, so the cleanup callback never observes a
        # populated box on the explicit-close arm. The dead
        # ``weakref.ref`` may survive until outer reclamation; the
        # cleanup callback's ``if inner is not None`` guard handles
        # the resolved-to-None case correctly.
        self._inner_finalize_handle: list[Any] = []
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
        if get_current_pid() != self._creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {self._creator_pid}, "
                f"current pid {get_current_pid()})"
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

        Defence-in-depth pid guard at the lowest sensible point in the
        sync stack: every PUBLIC caller already routes through
        ``_check_thread`` before reaching ``_run_sync`` / ``_ensure_loop``,
        but a subclass / refactor / new caller that forgets that
        perimeter check would otherwise return the parent's loop object
        in a forked child (``loop.is_closed()`` is a Python attribute
        inherited as False; ``self._thread`` survives only in the
        thread that called ``fork()``). The result is an
        ``asyncio.run_coroutine_threadsafe`` against a loop nobody
        drains — the caller hangs on ``Future.result(timeout=...)``
        for the configured per-RPC budget and then raises a generic
        ``TimeoutError`` / ``OperationalError`` instead of the
        canonical ``InterfaceError("Connection used after fork ...")``.
        Mirror the discipline of the at-fork resolve-leader-cache lock
        replacement at module top and the ``_check_thread`` guard on
        the public surface: keep the diagnostic shape uniform across
        every fork-violating entry point.
        """
        if get_current_pid() != self._creator_pid:
            raise InterfaceError(
                f"Connection used after fork; reconstruct from configuration "
                f"in the target process. (created in pid {self._creator_pid}, "
                f"current pid {get_current_pid()})"
            )
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
                # ``_inner_finalize_handle`` is a list captured by
                # reference; ``_get_async_connection`` populates it
                # inline with ``weakref.ref(inner)`` once
                # ``self._async_conn`` is built so the finalize can
                # disarm the inner's ResourceWarning finalizer and
                # reap any pending ``_invalidate`` drain task BEFORE
                # ``loop.stop`` lands. See ``_cleanup_loop_thread``'s
                # docstring for the boxed-handle rationale.
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
                # ``OperationalError`` (not ``InterfaceError``) for
                # parity with the async sibling at
                # ``aio/connection.py``: ``commit`` / ``rollback``
                # op_lock-acquire-timeout also raise ``OperationalError``.
                # SA's ``is_disconnect`` is gated on ``DatabaseError``
                # and recognises the ``OperationalError`` class — so a
                # contended slot is recycled by the pool rather than
                # surfaced as a programmer-bug class. The message
                # leads with the canonical ``"op_lock acquire timed
                # out"`` prefix so a sibling-thread/signal-handler
                # contention scenario is identifiable in logs.
                raise OperationalError(
                    f"op_lock acquire timed out after {self._timeout}s waiting "
                    "for another operation on this connection to release "
                    f"(id={id(self)}; may indicate re-entry from a signal handler "
                    "or concurrent use from another thread). Treat as a transient "
                    "condition and retry on a fresh connection.",
                    code=None,
                )
            # Defensive narrow wrap: if ``_ensure_loop()`` raises
            # before the coroutine is scheduled (rare paths: OS
            # thread-start failure, ``new_event_loop`` failing under
            # FD ulimit exhaustion), close ``coro`` so the
            # unscheduled coroutine doesn't emit
            # ``RuntimeWarning("coroutine was never awaited")`` at GC.
            # The sibling cleanup arms in the KI / SystemExit /
            # TimeoutError / CancelledError branches all close
            # ``coro``; this completes the discipline for
            # the third failure mode. Distinct from the
            # ``run_coroutine_threadsafe`` RuntimeError arm below —
            # that one knows the loop is closed; this one knows we
            # never even built the loop. Conflating them in one
            # except would route an OS-resource-exhaustion error
            # through the "event loop closed" remap, misleading
            # operators. Pinned by
            # tests/test_run_sync_ensure_loop_raise_closes_coroutine.py.
            try:
                loop = self._ensure_loop()
            except BaseException:
                coro.close()
                raise
            # ``self._timeout`` is the per-RPC-phase budget; a single
            # high-level sync call can stack up to
            # ``_SYNC_PHASES_MULTIPLIER`` phases (handshake + open + send
            # + read+drain) before the Future settles. The async surface
            # honours the per-phase contract by wrapping each RPC in
            # ``asyncio.timeout(self._timeout)`` and exposes no
            # cross-RPC ceiling, so the sync wrapper must absorb the
            # documented N × budget here or it silently fires false
            # positives on benign latency the async surface tolerates.
            sync_timeout = _SYNC_PHASES_MULTIPLIER * self._timeout
            # ``future`` is bound inside the KI-aware ``try`` below so a
            # KI/SystemExit landing between ``run_coroutine_threadsafe``
            # returning and ``future.result(...)`` entering the wait
            # still routes through the cleanup arm. The sentinel
            # ``future = None`` makes the KI cleanup arm's ``locals()``
            # lookup deterministic — if the schedule itself raised
            # ``RuntimeError("Event loop is closed")`` the inner arm
            # below remaps; if the schedule LANDED but a KI fires
            # before we entered the result wait, ``future`` is bound
            # to the scheduled future and the cleanup discipline
            # (cancel + invalidate + bounded-wait) runs against it.
            future: concurrent.futures.Future[T] | None = None
            try:
                try:
                    future = asyncio.run_coroutine_threadsafe(coro, loop)
                except RuntimeError as e:
                    # ``asyncio.run_coroutine_threadsafe`` raises bare
                    # ``RuntimeError("Event loop is closed")`` when the
                    # loop is closed between ``_ensure_loop()`` returning
                    # and the schedule call landing — the canonical race
                    # is a sibling thread (``do_terminate`` from a
                    # finalizer thread, manual ``loop.close()``, SIGTERM-
                    # with-budget shutdown). Without this catch the bare
                    # RuntimeError escapes the PEP 249 ``Error`` hierarchy
                    # (SA's ``is_disconnect`` is gated on ``DatabaseError``
                    # so it cannot classify the failure correctly), AND
                    # the unscheduled coroutine emits
                    # ``RuntimeWarning("coroutine was never awaited")`` at
                    # GC — a warning whose traceback does not point at
                    # dqlite, sending operators chasing the wrong layer.
                    # Close the coroutine and remap to ``OperationalError``
                    # (a ``DatabaseError`` subclass) with the original
                    # RuntimeError chained for diagnostics. Narrow the
                    # remap to the closed-loop substring so unrelated
                    # RuntimeErrors ("Non-thread-safe operation invoked on
                    # an event loop other than the current one" — a
                    # programmer-bug shape) propagate as themselves
                    # rather than being silently classified as a database
                    # connection failure. Close the coroutine on every
                    # arm so neither path leaks the unawaited-coroutine
                    # warning.
                    coro.close()
                    if "Event loop is closed" not in str(e):
                        raise
                    raise OperationalError(
                        f"event loop closed before coroutine could be scheduled: {e}"
                    ) from e
                # Future.result() provides a happens-before memory barrier,
                # ensuring all writes by the event loop thread are visible here.
                return future.result(timeout=sync_timeout)
            except TimeoutError as e:
                # ``TimeoutError`` can only be raised by
                # ``future.result(timeout=sync_timeout)`` above, which
                # means ``future`` is bound by this point. The assert
                # exists for mypy (which sees ``future: Future | None``
                # post-gap-window-guard) and as a defensive invariant.
                assert future is not None
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
                # If the coroutine actually completed (success branch
                # returned via ``future.result(timeout=...)`` in the
                # main try-block above; exception branch caught
                # ``recovered_error`` here), the connection is healthy:
                # ``_run_protocol``'s ``finally`` already cleared
                # ``_in_use``. Re-raise the recovered exception
                # immediately, skipping the null-out + invalidate +
                # bounded-cancel-wait blocks below — invalidating a
                # connection whose coroutine just finished cleanly
                # forces an unnecessary reconnect on every subsequent
                # sync call (silent reconnect storm under tight sync-
                # timeout tuning + slow-server / leader-flip churn).
                # The KI/SystemExit arm below has the same discipline.
                if recovered_error is not None:
                    if isinstance(recovered_error, asyncio.CancelledError):
                        raise OperationalError(
                            "Operation cancelled in async context (no meaning in sync caller)"
                        ) from recovered_error
                    # See the trailing ``recovered_error`` arm below
                    # for the ``noqa: B904`` rationale (bare ``raise``
                    # preserves causality vs the calling-thread
                    # timer).
                    raise recovered_error  # noqa: B904
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
                # coroutine that actually completed (success or late
                # server-side exception) does not get its connection
                # state torn out from under it. The success branch
                # returns ``future.result(timeout=...)`` in the main
                # try-block; the exception branch raises
                # immediately via the early-raise block (just above
                # ``future.cancel()``), so this null-out and the
                # subsequent ``_invalidate`` schedule fire only on a
                # genuine timeout where the coroutine is still in
                # flight.
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
                            OperationalError(
                                f"sync timeout after {sync_timeout}s "
                                f"({_SYNC_PHASES_MULTIPLIER} × per-phase budget "
                                f"of {self._timeout}s)"
                            ),
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
                # ``recovered_error`` is guaranteed None here: the
                # early-raise block immediately after the recovery
                # capture above re-raises the recovered exception
                # without falling through, so this point is reached
                # only on a genuine timeout (coroutine still in
                # flight; sync caller's ``Future.result(timeout=...)``
                # fired). Connection state is now ambiguous, hence
                # the unconditional ``OperationalError``.
                raise OperationalError(
                    f"Operation timed out after {sync_timeout} seconds "
                    f"({_SYNC_PHASES_MULTIPLIER} × per-phase budget of "
                    f"{self._timeout}s)"
                ) from e
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
                #
                # Gap-window guard: ``future`` may be ``None`` if the
                # KI/SystemExit landed BEFORE ``run_coroutine_threadsafe``
                # returned — e.g. PyErr_SetAsyncExc delivered to the
                # calling thread between the outer try entering and
                # the inner schedule call. ``coro`` is also unscheduled
                # in that case; close it so it does not emit
                # ``RuntimeWarning("coroutine was never awaited")`` at
                # GC. Without the guard, the cleanup arms below
                # would dereference ``future.done()`` and raise
                # ``AttributeError`` from inside the BaseException
                # handler, masking the original signal.
                if future is None:
                    coro.close()
                    raise
                #
                # Race-recovery (mirror of the TimeoutError arm
                # above): if the coroutine resolved the future
                # successfully (or with its own real exception)
                # between ``Future.result(...)`` raising the signal
                # and our cleanup, there is no wedged in-flight op
                # to poison. Skip the ``_invalidate`` schedule and
                # the synchronous ``_async_conn`` null-out so the
                # connection stays reusable on the next call. The
                # KI signal still re-raises below.
                if future.done() and not future.cancelled():
                    # ``future.cancel()`` on a done future is a no-op
                    # but still call it to keep state consistent
                    # with the wedge path below.
                    future.cancel()
                    # Drain any captured exception so asyncio doesn't
                    # log "Future exception was never retrieved".
                    with contextlib.suppress(BaseException):
                        future.result(timeout=0)
                    raise
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
                dial_timeout=getattr(self, "_dial_timeout", None),
                attempt_timeout=getattr(self, "_attempt_timeout", None),
                dial_func=getattr(self, "_dial_func", None),
            )
            # Late-publish the inner handle into the
            # ``_cleanup_loop_thread`` finalize's captured args. The
            # finalize was registered at ``_ensure_loop`` time before
            # the inner existed; mutating the captured list slot is
            # the canonical late-publish idiom for ``weakref.finalize``
            # (the finalize captures args by reference at registration
            # time). ``weakref.ref(inner)`` avoids strong-pinning the
            # inner from the finalize args — without the weakref, the
            # finalize args would form an outer→inner→outer reference
            # cycle through the outer's ``__dict__`` that prevented
            # the outer from being GC'd.
            with contextlib.suppress(Exception):
                self._inner_finalize_handle[:] = [weakref.ref(self._async_conn)]

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
        # Closed-first precedence — rationale at the canonical site
        # (``commit``): closed-conn diagnostic is more salient than
        # thread-affinity, and stdlib sqlite3 raises closed-first
        # regardless of thread. Every other public Connection method
        # (commit/rollback/cursor/transaction/execute/executemany/
        # autocommit/isolation_level/row_factory/text_factory setters)
        # orders the same way.
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._check_thread()
        # _get_async_connection is a coroutine; route through _run_sync
        # so we share the same loop-in-thread the cursor path uses.
        self._run_sync(self._get_async_connection())

    def _cascade_cursors(self) -> None:
        """Cascade close-state to every tracked cursor.

        Called from both fork-branch and main-branch arms of
        ``close()`` and ``force_close_transport()``. Mirrors stdlib
        ``sqlite3.Connection.close()``'s implicit cursor cascade and
        the async sibling ``AsyncConnection._cascade_cursors``.

        Always clears ``cur.messages`` per PEP 249 §6.4. Previously
        the four duplicated copies of this body diverged: the
        fork-branches dropped the ``del cur.messages[:]`` step that
        the main-branches included — a cascade-closed cursor in a
        forked child retained stale ``messages`` entries. The helper
        is the union, not the intersection: every cascaded cursor
        gets the full scrub regardless of fork-vs-main path.

        ``weakref.proxy(cur._connection)`` is wrapped in
        ``contextlib.suppress(TypeError)`` so a double-cascade (the
        proxy is already a proxy) is silently absorbed — same shape
        as ``Cursor.close``.
        """
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
        if get_current_pid() != self._creator_pid:
            self._closed = True
            self._closed_flag[0] = True
            # Cascade to tracked cursors so buffered fetches on them
            # stop silently answering from stale in-memory rows in
            # the child. stdlib sqlite3.Connection.close() does the
            # same; the non-fork branch below mirrors this loop.
            # Without it, a cursor inherited across fork retains
            # _closed=False and the parent's stale rows / description.
            self._cascade_cursors()
            if self._finalizer is not None:
                self._finalizer.detach()
                self._finalizer = None
            # Drop every parent-loop-bound reference so the child's GC
            # can reap the inherited daemon loop ``Thread``, asyncio
            # event loop, inner ``DqliteConnection``, and connect lock
            # without pinning them via ``threading._active`` (the
            # daemon-loop OS thread does not survive fork — only the
            # calling thread crosses POSIX ``fork(2)`` — so the Thread
            # object sits in ``_active`` indefinitely, pinning the
            # loop, which pins the selector and inherited socket FDs).
            # Mirrors the client-layer sibling ``DqliteConnection.close``
            # fork branch which nulls its parent-loop-bound state, and
            # the dbapi.aio ``AsyncConnection.close`` fork sibling.
            self._async_conn = None
            self._loop = None
            self._thread = None
            self._connect_lock = None
            self._transaction_owner = None
            return
        self._check_thread()
        self._closed = True
        # Flip the flag the finalizer reads so it knows this was an
        # explicit close (no ResourceWarning).
        self._closed_flag[0] = True
        # Cascade to tracked cursors so buffered fetches on them
        # stop silently answering from stale in-memory rows. stdlib
        # sqlite3.Connection.close() does the same. The helper writes
        # directly to the cursor's private attributes so we bypass
        # the Cursor.close() path (which would re-dispatch through
        # Cursor.messages).
        self._cascade_cursors()
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
                    # Narrow the suppression so the op_lock-acquire-
                    # timeout signal surfaces to the caller. The async
                    # sibling at ``aio/connection.py`` raises on
                    # contended close (force-closes the transport AND
                    # raises) so operators / SA pool see the recycle
                    # event. The sync side previously swallowed every
                    # ``Exception`` here, including the
                    # ``OperationalError("op_lock acquire timed out
                    # ...")`` raised by ``_run_sync`` under contention.
                    # Genuine transport / drain faults during close
                    # are still swallowed — close() is best-effort and
                    # the connection IS closed by the time control
                    # reaches the loop-teardown ``finally`` below.
                    try:
                        self._run_sync(self._close_async())
                    except OperationalError:
                        raise
                    except Exception:
                        pass
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
                    if writer is not None and self._loop is not None and not self._loop.is_closed():
                        # ``StreamWriter.close()`` mutates transport
                        # state (calls ``self._loop._remove_reader(...)``
                        # under the hood) and is documented as not
                        # thread-safe by stdlib asyncio — touching
                        # the selector from the calling thread while
                        # the loop is still running on its own thread
                        # races with the selector's transport-state
                        # bookkeeping. Schedule via
                        # ``call_soon_threadsafe`` like the sibling
                        # ``force_close_transport`` does (the sibling
                        # ``self._async_conn.force_close_transport()``
                        # call shape). FIFO discipline
                        # of the ready queue with the subsequent
                        # ``loop.stop`` queue ensures FIN goes out
                        # before ``run_forever`` exits.
                        with contextlib.suppress(RuntimeError):
                            self._loop.call_soon_threadsafe(_safe_writer_close, writer)
                    elif writer is not None:
                        # Loop is closed / unavailable — the
                        # ``_safe_writer_close`` synchronous call
                        # is the best we can do to flush FIN.
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
                        # Honour the operator's ``close_timeout`` knob
                        # for the join budget, mirroring
                        # ``force_close_transport()``. Floor at
                        # ``_LOOP_THREAD_JOIN_MIN_SECONDS`` so a tight
                        # ``close_timeout`` (down to the 0.01 s floor)
                        # still gives the queued ``loop.stop`` callback
                        # enough scheduling slack to land and the
                        # daemon thread to exit on a non-stuck loop.
                        # WAN-tuned operators set
                        # ``close_timeout >> 0.1`` and get the full
                        # configured window. A genuinely stuck loop
                        # bottoms out at the floor instead of the
                        # previous hard-coded 5 s.
                        self._thread.join(
                            timeout=max(self._close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS)
                        )
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
                            max(self._close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS),
                            exc_info=True,
                        )
                    self._loop = None
                    self._thread = None
                # Drop the asyncio.Lock bound to the loop we just closed;
                # the lazy-create branch in _get_async_connection rebuilds it
                # against the next loop so the primitive never outlives its
                # owning event loop.
                self._connect_lock = None

    def force_close_transport(self) -> None:
        """Force-close the underlying socket transport without
        awaiting any in-flight RPC.

        Synchronous, bounded by ``close_timeout``. Mirrors the async
        sibling :meth:`AsyncConnection.force_close_transport` for the
        sync path. Intended for last-resort shutdown scenarios where
        :meth:`close` would block on a stuck wire read — typically
        SQLAlchemy's ``do_terminate`` during ``engine.dispose()``
        under partition + SIGTERM, where ``close()``'s
        ``self._timeout``-bounded ``_run_sync(_close_async())`` adds
        latency the operator cannot afford.

        Idempotent. Never raises on already-closed inputs.

        Contract divergence from :meth:`close`:

        - Skips the ``_run_sync(_close_async())`` await, so a parked
          ``reader.read()`` does not gate the shutdown.
        - Schedules the synchronous ``writer.close()`` via
          ``call_soon_threadsafe`` (writer is not thread-safe per
          stdlib asyncio); the loop processes the call before
          ``loop.stop`` lands so FIN actually goes out.
        - Bounded by ``self._close_timeout`` for the thread join,
          not ``self._timeout``.
        - No ``_check_thread`` / no ``_op_lock`` acquire — terminate
          must work from finalize threads and signal handlers.
        """
        # PEP 249 §6.4 + project discipline: every public Connection
        # method clears ``messages`` as the first statement so a stale
        # entry from a prior call does not survive across the call
        # boundary. ``contextlib.suppress(AttributeError)`` tolerates
        # ``__new__``-built fixtures that bypass ``__init__``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        if self._closed:
            return
        self._closed = True
        self._closed_flag[0] = True
        # Fork-after-init: same shape as close()'s pid guard. Drop
        # local refs and skip touching the wire / dead loop.
        if get_current_pid() != self._creator_pid:
            self._cascade_cursors()
            if self._finalizer is not None:
                self._finalizer.detach()
                self._finalizer = None
            # Same parent-loop-bound nullification as close()'s fork
            # branch — see the comment block there. Without it, the
            # inherited daemon ``Thread`` + asyncio loop chain stays
            # alive in the child via ``threading._active`` until
            # interpreter exit.
            self._async_conn = None
            self._loop = None
            self._thread = None
            self._connect_lock = None
            self._transaction_owner = None
            return
        # Cascade cursors — same shape as close()'s cascade.
        self._cascade_cursors()
        if self._finalizer is not None:
            self._finalizer.detach()
            self._finalizer = None
        with self._loop_lock:
            inner = self._async_conn
            self._async_conn = None
            loop = self._loop
            # Disarm the inner client's ResourceWarning finalizer
            # (``DqliteConnection._connection_unclosed_warning``)
            # BEFORE the loop runs ``writer.close``: the warning's
            # three-flag gate fails open in the post-force-close
            # state, emitting a misleading "GC'd without close" on
            # the connection we are explicitly closing here.
            # ``close()`` detaches the inner finalizer inside
            # ``_close_impl``; ``force_close_transport`` doesn't route
            # through ``close()`` so the detach has to happen here.
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
                    # Reap any pending invalidation-drain task on the
                    # inner conn before stopping the loop. A prior
                    # ``_invalidate`` (e.g. scheduled by ``_run_sync``
                    # on a sync timeout) may have created an
                    # ``inner._pending_drain`` Task that is still in
                    # flight; without an explicit cancel queued before
                    # ``loop.stop``, ``inner`` falls out of scope after
                    # ``loop.close()`` and ``Task.__del__`` emits
                    # "Task was destroyed but it is pending" via
                    # asyncio's exception handler, plus the coroutine
                    # frame keeps the StreamReader/StreamWriter
                    # referenced (small leak per orphaned drain).
                    # Mirrors the async sibling's bounded re-snapshot
                    # reap at lines 842-911. The loop thread is still
                    # actively running until the queued ``loop.stop``
                    # processes (the ``call_soon_threadsafe`` ready
                    # queue runs in FIFO; stop is the LAST callback we
                    # queue), so a coroutine on the loop thread can
                    # call ``_invalidate`` synchronously from its
                    # except arms in client/connection.py and publish
                    # a FRESH ``_pending_drain`` task BETWEEN our
                    # snapshot and our null. The bounded loop closes
                    # the snapshot-vs-fresh-publish race; without it,
                    # the fresh task would be orphaned and surface as
                    # "Task was destroyed but it is pending" at GC.
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
                        # Cap exhausted: a racing ``_invalidate`` keeps
                        # creating fresh ``_pending_drain`` tasks each
                        # iteration. Final defensive null-out + WARNING
                        # so operators see the pathological feedback
                        # loop. Mirrors the async-sibling cap-exhausted
                        # branch in ``AsyncConnection.force_close_transport``.
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
                        # ``StreamWriter.close()`` is not thread-safe;
                        # schedule on the owning loop. The
                        # ``loop.stop`` we queue immediately afterwards
                        # is itself a ``call_soon_threadsafe`` and the
                        # loop processes ready callbacks in FIFO order,
                        # so the writer.close lands first and FIN goes
                        # out before ``run_forever`` exits.
                        with contextlib.suppress(RuntimeError):
                            loop.call_soon_threadsafe(_safe_writer_close, writer)
                with contextlib.suppress(RuntimeError):
                    loop.call_soon_threadsafe(loop.stop)
                join_budget = max(self._close_timeout, _LOOP_THREAD_JOIN_MIN_SECONDS)
                if self._thread is not None:
                    # Same floor as ``Connection.close()`` — the
                    # ``_CLOSE_TIMEOUT_FLOOR=0.01`` lower bound on
                    # ``close_timeout`` is too tight in practice for
                    # the queued ``loop.stop`` to land and the daemon
                    # thread to observe and exit cleanly.
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
        """Whether the connection currently has an open transaction.

        Callers use this in shutdown paths to decide whether to commit
        or rollback. Delegates to the underlying client-layer
        :class:`DqliteConnection` for the live "is BEGIN in flight"
        signal.

        **Divergence from stdlib**: stdlib
        ``sqlite3.Connection.in_transaction`` raises
        ``ProgrammingError`` on a closed connection; this driver
        returns ``False`` instead, by definition (a closed connection
        cannot hold an open transaction). Never-connected connections
        likewise return ``False``. This makes the getter safe to use
        in shutdown paths that need to decide whether to commit or
        rollback before close, without an extra closed-state try /
        except scaffold. Cross-driver code that relies on stdlib's
        raise behaviour to detect a closed connection should use the
        ``closed``-state probe directly, not ``in_transaction``.

        **Closed-state precedence**: the closed short-circuit runs
        BEFORE ``_check_thread()`` so a foreign-thread reader of a
        closed connection (e.g. a shutdown hook running on another
        thread) gets the documented ``False`` rather than a
        ``ProgrammingError`` thread-affinity violation. A closed
        connection is observably immutable; thread affinity becomes
        moot once close() has run.
        """
        # Snapshot the reference once so a concurrent close() that nulls
        # ``_async_conn`` cannot land between the None-check and the
        # attribute read. ``bool(...)`` keeps the mock-adapter safety
        # from the stdlib-parity introduction.
        conn = self._async_conn
        if conn is None or self._closed:
            return False
        self._check_thread()
        return bool(conn.in_transaction)

    @property
    def autocommit(self) -> "bool | int":
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

        **Setter / getter round-trip**: stores the setter input on
        ``self._autocommit_value`` and returns it. ``True`` and
        stdlib's ``LEGACY_TRANSACTION_CONTROL`` (``-1``) are both
        accepted; both no-op the wire layer (dqlite is fixed-mode
        autocommit) but the property reflects the caller's last
        input. The stdlib 3.12+ idiom ``conn.autocommit =
        sqlite3.LEGACY_TRANSACTION_CONTROL; assert conn.autocommit
        == sqlite3.LEGACY_TRANSACTION_CONTROL`` round-trips on this
        driver — the module exports ``LEGACY_TRANSACTION_CONTROL =
        -1`` precisely so this idiom works. The default (never-set)
        return is ``True``. Setting to ``False`` (or any non-
        ``True``, non-``-1`` value) raises ``NotSupportedError``.

        **Closed-state behaviour**: raises
        ``InterfaceError("Connection is closed ...")`` on a closed
        connection, matching stdlib `sqlite3`'s
        ``ProgrammingError("Cannot operate on a closed database.")``.
        Cross-driver teardown probes consulting the getter during
        dispose see a sharp diagnostic rather than the misleading
        ``True`` sentinel against a closed connection.

        **Fork-after-init**: raises ``InterfaceError("...used after
        fork...")`` when read from a forked child process. The
        ``_autocommit_value`` instance attribute is fork-inheritable
        (a plain attribute), so without the pid guard a forked child
        would read the parent's last setter input against a dead
        inner transport. Mirrors the canonical guard shape used by
        ``_stub_unsupported`` / ``_ensure_locks`` so every public
        surface on this class surfaces fork-after-init with the same
        diagnostic before any value read.
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
        # PEP 249 §6.4 + project discipline: every public state-
        # mutating method clears ``messages`` first. Closed-state
        # precedence: closed-conn diagnostic is more salient than
        # thread-affinity (matches ``row_factory.setter``,
        # ``commit``/``rollback``, ``cursor()`` discipline).
        # ``contextlib.suppress(AttributeError)`` tolerates
        # ``__new__``-built fixtures that bypass ``__init__``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        with contextlib.suppress(AttributeError):
            if self._closed:
                raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Threadsafety=1 affinity contract — even the no-op accept-path
        # is an attempt that must surface as a contract violation if
        # invoked cross-thread. Sibling ``row_factory.setter`` calls
        # ``_check_thread()`` for the same reason; the no-op-accept
        # arms here previously bypassed it.
        self._check_thread()
        # Accept ``True`` (acknowledges the existing mode) and the
        # stdlib sentinel ``sqlite3.LEGACY_TRANSACTION_CONTROL``
        # (numerically ``-1``) — stdlib's 3.12+ surface uses the
        # sentinel as the "do not change isolation" signal that
        # cross-driver code passes through. Both no-op the wire
        # layer (dqlite is fixed-mode autocommit) but we STORE the
        # caller's input so the getter round-trips. Cross-driver
        # idiom ``conn.autocommit = sqlite3.LEGACY_TRANSACTION_CONTROL;
        # assert conn.autocommit == -1`` now works on this driver —
        # the module exports ``LEGACY_TRANSACTION_CONTROL = -1``
        # for precisely this idiom.
        # Any other value (``False`` / ``0`` / ``1`` / truthy
        # non-bool) raises ``NotSupportedError`` — stdlib itself
        # enforces a similarly strict gate (no PyObject_IsTrue
        # coercion).
        #
        # **Inner-AsyncConnection slot is INDEPENDENT** — see the
        # same paragraph on ``isolation_level.setter`` below. The
        # sync setter stores on ``self._autocommit_value`` only;
        # ``self._async_conn._autocommit_value`` is not mirrored.
        # No wire-layer effect (dqlite is fixed-mode autocommit);
        # divergence is pinned by
        # ``test_property_setters_do_not_mirror_to_inner_async_conn.py``.
        if value is True or value == -1:
            self._autocommit_value: bool | int = value
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
        parity surface.

        **Setter / getter round-trip**: stores the (validated) setter
        input on ``self._isolation_level_value`` and returns it.
        ``None`` (the default), ``""`` (stdlib's default value of
        the property), and the implicit-BEGIN ``"DEFERRED"`` /
        ``"IMMEDIATE"`` / ``"EXCLUSIVE"`` variants are all accepted;
        every value no-ops the wire layer (dqlite is fixed-mode
        autocommit) but the property reflects the caller's last
        input. This preserves the canonical cross-driver
        "mirror source config to dst" idiom
        (``dst.isolation_level = src.isolation_level`` where ``src``
        is a stdlib ``sqlite3.Connection`` that defaults to ``""``)
        — the round-trip is the point of the widening.

        The default (never-set) return is ``None`` — truthful for
        dqlite's autocommit-by-default mode (the bijection
        ``autocommit=True`` ↔ ``isolation_level=None``).

        Without this property, ``conn.isolation_level = None``
        succeeded silently (Python allows arbitrary instance
        attribute writes without ``__slots__``); the user's
        attempt to express "use autocommit" had no effect on the
        driver. The property closes the silent-write footgun.

        **Closed-state behaviour**: raises ``InterfaceError`` on a
        closed connection, matching stdlib `sqlite3`'s
        ``ProgrammingError("Cannot operate on a closed database.")``
        on the equivalent getter.

        **Fork-after-init**: raises ``InterfaceError("...used after
        fork...")`` when read from a forked child process —
        ``_isolation_level_value`` is fork-inheritable. Mirrors the
        sibling ``autocommit`` getter's discipline.
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
        # PEP 249 §6.4 + closed-first precedence — see
        # ``autocommit.setter`` for the rationale.
        # ``contextlib.suppress(AttributeError)`` tolerates
        # ``__new__``-built fixtures that bypass ``__init__``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        with contextlib.suppress(AttributeError):
            if self._closed:
                raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Threadsafety=1 affinity contract — see ``autocommit.setter``
        # for the rationale (no-op accept-path is still an attempt).
        self._check_thread()
        # Accept the stdlib pre-3.12 accept-set as no-ops: ``None``,
        # ``""`` (the stdlib DEFAULT value of the property), and the
        # implicit-BEGIN ``"DEFERRED"`` / ``"IMMEDIATE"`` /
        # ``"EXCLUSIVE"`` variants. dqlite is fixed-mode autocommit
        # at the wire layer, so all five collapse to the same
        # behaviour — accepting them preserves the canonical
        # cross-driver "mirror source config to dst" idiom (``dst
        # .isolation_level = src.isolation_level`` where ``src`` is
        # a stdlib ``sqlite3.Connection`` that defaults to ``""``).
        # We STORE the caller's input on ``self._isolation_level_value``
        # so the getter round-trips — without storage, the canonical
        # cross-driver idiom assigns the value silently and the next
        # read returns the default. Mirrors the ``autocommit.setter``
        # storage discipline established in the same widening round.
        #
        # **Inner-AsyncConnection slot is INDEPENDENT.** The setter
        # stores on the sync wrapper only; ``self._async_conn
        # ._isolation_level_value`` is not mirrored. The wire layer
        # no-ops every accepted value (dqlite is fixed-mode
        # autocommit), so divergence between the sync and inner slot
        # has no observable effect on SQL execution. Callers reading
        # the inner's view explicitly via ``conn._async_conn
        # .isolation_level`` (a private accessor) get the inner's
        # default, not the outer's last-set value — by design.
        # Threading the setter write across the loop-thread boundary
        # to mirror would introduce ordering hazards the rest of the
        # sync surface deliberately avoids, with zero behavioural
        # benefit. The pin in
        # ``test_property_setters_do_not_mirror_to_inner_async_conn.py``
        # locks the divergence so a future refactor that accidentally
        # couples the slots without re-thinking the threading model
        # surfaces fast.
        #
        # Genuinely invalid values (non-string, unknown string)
        # raise ``ProgrammingError`` (PEP 249 §7 "caller-shape
        # misuse"), NOT ``NotSupportedError`` (which is for
        # features the database lacks).
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
        # Closed-state precedence: closed-conn diagnostic is more
        # salient than thread-affinity. Async sibling at
        # ``aio/connection.py`` orders closed-first; sync siblings
        # historically diverged. Stdlib sqlite3 also raises closed-
        # first regardless of thread.
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._check_thread()
        # Reject explicit ``commit()`` from inside ``with
        # conn.transaction():`` body. The ctxmgr owns transaction
        # boundaries — a stray commit ends the transaction without
        # exiting the block, and the surrounding rollback-at-exit
        # then no-ops because ``in_transaction`` is already False.
        # Mirrors the async sibling's stray-commit reject arm in
        # ``AsyncConnection.commit``.
        # ``getattr`` so test helpers that build via ``Connection.__new__``
        # (skipping ``__init__``) without seeding the slot don't crash;
        # production paths always have the attribute from ``__init__``.
        _tx_owner = getattr(self, "_transaction_owner", None)
        if _tx_owner is not None and _tx_owner == threading.get_ident():
            raise InterfaceError(
                "commit() cannot be issued inside conn.transaction(); "
                "the context manager owns transaction boundaries — "
                "exit the ``with`` block first."
            )
        if self._async_conn is None:
            return
        # Cancel-after-invalidate contract — see async sibling
        # ``aio/connection.py``'s ``commit()`` for full rationale.
        # A prior commit/rollback cancelled mid-flight invalidates
        # the inner client conn AND clears its ``in_transaction``
        # flag. A naive retry would then short-circuit on the False
        # flag and silently return — hiding partial-commit
        # ambiguity (the cancelled commit may or may not have
        # reached the leader). Raise BEFORE the ``in_transaction``
        # short-circuit. The sync version doesn't need the in-lock
        # recheck (no async race window — ``_check_thread`` makes
        # the sync caller single-threaded relative to itself).
        if getattr(self._async_conn, "_protocol", "_sentinel") is None:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}); reconnect before "
                "retrying commit / rollback. The prior call may have "
                "reached the leader before cancel landed; server-side "
                "transaction state is ambiguous."
            )
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
        # Closed-state precedence — see ``commit`` for full rationale.
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._check_thread()
        # Reject stray ``rollback()`` from inside ``with
        # conn.transaction():`` body — the ctxmgr owns boundaries.
        # Mirrors the async sibling and the same-shape guard in
        # ``commit()``.
        # ``getattr`` so test helpers that build via ``Connection.__new__``
        # (skipping ``__init__``) without seeding the slot don't crash;
        # production paths always have the attribute from ``__init__``.
        _tx_owner = getattr(self, "_transaction_owner", None)
        if _tx_owner is not None and _tx_owner == threading.get_ident():
            raise InterfaceError(
                "rollback() cannot be issued inside conn.transaction(); "
                "the context manager owns transaction boundaries — "
                "raise from inside the ``with`` block to trigger "
                "rollback-at-exit, or exit the block first."
            )
        if self._async_conn is None:
            return
        # Cancel-after-invalidate guard — see ``commit`` for the full
        # rationale. Raise BEFORE the ``in_transaction`` short-circuit
        # so a post-``_invalidate`` rollback surfaces as
        # ``InterfaceError`` instead of silently no-opping.
        if getattr(self._async_conn, "_protocol", "_sentinel") is None:
            raise InterfaceError(
                f"Connection invalidated (id={id(self)}); reconnect before "
                "retrying commit / rollback. The prior call may have "
                "reached the leader before cancel landed; server-side "
                "transaction state is ambiguous."
            )
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

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Synchronous context manager wrapping ``BEGIN`` / ``COMMIT``
        / ``ROLLBACK``.

        Mirrors :meth:`AsyncConnection.transaction` and the canonical
        sync-DB-API pattern used by ``psycopg.Connection.transaction``
        and ``psycopg2.connection``. Without this method,
        ``with conn.transaction(): ...`` raised ``AttributeError``
        outside the ``dbapi.Error`` hierarchy — cross-driver porting
        code's ``except dbapi.Error:`` arm could not catch it.

        Issues ``BEGIN`` on enter, ``COMMIT`` on clean exit,
        ``ROLLBACK`` on exception. Stray :meth:`commit` /
        :meth:`rollback` from inside the body raise ``InterfaceError``
        — the ctxmgr owns boundaries. Nested ``with
        conn.transaction()`` raises ``InterfaceError``. Closed
        connections raise ``InterfaceError`` on enter.

        The sync surface emits ``BEGIN`` / ``COMMIT`` / ``ROLLBACK``
        directly through a cursor rather than driving the client
        layer's task-scoped ``transaction()`` async ctxmgr — the
        sync caller is single-threaded by ``_check_thread`` so the
        client's task-affinity guard would gratuitously reject
        sequential ``_run_sync`` calls inside one ``with`` block.
        """
        del self.messages[:]
        if self._closed:
            raise InterfaceError(f"Connection is closed (id={id(self)})")
        self._check_thread()
        if self._transaction_owner is not None:
            raise InterfaceError(
                f"Nested conn.transaction() not supported (id={id(self)}); "
                "exit the outer block before opening a new one."
            )
        cursor = self.cursor()
        # Set the owner slot INSIDE the try frame so a BaseException
        # (KeyboardInterrupt / SystemExit) at the bytecode boundary
        # between the assignment and the try-setup cannot leak the
        # slot pinned to a now-dying thread. Mirrors the async sibling.
        token = threading.get_ident()
        try:
            cursor.execute("BEGIN")
            # Set owner AFTER BEGIN — if BEGIN itself raises, the
            # ctxmgr never enters its body and the owner slot stays
            # clear so the caller's error-handling can still issue
            # commit/rollback.
            self._transaction_owner = token
            try:
                yield
            except BaseException:
                # Best-effort rollback: emit ROLLBACK while temporarily
                # clearing the owner slot so the cursor's commit/
                # rollback affordance through this same connection
                # would not trip the owner-token guard. Reset it in
                # the outer finally regardless. Suppress any
                # exception from ROLLBACK so the caller's original
                # exception is what propagates — chaining via
                # ``__context__`` is automatic.
                self._transaction_owner = None
                try:
                    with contextlib.suppress(Exception):
                        cursor.execute("ROLLBACK")
                finally:
                    self._transaction_owner = token
                raise
            else:
                # Clear the owner slot before COMMIT so the
                # cursor.execute("COMMIT") path doesn't trip the
                # owner-token guard at any layer that might check it
                # in the future. Reset in the outer finally.
                self._transaction_owner = None
                try:
                    cursor.execute("COMMIT")
                finally:
                    self._transaction_owner = token
        finally:
            # Only clear if we still own the slot — defensive against
            # a hypothetical re-entry that shouldn't be reachable
            # given the guard above. Mirrors the async sibling, which
            # uses ``is`` because tasks are unique objects; the sync
            # token is a thread id (small int) so ``==`` is the right
            # comparison (CPython interns small ints but the contract
            # is not guaranteed at the language level).
            if self._transaction_owner == token:
                self._transaction_owner = None
            with contextlib.suppress(Exception):
                cursor.close()

    def cursor(self, **unknown_kwargs: object) -> Cursor:
        """Return a new Cursor object.

        Reject unknown kwargs (notably stdlib's ``factory=`` Cursor-
        subclass hook) with ``NotSupportedError`` so cross-driver
        porting code's ``except dbapi.Error:`` catches the rejection
        rather than the bare ``TypeError`` Python raises for an
        unexpected kwarg. Symmetric with ``connect()``'s
        ``**unknown_kwargs`` pattern that rejects stdlib-only kwargs
        with ``NotSupportedError`` rather than silently ignoring them.
        """
        del self.messages[:]
        # Affinity precedence: closed → thread → kwarg-shape. Surface
        # the most-salient diagnostic first. Stdlib sqlite3 and the
        # in-package ``Cursor.execute`` rationale (cursor.py) both
        # order closed-state ahead of input-shape rejection; the
        # thread check is hoisted ABOVE the kwarg reject for the same
        # reason — a foreign-thread caller passing ``factory=...``
        # should see ``ProgrammingError("...same thread...")`` rather
        # than ``NotSupportedError(unknown kwarg)``, otherwise the
        # next no-kwarg call would still fail on the thread check and
        # the operator gets two different diagnostics for the same
        # underlying misuse. NOTE: ``_stub_unsupported`` deliberately
        # skips the thread check (rejection is universal there —
        # those features are unsupported in every state); the asymmetry
        # is intentional, documented at ``_stub_unsupported``.
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
        self._cursors.add(cur)
        # Re-check ``_closed`` after add: ``cursor()`` is creator-
        # thread-only but ``force_close_transport`` is documented as
        # callable from finalize threads / signal handlers / SA-pool
        # reclaim threads (no ``_check_thread`` / no ``_op_lock``
        # acquire; see ``force_close_transport`` docstring). The
        # cascade snapshot at ``list(self._cursors)`` can therefore
        # run on a sibling thread between this method's prelude
        # ``if self._closed:`` check and the WeakSet add — and the
        # freshly-built cursor would skip the cascade scrub and be
        # returned to the caller with ``_closed=False`` and a strong
        # ref to the now-dead Connection. Mirrors the async sibling
        # at ``aio/connection.py`` (the verbatim re-check + scrub +
        # discard block). Defense-in-depth: if ``_closed`` flipped
        # during the construction-and-add window, apply the same
        # scrub the cascade would have applied AND discard the entry
        # so ``self._cursors`` matches the ``_cascade_cursors``
        # postcondition (empty after close). Without ``discard()``,
        # post-close diagnostics that read ``len(conn._cursors)``
        # see a stale count, and any future cascade field added
        # (e.g. a buffer pointer) would silently leak on the late-
        # added cursor.
        if self._closed:
            cur._closed = True
            cur._rows = []
            cur._description = None
            cur._rowcount = -1
            cur._lastrowid = None
            cur._row_index = 0
            del cur.messages[:]
            # Do NOT swap ``cur._connection`` to a ``weakref.proxy``
            # here. The cursor is already scrubbed (``_closed=True``,
            # fields cleared) and discarded from ``_cursors`` below,
            # so the "no strong-pin of the closed Connection" rationale
            # does not apply: a closed cursor's strong ref to its
            # Connection is metadata-only (no operational state is
            # reached through it). Preserving the strong ref keeps
            # PEP 249 §6.5.1 identity (``cursor.connection is conn``)
            # AND hashability (``weakref.proxy`` instances are not
            # hashable) on the race-leaked path, matching what the
            # non-race path already does for every other closed cursor.
            self._cursors.discard(cur)
        return cur

    def execute(
        self,
        operation: str,
        parameters: Sequence[Any] | None = None,
        /,
    ) -> Cursor:
        """Stdlib ``sqlite3.Connection`` convenience extension (NOT part
        of PEP 249 §10 optional extensions) — open a cursor, run
        ``execute``, return the cursor.

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
        # PEP 249 §6.4: messages list is cleared automatically by all
        # standard connection methods. ``self.cursor()`` does its own
        # clear, but eagerly clearing here aligns this shortcut's
        # prelude shape with the seven other public Connection
        # methods that all clear-before-anything-else.
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
        """Stdlib ``sqlite3.Connection`` convenience extension (NOT part
        of PEP 249 §10 optional extensions) — open a cursor, run
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
        # PEP 249 §6.4: see ``execute`` shortcut for the eager-clear
        # rationale.
        del self.messages[:]
        # Closed-state precedence: route through ``self.cursor()`` so
        # a closed connection raises ``InterfaceError`` BEFORE the
        # outer-shape check fires. Mirrors ``Connection.cursor()``'s
        # ordering: closed → cross-thread → input-shape. The
        # outer-shape check moves AFTER cursor construction so a
        # closed connection that receives a bad-shape ``seq`` raises
        # the closed-state ``InterfaceError`` (cross-driver feature-
        # probe / pool-recycle hooks expect that class), not a
        # shape ``ProgrammingError``.
        cur = self.cursor()
        # Reject the outer shapes that would silently iterate over keys
        # (dict) / characters (str / bytes / bytearray / memoryview), or
        # iterate in non-deterministic order (set / frozenset). The
        # shared ``_validate_executemany_seq_shape`` helper is the
        # single source of truth so this shortcut and
        # ``Cursor.executemany`` produce one diagnostic and one
        # accept/reject contract. ``Mapping`` at large is NOT rejected
        # so an OrderedDict-of-rows pattern still works — only literal
        # ``dict`` (the common single-row misuse) is denied.
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

        Mirrors the psycopg / asyncpg convention so callers porting
        idempotent-close patterns (``if not conn.closed: conn.close()``)
        do not hit ``AttributeError``. PEP 249 does not mandate this
        property; stdlib ``sqlite3`` famously omits it. The underlying
        flag is already maintained by every method that mutates
        closed-ness.

        Peer-driver parity (psycopg, asyncpg) — both return True for
        invalidated connections so cross-driver code branching on
        ``conn.closed`` to drive reconnect heuristics works correctly.
        Use :attr:`invalidated` if you specifically need to distinguish
        the two states. Mirrors the async sibling at
        ``aio/connection.py``'s ``AsyncConnection.closed``.
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
        ``invalidated`` lets callers distinguish. Mirrors the async
        sibling ``AsyncConnection.invalidated``.
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

        Set to a callable ``factory(cursor, row) -> Any`` to wrap
        each fetched tuple before returning. ``None`` (default)
        returns plain tuples per PEP 249. New cursors inherit this
        default; assigning ``cur.row_factory = ...`` overrides
        per-cursor.

        **Factory contract**: factories that require a specific
        ``Cursor`` subclass — notably ``sqlite3.Row``, whose
        C-extension constructor type-checks the first argument to
        be ``pysqlite_CursorType`` — do NOT work with this driver.
        The setter accepts them, but the first fetch surfaces
        ``DataError("row_factory call failed: argument 1 must be
        sqlite3.Cursor, not Cursor")``.

        Use plain callables (lambdas, dataclass builders,
        ``namedtuple._make``) instead. See ``Cursor.row_factory``
        for the recommended shapes.
        """
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        # State-mutating setter — enforce the same closed-then-thread
        # discipline as every other public method on this class. The
        # getter intentionally bypasses the check (read-only / GIL-
        # atomic), but a closed-conn setter mutation would silently
        # succeed and a cross-thread setter mutation would let a
        # foreign thread override the row_factory for cursors created
        # by the creator thread. Mirrors ``Cursor.row_factory.setter``
        # which has both checks. ``del self.messages[:]`` mirrors the
        # PEP 249 §6.4 + project discipline applied to every public
        # state-mutating method. ``contextlib.suppress(AttributeError)``
        # tolerates ``__new__``-built fixtures that bypass ``__init__``.
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
        """stdlib ``sqlite3.Connection.text_factory``-parity stub.

        dqlitedbapi always returns TEXT cells as ``str`` (UTF-8
        decoded at the wire layer); custom text-factory routing is
        not supported. Setter rejects non-``str`` values with
        ``NotSupportedError`` so a silent write cannot happen."""
        return str

    @text_factory.setter
    def text_factory(self, value: object) -> None:
        # PEP 249 §6.4 + closed-first precedence — see
        # ``autocommit.setter`` for the rationale.
        # ``contextlib.suppress(AttributeError)`` tolerates
        # ``__new__``-built fixtures that bypass ``__init__``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        with contextlib.suppress(AttributeError):
            if self._closed:
                raise InterfaceError(f"Connection is closed (id={id(self)})")
        # Threadsafety=1 affinity contract — see ``autocommit.setter``
        # for the rationale.
        self._check_thread()
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

    # ``*args, **kwargs`` shape so any caller signature — positional,
    # keyword, novel-PEP-249-extension kwarg — reaches
    # ``_stub_unsupported`` and surfaces ``NotSupportedError`` inside
    # the ``dqlitedbapi.Error`` hierarchy. Tightly-typed signatures
    # leak bare ``TypeError`` outside the hierarchy, breaking cross-
    # driver feature-probe code (``except dbapi.Error: ...``).
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
        """Shared helper for ``NotSupportedError`` stubs: clear
        ``self.messages`` per PEP 249 §6.4 messages-clear contract,
        check fork-after-init (canonical ``InterfaceError`` per the
        project-wide convention) and closed-state per stdlib
        ``sqlite3`` precedence, then raise.

        Pid check runs BEFORE the closed-check so a forked child sees
        the canonical fork diagnostic (``InterfaceError``, routes
        through cross-driver retry middleware) instead of
        ``NotSupportedError`` (``DatabaseError``-subtree class, not
        recognised by ``is_disconnect`` classifiers). ``_check_thread``
        is still NOT applied — these are universally-unsupported
        regardless of state, and thread-affinity creep here is beyond
        what the stub represents. The pid check is structurally
        different: it surfaces an existential "this object is no
        longer addressable" condition, not a per-thread misuse.

        ``contextlib.suppress(AttributeError)`` tolerates
        ``__new__``-built fixtures that bypass ``__init__`` and so
        lack ``_closed`` / ``messages`` / ``_creator_pid``."""
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

    # stdlib ``sqlite3.Connection``-parity stubs for VDBE-callback
    # / db-status / db-config / serialize / blob-open primitives.
    # None are wire-feasible (the VDBE / pager runs server-side; no
    # client-callable hook). Stub with ``NotSupportedError`` so the
    # rejection stays inside the ``dbapi.Error`` hierarchy instead
    # of leaking ``AttributeError``. Same family as the existing
    # ``load_extension`` / ``backup`` / ``iterdump`` / ``create_*``
    # stubs. Each stub routes through ``_stub_unsupported`` which
    # performs the messages-clear and closed-check prelude per
    # PEP 249 §6.4 + stdlib precedence.

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
        """dqlite-server does not surface a total_changes counter on
        the wire.

        Stdlib ``sqlite3.Connection.total_changes`` is an int-valued
        attribute. This driver exposes it as a callable stub
        (parens required) to keep the ``hasattr(conn, "total_changes")``
        invariant that the rest of the stub family relies on —
        ``hasattr`` would propagate the ``NotSupportedError`` raised
        from a property-getter, breaking cross-driver feature-probe
        code. Pinned by tests/test_total_changes_hasattr_safe.py and
        tests/test_pep249_stub_hasattr_divergence.py.
        """
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
        # ``*args/**kwargs`` shape so any caller signature — including
        # the zero-arg form a typing-confused operator might write —
        # reaches ``_stub_unsupported`` and surfaces a
        # ``NotSupportedError`` inside the ``dqlitedbapi.Error``
        # hierarchy. Tightly-typed signatures leak bare ``TypeError``
        # outside the hierarchy, breaking cross-driver feature-probe
        # code (``except dbapi.Error: ...``).
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
        # Spell ``filter=`` explicitly so ``inspect.signature`` matches
        # the documented stdlib-3.13 shape ``(*, filter=None)`` for
        # cross-driver tooling that walks the dbapi-connection API
        # surface (doc generators, IDE auto-complete,
        # compatibility-shim detection). ``**kwargs`` still absorbs
        # any future Python additions so callers route through
        # ``_stub_unsupported`` rather than leaking a bare ``TypeError``
        # outside the ``dqlitedbapi.Error`` hierarchy. Stdlib's
        # ``iterdump`` is keyword-only (no positional args after
        # ``self``); we mirror that to surface positional misuse as a
        # bare ``TypeError`` at the call site, matching the stdlib
        # diagnostic rather than masking it.
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
        # Sibling-discipline with the client-layer ``DqliteConnection.__repr__``
        # at ``dqliteclient/connection.py``: route ``_address`` through
        # ``sanitize_for_log`` before ``!r`` so an attacker-influenced
        # address (custom ``dial_func`` or leader-redirect target that
        # survived ``parse_address``) renders with the same operator-
        # readable ``?`` substitution everywhere the address appears in
        # logs, not the cosmetically-different `` `` escape Python's
        # ``str.__repr__`` would produce. The client commit
        # ``Strip invisible-character class from address and server-text
        # interpolations`` motivated the discipline; this is the one-
        # layer-up sibling for the dbapi surface.
        safe_addr = sanitize_for_log(str(self._address))
        return f"<Connection address={safe_addr!r} database={self._database!r} {state}>"

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
        """Materialise the underlying connection and return self.

        .. note::

            **Asymmetric lifecycle** (mirrors the async sibling
            :meth:`AsyncConnection.__aenter__`). ``__enter__`` calls
            :meth:`connect`, but :meth:`__exit__` performs commit /
            rollback only — it does **NOT** close the connection
            (matches stdlib ``sqlite3.Connection.__exit__``). After
            ``with`` exits the underlying socket + loop thread are
            still alive; call ``conn.close()`` explicitly or hand
            ownership to a pool. See :meth:`__exit__` for the
            stdlib-parity rationale.
        """
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
        """Commit on clean exit, rollback on exception, do NOT close.

        Matches stdlib ``sqlite3.Connection`` parity: ``__exit__``
        finishes the transaction (commit on clean exit, rollback on
        exception) but leaves the underlying connection open so the
        same instance is reusable in a subsequent ``with`` block. This
        is the sync sibling of ``AsyncConnection.__aexit__`` and
        carries the same no-close contract.

        Both arms tolerate KeyboardInterrupt / SystemExit landing
        mid-COMMIT or mid-ROLLBACK: server-side state may be ambiguous
        if the request reached the leader before the signal landed,
        but the signal still propagates faithfully (with a DEBUG
        breadcrumb for operator forensics).
        """
        # If no query has ever run, there's no transaction to commit or
        # roll back — nothing to do; the connection remains reusable,
        # matching stdlib sqlite3 / psycopg semantics.
        if self._async_conn is None:
            return
        if exc_type is None:
            # Clean exit: commit. Let exceptions propagate; silent
            # data loss is worse than a noisy failure. A KI / SystemExit
            # landing inside the COMMIT round-trip is partial-commit-
            # ambiguous (the COMMIT may or may not have reached the
            # leader); log a DEBUG breadcrumb so operators can correlate
            # the cancelled close with the source signal — symmetric
            # with the async sibling (see ``AsyncConnection.__aexit__``'s
            # rollback breadcrumb arm in ``aio/connection.py``) and with
            # this method's own rollback arm below.
            try:
                self.commit()
            except (KeyboardInterrupt, SystemExit):
                logger.debug(
                    "Connection.__exit__ (address=%s, id=%s): "
                    "clean-exit commit interrupted by signal; "
                    "transaction state may be ambiguous (commit-or-not "
                    "on the leader)",
                    self._address,
                    id(self),
                    exc_info=True,
                )
                raise
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
        # psycopg2. (psycopg3 closes on exit; we deliberately don't,
        # because closing-on-context-manager-exit conflicts with common
        # SA-style usage where Connection lifetimes outlive a single
        # ``with conn:`` block.) Callers who want eager close use
        # ``conn.close()`` explicitly or go through a pool.


# PEP 249 optional parity extension mirroring the exception-class
# attributes on ``Connection``: expose the ``Cursor`` class so cross-
# driver adapter / instrumentation code can ``isinstance(cur,
# conn.Cursor)`` without importing ``dqlitedbapi.cursor``. Assigned
# outside the class body to avoid shadowing the ``Cursor`` type name
# in method annotations within the class scope. Not a
# ``cursor_factory`` hook — ``cursor()`` still instantiates ``Cursor``
# directly; this is purely an introspection / isinstance-check surface.
Connection.Cursor = Cursor  # type: ignore[attr-defined]
