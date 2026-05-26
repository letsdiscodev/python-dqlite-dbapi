"""PEP 249 Cursor implementation for dqlite."""

import asyncio
import contextlib
import re
import weakref
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Mapping, Sequence, Sized
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, NoReturn, Protocol, Self

import dqliteclient.exceptions as _client_exc
from dqlitedbapi._constants import cluster_policy_rejection_message
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from dqlitedbapi.types import (
    UNKNOWN as _UNKNOWN_TYPE,
)
from dqlitedbapi.types import (
    RowFactory,
    _convert_bind_param,
    _datetime_from_iso8601,
    _datetime_from_unixtime,
    _DBAPIType,
    _Description,
)
from dqlitewire import (
    BARE_DATABASE_ERROR_CODES,
    DQLITE_NOTFOUND,
    DQLITE_PARSE,
    DQLITE_PROTO,
    SQLITE_AUTH,
    SQLITE_CONSTRAINT,
    SQLITE_INTERNAL,
    SQLITE_MISMATCH,
    SQLITE_MISUSE,
    SQLITE_NOLFS,
    SQLITE_NOMEM,
    SQLITE_NOTFOUND,
    SQLITE_NOTICE,
    SQLITE_RANGE,
    SQLITE_TOOBIG,
    SQLITE_WARNING,
    ValueType,
    primary_sqlite_code,
    sanitize_for_log,
)
from dqlitewire import EncodeError as _WireEncodeError

__all__ = ["Cursor"]


# Primary codes routed to specific PEP 249 classes. The constants
# live in ``dqlitewire.constants`` alongside the rest of the SQLite
# primary set so the wire layer is the single source of truth for
# numeric code values. The extended ``SQLITE_CONSTRAINT_*`` family
# (``CHECK = 275``, ``UNIQUE = 2067``, ``FOREIGNKEY = 787``, etc.)
# share ``code & 0xFF == 19``; the lookup at the call site uses
# ``primary_sqlite_code(...)`` to mask before the registry probe.
#
# Registry rationale (stdlib ``sqlite3``'s ``util.c::get_exception_class``):
# - SQLITE_INTERNAL (2) → InternalError ("cursor not valid anymore")
# - SQLITE_NOMEM (7) → InternalError (stdlib raises MemoryError; we
#   route through PEP 249's hierarchy so ``except dbapi.Error:``
#   continues to catch)
# - SQLITE_NOTFOUND (12) → InternalError (groups with INTERNAL /
#   IOERR_VNODE / IOERR_CONVPATH per stdlib). The dqlite-namespace
#   twin ``DQLITE_NOTFOUND`` (1002) is a separate "server-side
#   scratch registry miss" → ProgrammingError.
# - SQLITE_TOOBIG (18) → DataError ("value exceeds size limit")
# - SQLITE_CONSTRAINT (19) plus extended family → IntegrityError
# - SQLITE_MISMATCH (20) → IntegrityError (stdlib groups with
#   CONSTRAINT for STRICT-table datatype mismatch; cross-driver
#   parity matters more than the defensible-DataError reading)
# - SQLITE_MISUSE (21) → InterfaceError (driver-interface misuse)
# - SQLITE_RANGE (25) → InterfaceError (bind index out of range)
# - SQLITE_NOLFS (22), SQLITE_AUTH (23), SQLITE_NOTICE (27),
#   SQLITE_WARNING (28) → DatabaseError (stdlib's ``default:`` arm;
#   forward-compat parity for codes dqlite-server does not currently
#   emit)
# - SQLITE_CORRUPT (11), SQLITE_FORMAT (24), SQLITE_NOTADB (26) →
#   DatabaseError (server-side file malformed / wrong format)

# Registry of primary-code → PEP 249 class. Keep the default
# (OperationalError) outside the dict so adding a code is one line.
_CODE_TO_EXCEPTION: Final[
    dict[
        int,
        type[
            OperationalError
            | IntegrityError
            | InternalError
            | DataError
            | ProgrammingError
            | DatabaseError
            | InterfaceError
        ],
    ]
] = {
    SQLITE_CONSTRAINT: IntegrityError,
    SQLITE_INTERNAL: InternalError,
    SQLITE_TOOBIG: DataError,
    SQLITE_MISMATCH: IntegrityError,
    SQLITE_RANGE: InterfaceError,
    SQLITE_MISUSE: InterfaceError,
    SQLITE_NOTFOUND: InternalError,
    SQLITE_NOMEM: InternalError,
    # Wire SSOT for slot-fatal "bare DatabaseError" routes:
    # ``BARE_DATABASE_ERROR_CODES`` (SQLITE_CORRUPT / SQLITE_FORMAT /
    # SQLITE_NOTADB). The SQLAlchemy adapter derives its slot-fatal
    # classification from the same wire set
    # (``sqlalchemydqlite/base.py::_BARE_DBE_DISCONNECT_CODES``);
    # deriving here too keeps the two consumers aligned automatically
    # if the wire set grows. The dict-merge spread is dict-literal-safe
    # because the SSOT codes do not collide with any of the entries
    # above.
    **{code: DatabaseError for code in BARE_DATABASE_ERROR_CODES},
    # Codes that intentionally fall through to the OperationalError
    # default (no explicit entry needed): BUSY, LOCKED, READONLY,
    # IOERR, FULL, CANTOPEN, EMPTY, SCHEMA, PROTOCOL (15), PERM,
    # ABORT, INTERRUPT, ERROR. These are stdlib's OperationalError
    # codes; we used to enumerate PROTOCOL explicitly as
    # "documentary" but that just invited symmetry pressure to add
    # 12 more no-op entries.
    # Defensive pass-through entries (NOT in the wire SSOT because
    # dqlite-server does not currently emit them): route to bare
    # DatabaseError so a future server release that surfaces one does
    # not land on the unclassified-OperationalError default. CPython
    # stdlib parity — see the primary-code constants above.
    SQLITE_NOLFS: DatabaseError,
    SQLITE_AUTH: DatabaseError,
    SQLITE_NOTICE: DatabaseError,
    SQLITE_WARNING: DatabaseError,
    # dqlite-namespace error codes (>= 1000). ``primary_sqlite_code``
    # passes them through unchanged (see ``dqlitewire.constants``),
    # so the dispatch table keys match the code observed on the wire.
    # Upstream emission sites:
    # - ``gateway.c::handle_request_*`` paths emit ``DQLITE_PROTO``
    #   for "unrecognised request type" and similar protocol-misuse
    #   replies → ``InterfaceError`` per PEP 249 §6.
    # - ``gateway.c::handle_request_open`` emits ``DQLITE_NOTFOUND``
    #   for "database does not exists" → ``ProgrammingError``
    #   (database-name typo / stale config).
    # - ``gateway.c`` emits ``DQLITE_PARSE`` for schema-version
    #   mismatch / unrecognised cluster format / unrecognised request
    #   type → ``ProgrammingError`` (caller-fault).
    DQLITE_PROTO: InterfaceError,
    DQLITE_NOTFOUND: ProgrammingError,
    DQLITE_PARSE: ProgrammingError,
}


def _classify_operational(
    code: int | None,
) -> type[
    OperationalError
    | IntegrityError
    | InternalError
    | DataError
    | ProgrammingError
    | DatabaseError
    | InterfaceError
]:
    """Pick a PEP 249 exception class from a SQLite or dqlite error code.

    Returns OperationalError for unknown / unmapped codes so the
    existing "anything can surface as OperationalError" contract
    holds; mapped codes surface as their PEP 249 equivalent
    (IntegrityError, InternalError, DataError, ProgrammingError,
    InterfaceError). Dqlite-namespace codes (DQLITE_PROTO/PARSE/
    NOTFOUND, ≥ 1000) bypass the SQLite-primary mask via
    ``primary_sqlite_code`` and dispatch directly.
    """
    if code is None:
        return OperationalError
    return _CODE_TO_EXCEPTION.get(primary_sqlite_code(code), OperationalError)


async def _call_client[T](coro: Awaitable[T]) -> T:
    """Await a client-layer coroutine, mapping its exceptions into the
    PEP 249 hierarchy. Preserves the original via ``from``.

    Mapping (rows in match-order — ClusterPolicyError is matched
    before ClusterError because it is the more specific subclass;
    wire.EncodeError is matched before InterfaceError because the
    code path-checks it before falling through to InterfaceError):
      client.OperationalError (constraint code) → dbapi.IntegrityError
      client.OperationalError (other codes)     → dbapi.OperationalError
      client.DqliteConnectionError → dbapi.OperationalError (network flavor)
      client.ClusterPolicyError    → dbapi.InterfaceError
      client.ClusterError          → dbapi.OperationalError
      client.ProtocolError         → dbapi.OperationalError
      client.DataError             → dbapi.DataError
      wire.EncodeError             → dbapi.DataError
      client.InterfaceError        → dbapi.InterfaceError
      any other DqliteError        → dbapi.DatabaseError

    Every ``dqliteclient`` exception is a subclass of ``DqliteError``;
    the trailing catch-all ensures a new client exception class cannot
    bypass PEP 249 wrapping. PEP 249 requires all database-sourced
    errors to surface as ``Error`` subclasses. ``DatabaseError`` is the
    conservative fallback (rather than ``InterfaceError``) so that a
    future ``dqliteclient.CircuitOpenError`` or similar — which is a
    server / cluster-state condition rather than a driver-interface
    misuse — is classified correctly: cross-driver code using
    ``except DatabaseError:`` for server-sourced failures catches
    future error classes; ``InterfaceError`` is reserved for driver-
    misuse shapes (closed cursor, wrong-type args, etc.).
    """
    try:
        return await coro
    except _client_exc.OperationalError as e:
        # Classify by SQLite extended error code. Constraint violations
        # (primary code 19) become IntegrityError per PEP 249; everything
        # else stays OperationalError so callers that branch on
        # leader-change / busy codes (``_is_no_transaction_error``, the
        # SQLAlchemy dialect's ``is_disconnect``) continue to work.
        # Use ``e.message`` directly. Equivalent to ``str(e)`` now that
        # ``OperationalError.__str__`` no longer prefixes ``[code]``; kept
        # as ``e.message`` for explicit attribute access — the code is
        # carried separately on the ``code`` keyword and we do not want it
        # in the message text twice if ``__str__`` ever re-introduces the
        # prefix.
        exc_cls = _classify_operational(e.code)
        # Plumb the full server text through ``raw_message`` so callers
        # that want the un-truncated diagnostic (operators reading
        # logs, structured-error tooling) don't have to walk
        # ``__cause__`` for it. ``message`` stays truncated for safe
        # default ``str(exc)``.
        #
        # InterfaceError carries the same ``code`` / ``raw_message``
        # surface as DatabaseError so SA's ``is_disconnect`` and
        # operator log tooling can branch on the wire code without
        # walking ``__cause__``. Symmetric with the DatabaseError
        # branch; both subclass code-bearing surfaces.
        raise exc_cls(e.message, code=e.code, raw_message=e.raw_message) from e
    except _client_exc.DqliteConnectionError as e:
        # DqliteConnectionError optionally carries the SQLite code
        # from a leader-change rewrap on the connect path; thread it
        # so SA's is_disconnect code-based classifier can fire on
        # both connect and query paths. The ``raw_message`` accessor
        # comes from the ``DqliteError`` base — callers reading the
        # un-truncated diagnostic don't need to walk ``__cause__``;
        # the ``or str(e)`` fallback covers raises that constructed
        # the error without explicit raw_message.
        code = getattr(e, "code", None)
        raw_msg = e.raw_message or str(e)
        raise OperationalError(str(e), code=code, raw_message=raw_msg) from e
    except _client_exc.ClusterPolicyError as e:
        # Deterministic: configured policy rejected the leader. The
        # client layer explicitly excludes this from retry (see
        # ``cluster.connect`` ``excluded_exceptions``). PEP 249's
        # ``InterfaceError`` ("driver interface cannot process this
        # operation") fits better than ``ProgrammingError`` here —
        # policy rejection is a driver-interface / configuration
        # mismatch, not caller-supplied SQL. The distinguishing prefix
        # ``"Cluster policy rejection;"`` lets callers branch on the
        # message without importing client-layer types, and the SA
        # dialect's ``is_disconnect`` narrows ``InterfaceError`` to
        # "connection is closed" / "cursor is closed" — so the pool
        # invalidates the permanent-reject slot without scheduling a
        # retry against the policy wall.
        # Apply the "Cluster policy rejection;" prefix only to the
        # user-facing ``message`` — ``raw_message`` is reserved for
        # the verbatim server text per the raw_message contract. The
        # prefix is the discriminator for callers branching without
        # importing client-layer types; it has no place on the
        # un-modified server-text accessor.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(
            cluster_policy_rejection_message(None, str(e)),
            code=None,
            raw_message=raw_msg,
        ) from e
    except _client_exc.ClusterError as e:
        # Non-policy ClusterError — transient, code=None.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(str(e), code=None, raw_message=raw_msg) from e
    except _client_exc.ProtocolError as e:
        # Wire decode / stream error — the socket is desynced, even if
        # TCP is alive. PEP 249 ``OperationalError`` ("problems with
        # the database's operation, e.g. connection lost") fits the
        # semantic better than ``InterfaceError`` ("driver misuse"),
        # and — paired with the ``"wire decode failed"`` substring in
        # the SA dialect's ``_dqlite_disconnect_messages`` list —
        # routes the failure through the disconnect-classifier's
        # substring branch so the pool slot invalidates on the first
        # round-trip.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(str(e), code=None, raw_message=raw_msg) from e
    except _client_exc.DataError as e:
        # client.DataError carries no server code today (encode-side
        # error surface), but plumb code=None explicitly so the
        # signature stays symmetric with the coded branches above.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise DataError(str(e), code=None, raw_message=raw_msg) from e
    except _WireEncodeError as e:
        # Wire-layer encode failure that escaped the client's
        # ``_run_protocol`` (e.g. a Message constructor raising
        # mid-validation before ``_run_protocol`` was reached).
        # Encode-side problems are caller-input issues — PEP 249 §7
        # ``DataError`` ("problems with the processed data: division
        # by zero, numeric value out of range, etc."). Without this
        # arm the wire EncodeError would propagate uncaught (it is
        # NOT a ``_client_exc.ProtocolError`` — the inheritance runs
        # the other direction). Pre-fix the bare wire exception
        # leaked past ``except dbapi.Error:`` blocks.
        raise DataError(f"wire encode failed: {e}", code=None, raw_message=str(e)) from e
    except _client_exc.InterfaceError as e:
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(str(e), code=None, raw_message=raw_msg) from e
    except _client_exc.DqliteError as e:
        # Catch-all for any future subclass of DqliteError not
        # enumerated above. PEP 249 §7: errors that occur during the
        # operation of the database are wrapped in DatabaseError or
        # its subclasses; an InterfaceError wrap would mis-classify
        # a server-sourced error as a driver-misuse error. Use
        # DatabaseError as the conservative wrap class so cross-
        # driver code using ``except DatabaseError:`` for server-side
        # failures catches future error classes correctly.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise DatabaseError(
            f"unrecognized client error ({type(e).__name__}): {e}",
            code=None,
            raw_message=raw_msg,
        ) from e
    # Note: the previous ``(TypeError, ValueError)`` blanket catch
    # was REMOVED. The wire encoder (``dqlitewire.types``) consistently
    # raises ``EncodeError`` (already caught above as
    # ``_WireEncodeError``) for bind-shape rejections; the bare
    # ``TypeError`` / ``ValueError`` catch over-attributed every
    # coro-internal fault (driver refactor bugs, third-party retry
    # middleware typos, ``asyncio.timeout(invalid)`` misuse) to the
    # caller's bind values and mis-classified driver-internal bugs
    # as ``DataError("caller-input fault ...")``. Operators reading
    # the diagnostic class and message would audit the bind shape
    # while the real root cause sat un-attributed on
    # ``__cause__``. The narrow path is: wire-encode rejection →
    # ``EncodeError`` → ``DataError`` (the arm above); coro-internal
    # ``TypeError`` / ``ValueError`` propagates raw so the
    # originating frame surfaces honestly.
    except BaseExceptionGroup as eg:
        # PEP 249 §7 mandates Error-class surface for every database-
        # related fault. ``BaseExceptionGroup`` does NOT inherit from
        # ``Exception`` (PEP 654 — it inherits from ``BaseException``
        # so ``except Exception:`` blocks correctly miss it), and no
        # client / wire exception class matches it. A group raised
        # from inside the awaited coro — by a future ``TaskGroup``
        # codec path, by third-party retry / telemetry middleware
        # wrapping the coro, or by an upstream `_bounded_group`
        # primary raise — would otherwise propagate as-is past every
        # ``except dbapi.Error:`` clause.
        #
        # PEP 654 + the asyncio cancellation invariant require that
        # ``CancelledError`` (and ``KeyboardInterrupt`` /
        # ``SystemExit``) propagate or be explicitly acknowledged —
        # they must NOT be silently converted into ordinary
        # ``Exception`` subclasses. Split the group: any
        # ``CancelledError`` / ``KeyboardInterrupt`` / ``SystemExit``
        # children re-raise as their own group so the caller's
        # structured-concurrency parent (asyncio.TaskGroup, anyio,
        # third-party telemetry middleware) sees the cancel signal.
        # Only the ``Exception``-subclass remainder is wrapped as
        # ``DatabaseError`` for the PEP 249 §7 contract.
        cancel_group, remainder = eg.split(
            lambda e: isinstance(e, (asyncio.CancelledError, KeyboardInterrupt, SystemExit))
        )
        if cancel_group is not None:
            # ``raise ... from None`` because the cancel-class
            # children are not "errors during exception handling" —
            # they are the original signal we are forwarding to the
            # caller's structured-concurrency parent.
            raise cancel_group from None
        # The split contract guarantees ``remainder is not None`` when
        # ``cancel_group is None``: the group must have had at least
        # one child for the arm to trigger, and that child wasn't in
        # the cancel partition. Defensive narrowing via ``if`` instead
        # of ``assert`` so the subsequent ``remainder.exceptions``
        # access doesn't surface ``AttributeError`` under ``python -O``
        # (which strips ``assert``). The branch is logically
        # unreachable under the BaseExceptionGroup contract; raising
        # the original ``eg`` is the safe fallback.
        if remainder is None:
            raise eg
        # Wrap as ``DatabaseError`` (the most generic Error subclass
        # for "errors during database operation"; see PEP 249 §6.5 +
        # §7) preserving the remainder on ``__cause__`` so SA's
        # ``_walk_cause_chain`` can still descend the children and
        # ``is_disconnect`` can classify the leaf exceptions. The
        # message names the group size and the children-type
        # cardinality so an operator can triage without walking the
        # chain manually.
        child_classes = {type(c).__name__ for c in remainder.exceptions}
        raise DatabaseError(
            f"aggregate {type(remainder).__name__} with {len(remainder.exceptions)} child(ren) "
            f"of class(es) {sorted(child_classes)}",
            code=None,
            raw_message=str(remainder),
        ) from remainder


if TYPE_CHECKING:
    from dqlitedbapi.connection import Connection


# Per-wire-type result converters. NULL/empty values pass through as None
# (guarded at the call site); unrecognized types pass through unchanged
# because the wire codec already produced an appropriate Python primitive.
#
# No ``isinstance`` guard inside the lambdas: the wire layer is
# authoritative — if the per-row type says ISO8601, the value IS a str;
# if it says UNIXTIME, the value IS an int. A mismatch indicates a
# malformed frame, which ``_datetime_from_iso8601`` / ``_datetime_from_unixtime``
# surface as ``DataError``.
_RESULT_CONVERTERS: Final[dict[int, Callable[[Any], Any]]] = {
    int(ValueType.ISO8601): _datetime_from_iso8601,
    int(ValueType.UNIXTIME): _datetime_from_unixtime,
}


def _convert_row(row: Sequence[Any], row_types: Sequence[int]) -> tuple[Any, ...]:
    """Apply result-side converters to a row using its per-row wire types.

    ``row_types`` must be the types the wire protocol attached to *this
    specific row*, not ``column_types`` (which only reflects row 0).
    SQLite is dynamically typed; different rows in the same column
    can carry different wire ``ValueType`` tags under UNION,
    ``CASE``, ``COALESCE``, and ``typeof()``. Using per-row types
    preserves round-trip fidelity for heterogeneous result sets.
    """
    result = list(row)
    for i, tcode in enumerate(row_types):
        converter = _RESULT_CONVERTERS.get(tcode)
        if converter is not None and result[i] is not None:
            result[i] = converter(result[i])
    return tuple(result)


def _convert_rows(
    rows: Sequence[Sequence[Any]],
    row_types: Sequence[Sequence[int]],
    column_types: Sequence[int],
) -> list[tuple[Any, ...]]:
    """Build the cursor's ``_rows`` list from a fetched result set.

    Detects up-front whether any column actually carries a registered
    converter type (``_RESULT_CONVERTERS`` only holds two entries:
    ``ISO8601`` and ``UNIXTIME``). For the common case — INTEGER /
    REAL / TEXT / BLOB / NULL columns only — the per-cell converter
    walk in :func:`_convert_row` would rebuild each row tuple without
    producing any value change, so we skip it and materialise rows
    directly as tuples. This shaves O(n_cells) of pure-Python work
    from every converter-free fetch, which on the async surface runs
    on the event-loop thread after ``await client.query_sql(...)``
    returns.

    The probe inspects both ``column_types`` and every per-row type
    list: SQLite's dynamic typing allows a column to carry different
    wire ``ValueType`` tags across rows, so a row whose type diverges
    from row 0 may still need conversion even when ``column_types``
    looks converter-free.
    """
    if not rows:
        return []
    needs_conversion = any(t in _RESULT_CONVERTERS for t in column_types) or any(
        t in _RESULT_CONVERTERS for rt in row_types for t in rt
    )
    if not needs_conversion:
        return [tuple(row) for row in rows]
    return [
        _convert_row(row, row_types[i] if i < len(row_types) else column_types)
        for i, row in enumerate(rows)
    ]


def _reject_non_sequence_params(params: Any) -> None:
    """Reject mappings, unordered containers, and str/bytes per PEP 249 qmark rules.

    PEP 249: for ``qmark`` paramstyle "the sequence is mandatory and the
    driver will not accept mappings." We also reject ``set`` / ``frozenset``
    — they are sequences structurally but unordered, which silently
    scrambles positional bindings. And we reject ``str`` /
    ``bytes`` / ``bytearray`` / ``memoryview`` — they are iterable, so
    they would silently "explode" into character/byte binds and the
    caller almost always meant ``(value,)`` instead.
    """
    if params is None:
        return
    if isinstance(params, (str, bytes, bytearray, memoryview)):
        raise ProgrammingError(
            f"parameters must be a sequence of values, not "
            f"{type(params).__name__!r}; did you mean to pass a tuple "
            f"like (value,) with a single element?"
        )
    if isinstance(params, Mapping):
        raise ProgrammingError(
            "qmark paramstyle requires a sequence; got a mapping. "
            "Use a list or tuple positionally matching the ? placeholders."
        )
    if isinstance(params, (set, frozenset)):
        raise ProgrammingError(
            "qmark paramstyle requires an ordered sequence; got a set. "
            "Use a list or tuple positionally matching the ? placeholders."
        )
    # Reject generators / iterators / any non-Sized iterable. Stdlib
    # ``sqlite3`` raises ``ProgrammingError("parameters are of
    # unsupported type")`` for these; cross-driver portability argues
    # for matching the behaviour. The substantive correctness reason
    # is the leader-flip retry path: ``connect()``'s retry-with-backoff
    # may need to re-issue the same statement after a leader flip, but
    # a single-pass iterable can be drained only once. The current
    # ``_convert_params`` materialise-into-list side effect masks this
    # for the happy path, but the rejection at the boundary makes the
    # constraint visible to callers (with an actionable diagnostic)
    # rather than buried inside the wire-layer parameter-count check.
    if not isinstance(params, Sized):
        raise ProgrammingError(
            f"qmark paramstyle requires a sized sequence (tuple/list); got "
            f"{type(params).__name__!r}. Generators and other non-sized "
            f"iterables are rejected because the driver may need to "
            f"re-execute on leader-flip retry, which a single-pass "
            f"iterable cannot satisfy. Materialise into a tuple/list "
            f"explicitly."
        )


# Outer shapes that ``executemany`` must reject before iteration begins.
# ``str`` / ``bytes`` / ``bytearray`` / ``memoryview`` would silently
# iterate over characters / bytes (each yielded element becoming a
# "parameter set" — almost always a caller bug). ``set`` / ``frozenset``
# iterate in non-deterministic order, scrambling row order. ``dict`` is
# handled separately below via an exact-type check so dict SUBCLASSES
# (``OrderedDict``, ``defaultdict``, ``Counter``, ...) remain accepted
# as ordered iterables of parameter sets — the common
# ``OrderedDict([(0, params0), (1, params1)])``-of-rows pattern works
# unchanged. Only a literal ``dict`` (the common single-row misuse) is
# denied.
_REJECTED_EXECUTEMANY_SEQ_TYPES: Final[tuple[type, ...]] = (
    str,
    bytes,
    bytearray,
    memoryview,
    set,
    frozenset,
)


def _validate_executemany_seq_shape(seq_of_parameters: object) -> None:
    """Reject outer shapes that ``executemany`` would silently iterate.

    Called from both ``Cursor.executemany`` / ``AsyncCursor.executemany``
    and the ``Connection.executemany`` / ``AsyncConnection.executemany``
    shortcuts so the four entry points share one diagnostic and one
    accept/reject contract. Without the cursor-side check, a caller
    passing ``"abc"`` mutates cursor state per-character before the
    inner ``_reject_non_sequence_params`` rejects the character; the
    connection-shortcut layer already rejected upfront, leaving the
    cursor path as the only asymmetric arm.
    """
    if isinstance(seq_of_parameters, _REJECTED_EXECUTEMANY_SEQ_TYPES):
        raise ProgrammingError(
            f"executemany seq_of_parameters must be an iterable of "
            f"parameter sets (e.g. list of tuples), not "
            f"{type(seq_of_parameters).__name__}. Iterating a "
            f"{type(seq_of_parameters).__name__} parameter-set is "
            f"almost certainly a bug."
        )
    # Exact-type check for ``dict`` only — subclasses (OrderedDict,
    # defaultdict, Counter, ChainMap-via-dict-adapter) iterate keys in
    # insertion order and are legitimate ordered iterables of parameter
    # sets when the caller deliberately uses an indexed mapping for
    # batch ordering.
    if type(seq_of_parameters) is dict:
        raise ProgrammingError(
            "executemany seq_of_parameters must be an iterable of "
            "parameter sets (e.g. list of tuples), not dict. Iterating "
            "a dict parameter-set is almost certainly a bug; wrap as "
            "[params] for a single-row batch."
        )


def _convert_params(params: Sequence[Any] | None) -> list[Any] | None:
    """Convert driver-level bind parameters (e.g. datetime) to wire primitives.

    A user-registered adapter (via ``register_adapter``) may raise
    arbitrary exceptions on a malformed input. PEP 249 §7 mandates
    every database-related failure surfaces as an ``Error`` subclass
    so cross-driver code can write ``except dbapi.Error:`` blocks.
    Wrap any non-``Error`` exception escaping ``_convert_bind_param``
    as ``DataError`` (the right PEP 249 class for "problems with the
    processed data"). Adapters that already raise an ``Error``
    subclass directly (e.g. ``DataError`` for an invalid binding
    shape) pass through unchanged — no double-wrap, the cause-chain
    stays clean.

    Mirrors the wire-encode wrap discipline at ``_call_client``'s
    ``except (TypeError, ValueError)`` arm.
    """
    _reject_non_sequence_params(params)
    if params is None:
        return None
    converted: list[Any] = []
    for p in params:
        try:
            converted.append(_convert_bind_param(p))
        except Error:
            # Already a PEP 249 Error subclass — propagate unchanged
            # (no double-wrap).
            raise
        except Exception as e:
            # Stdlib parity: a user-defined ``__conform__`` raising
            # an exception propagates UNWRAPPED. ``_convert_bind_param``
            # tags such exceptions with ``_dqlite_conform_propagate``
            # so this arm can re-raise the original without wrapping.
            # Cross-driver code using ``except dbapi.Error:`` must
            # NOT silently swallow programmer bugs in user-defined
            # ``__conform__`` implementations — see CPython
            # ``Modules/_sqlite/microprotocols.c
            # ::_pysqlite_microprotocols_adapt`` for the reference
            # behaviour cited in ``_convert_bind_param``'s docstring.
            # ``register_adapter`` callback raises (without the
            # marker) still wrap as ``DataError`` per the documented
            # adapter-misuse contract.
            if getattr(e, "_dqlite_conform_propagate", False):
                raise
            raise DataError(
                f"adapter for {type(p).__name__} failed: {e}",
                code=None,
            ) from e
    return converted


def _strip_leading_comments(sql: str) -> str:
    """Strip leading SQL comments (-- and /* */) and whitespace.

    Also strips a leading UTF-8 BOM (``\\ufeff``). SQLite's
    ``prepare.c::sqlite3_prepare_v2`` skips a leading BOM before
    tokenisation; Python's ``str.strip()`` does NOT consider
    ``\\ufeff`` whitespace, so a SQL file imported via
    ``encoding='utf-8'`` (instead of ``utf-8-sig``) or written with
    PowerShell ``Set-Content`` / Notepad would otherwise trip the
    classifier prefix-checks.

    NOTE: this helper is also used by ``_is_multi_statement``'s tail
    walk to detect content past a ``;`` boundary. It must NOT consume
    bare ``;`` separators — the multi-statement walker's
    security-posture invariant requires that a trailing ``;`` after a
    real statement IS detected as multi-statement (the
    ``_is_multi_statement`` walker tail-checks each post-``;`` segment
    for non-empty content). The empty-statement classifier branch in
    ``_classify_caller_sql`` uses the sibling
    ``_strip_leading_comments_and_separators`` helper which DOES
    consume ``;`` separators — that helper applies only when the
    walker has already determined the input has at most one
    statement.
    """
    s = sql.lstrip("﻿").strip()
    while True:
        if s.startswith("--"):
            newline = s.find("\n")
            if newline == -1:
                return ""
            s = s[newline + 1 :].strip()
        elif s.startswith("/*"):
            end = s.find("*/")
            if end == -1:
                # Symmetric with the unterminated-``--`` branch above:
                # collapse to "" so downstream "no verb" handling kicks
                # in. Mirrors the client-layer copy.
                return ""
            s = s[end + 2 :].strip()
        else:
            break
    return s


def _strip_leading_comments_and_separators(sql: str) -> str:
    """Like :func:`_strip_leading_comments` but additionally consumes
    bare ``;`` separators.

    Used ONLY by the empty-statement classification in
    :func:`_classify_caller_sql`. Do NOT use in
    :func:`_is_multi_statement`'s tail walk — the multi-statement
    walker's security-posture invariant relies on detecting a trailing
    ``;`` after a real statement as multi-statement.

    A SQL input of just ``";"``, ``"; ; ;"``, ``"/* */;-- foo\\n;"``
    etc. classifies as **empty** (no real content). Stdlib ``sqlite3``
    silently no-ops on these; dqlite raises
    ``ProgrammingError("empty statement")`` per the project's
    documented divergence (caller-side bug → caller-side error class).
    """
    s = sql.lstrip("﻿").strip()
    while True:
        if s.startswith("--"):
            newline = s.find("\n")
            if newline == -1:
                return ""
            s = s[newline + 1 :].strip()
        elif s.startswith("/*"):
            end = s.find("*/")
            if end == -1:
                return ""
            s = s[end + 2 :].strip()
        elif s.startswith(";"):
            s = s[1:].strip()
        else:
            break
    return s


_ROW_RETURNING_PREFIXES: Final[tuple[str, ...]] = ("SELECT", "VALUES", "PRAGMA", "EXPLAIN", "WITH")

# Word-boundary regex for the RETURNING-keyword scan. The previous
# literal-space substring scan (`` RETURNING `` / `` RETURNING`` at
# end) missed every multi-line SQL formatting style produced by
# sqlfluff, pgFormatter, and hand-written queries that place
# RETURNING on its own line. Result: ``INSERT INTO t VALUES (?)\n
# RETURNING id`` silently classified as exec-only; rows discarded;
# cursor.description None; cursor.fetchall() []. Data-loss-grade.
# The regex matches RETURNING as a whole word after the noise-
# stripped, comment-stripped text has been uppercased; the noise
# stripper at ``_strip_sql_noise`` neutralises identifiers and
# string literals so a column named RETURNING (quoted) is safe.
_RETURNING_WORD_RE: Final[re.Pattern[str]] = re.compile(r"\bRETURNING\b")

# Verbs that take no parameters and cannot legitimately drive an
# ``executemany`` call. Stdlib ``sqlite3.Cursor.executemany`` rejects
# the same shapes via its statement-type check; admitting them here
# silently re-runs the bare statement N times against ignored bind
# params, producing duplicate server-side savepoint frames (and other
# state divergence) that compound with the duplicate-name LIFO rule.
_EXECUTEMANY_REJECT_VERBS: Final[frozenset[str]] = frozenset(
    {"SAVEPOINT", "RELEASE", "ROLLBACK", "BEGIN", "COMMIT", "END"}
)

# SQL noise that the RETURNING-keyword scan must skip past: single-quoted
# string literals (with '' escapes), double-quoted identifiers (with ""
# escapes), bracket-quoted identifiers (MSSQL / Access style, which SQLite
# also accepts), ``-- line comments``, and ``/* block comments */``.
# Without this stripper a value like ``INSERT INTO t VALUES('some
# RETURNING thing')`` or an identifier like ``SET "returning" = 1`` got
# misclassified as row-returning, the statement was dispatched through
# QUERY_SQL, and ``_rowcount`` / ``_lastrowid`` reported zero / None.
_SQL_NOISE_RE: Final[re.Pattern[str]] = re.compile(
    r"""
    '(?:[^']|'')*'          # single-quoted string literal
    | "(?:[^"]|"")*"        # double-quoted identifier
    | \[[^\]]*\]            # bracket-quoted identifier
    | `(?:[^`]|``)*`        # backtick-quoted identifier (MySQL-compat;
                            # SQLite accepts these and uses doubled-`
                            # escaping inside, parity with the
                            # double-quoted identifier branch)
    | --[^\n]*              # line comment
    | /\*.*?\*/             # block comment
    """,
    re.VERBOSE | re.DOTALL,
)


def _strip_sql_noise(sql: str) -> str:
    """Replace string literals / identifiers / comments with a space.

    Preserves keyword boundaries so the downstream ``\\bRETURNING\\b``
    regex scan still sees a well-bounded match in the cleaned text.
    The space substitution is important: collapsing to empty would
    fuse adjacent tokens into identifiers that could themselves
    trigger false positives.
    """
    return _SQL_NOISE_RE.sub(" ", sql)


def _is_multi_statement(sql: str) -> bool:
    """Return ``True`` if ``sql`` contains more than one statement.

    The dqlite server's prepare path returns a single statement
    (the wire protocol's ``PrepareRequest`` ignores trailing text
    after the first ``;``). A user passing
    ``"INSERT ...; INSERT ..."`` would otherwise get only the first
    INSERT executed with no diagnostic.

    A trailing ``;`` is fine; whitespace and comments after the final
    ``;`` are fine. Any non-whitespace, non-comment content past a
    ``;`` is rejected as a multi-statement.

    String literals and comments are neutralised first (via
    ``_strip_sql_noise``) so a ``;`` inside a string or comment is
    not detected as a statement boundary.
    """
    cleaned = _strip_sql_noise(sql)
    semicolon = cleaned.find(";")
    while semicolon != -1:
        # The character past the ``;`` and everything after must
        # reduce to whitespace / comments / nothing.
        tail = _strip_leading_comments(cleaned[semicolon + 1 :])
        if tail:
            return True
        next_idx = cleaned.find(";", semicolon + 1)
        if next_idx == -1:
            return False
        semicolon = next_idx
    return False


def _validate_caller_param_shape(parameters: Sequence[Any] | None) -> None:
    """Reject structural outer-shape mistakes in caller-supplied
    parameters (str / bytes / bytearray / memoryview / Mapping for
    qmark / set / frozenset).

    Hoisted out of :func:`_classify_caller_sql` so callers can invoke
    it BEFORE the ``_reset_execute_state`` scrub. The shape is a
    caller-shape misuse (no SQL has been prepared yet); per
    ``Cursor.execute``'s documented contract the reset must NOT
    fire for these — the cursor's prior result-set state stays
    intact so a caller's retry-with-coerce idiom can inspect
    ``cur.description`` to shape the retry.

    Unsized iterables (generators / iterators) deliberately skip the
    Mapping / set / frozenset checks via type-tagging — the
    binding-layer's ``_reject_non_sequence_params`` handles those.
    """
    if parameters is None:
        return
    if isinstance(parameters, (str, bytes, bytearray, memoryview)):
        raise ProgrammingError(
            f"parameters must be a sequence of values, not "
            f"{type(parameters).__name__!r}; did you mean to pass a tuple "
            f"like (value,) with a single element?"
        )
    if isinstance(parameters, Mapping):
        raise ProgrammingError(
            "qmark paramstyle requires a sequence; got a mapping. "
            "Use a list or tuple positionally matching the ? placeholders."
        )
    if isinstance(parameters, (set, frozenset)):
        raise ProgrammingError(
            "qmark paramstyle requires an ordered sequence; got a set. "
            "Use a list or tuple positionally matching the ? placeholders."
        )


def _classify_caller_sql(
    operation: str,
    parameters: Sequence[Any] | None,
    *,
    skip_param_count_check: bool = False,
) -> None:
    """Pre-flight classification of caller-supplied SQL.

    Raises ``ProgrammingError`` for three caller-side mistakes that
    would otherwise produce either silent data loss or a misleading
    error class from the server:

    - **Empty / whitespace / comment-only SQL**. Server emits
      ``failure(code=0, "empty statement")``, classified as
      ``OperationalError`` by the wire-mapping table. PEP 249 §7
      requires caller-side errors to be ``ProgrammingError``.
    - **Multi-statement SQL**. Server's prepare path returns only
      the first statement; without this guard, ``"INSERT ...;
      INSERT ..."`` silently drops everything past the first ``;``
      with no diagnostic. Callers with multi-statement intent must
      split client-side and call ``execute`` per statement —
      ``executescript`` is a stub that raises ``NotSupportedError``
      (see the README §"Limitations vs. stdlib `sqlite3`").
    - **Wrong ``?`` count vs ``len(parameters)``**. Server rejects
      with ``SQLITE_RANGE (25)`` after a wire round-trip; pre-
      flighting saves the RTT and matches stdlib's
      ``ProgrammingError("Incorrect number of bindings supplied.
      The current statement uses N, and there are M supplied.")``.

    Each detector neutralises string literals, identifiers, and
    comments via ``_strip_sql_noise`` first so a ``;`` / ``?``
    inside a quoted token / comment is not treated as syntactically
    significant.
    """
    # NUL byte in SQL: stdlib raises ``ProgrammingError("the query
    # contains a null character")`` pre-flight. The wire encoder
    # would also reject (with ``EncodeError`` → ``DataError``) but
    # ``DataError`` is the wrong PEP 249 class for caller-supplied
    # malformed SQL — the same bucket that catches the multi-statement
    # and "empty statement" rejections. Pre-flight here so the right
    # class surfaces without a wire RTT.
    if "\x00" in operation:
        raise ProgrammingError("the query contains a null character")
    # Empty-SQL: ``_strip_leading_comments_and_separators`` returns
    # "" if the SQL is blank, whitespace, comment-only, or consists
    # solely of ``;`` separators (e.g. ``";"``, ``"; ; ;"``). Stdlib
    # silently no-ops on the bare-semicolon shapes; we route them to
    # the same caller-side ``ProgrammingError`` as empty / comment-
    # only SQL because they are equivalent (zero non-empty
    # statements). Stdlib raises ``ProgrammingError`` for ``""``;
    # match.
    if not _strip_leading_comments_and_separators(operation):
        raise ProgrammingError("empty statement")
    # Multi-statement: stdlib raises with this exact wording.
    if _is_multi_statement(operation):
        raise ProgrammingError("You can only execute one statement at a time.")
    # The placeholder/len check is skipped for the ``executemany``
    # one-time pre-flight (which deliberately passes
    # ``skip_param_count_check=True`` because the per-iteration check
    # runs in the executemany loop body). For the ``execute`` path,
    # ``parameters=None`` (caller omitted the second arg) is treated
    # as ``param_count=0`` so ``cur.execute("SELECT ?")`` pre-flights
    # with the same ``ProgrammingError`` stdlib raises (rather than a
    # wire RTT producing ``InterfaceError`` via ``SQLITE_RANGE``).
    if skip_param_count_check:
        return
    if parameters is not None:
        # Structural outer-shape rejection has already happened at the
        # caller (execute / executemany) BEFORE _reset_execute_state,
        # to preserve the cursor's prior result-set state on
        # caller-shape misuse. Re-invoke the helper here so callers
        # going through the executemany loop's per-iteration path get
        # the same diagnostic without bypassing the structural gate.
        _validate_caller_param_shape(parameters)
        try:
            param_count = len(parameters)
        except TypeError:
            return  # caller will trip the binding-layer rejection
    else:
        param_count = 0
    # Count ``?`` placeholders in the noise-stripped SQL.
    cleaned = _strip_sql_noise(operation)
    placeholder_count = cleaned.count("?")
    if placeholder_count != param_count:
        raise ProgrammingError(
            f"Incorrect number of bindings supplied. The current "
            f"statement uses {placeholder_count}, and there are "
            f"{param_count} supplied."
        )


class _ExecuteManyCursor(Protocol):
    """Structural shape of :class:`Cursor` / :class:`AsyncCursor` as
    consumed by :class:`_ExecuteManyAccumulator`.

    The two cursor classes are not related by inheritance (separate
    sync / async trees) and a true Union would require a circular
    import. PEP 544 structural typing captures the four attributes the
    accumulator actually reads + writes, so mypy catches typos that
    were previously hidden under ``cursor: Any``.
    """

    _rowcount: int
    _description: _Description
    _rows: list[tuple[Any, ...]]
    _row_index: int
    _closed: bool


class _ExecuteManyAccumulator:
    """Shared state for the RETURNING-aware ``executemany`` loop.

    Both the sync and async cursor implementations iterate
    ``seq_of_parameters`` calling their respective single-statement
    helper. For statements with a RETURNING clause, rows produced on
    each iteration must accumulate so a subsequent ``fetchall`` yields
    every returned row across parameter sets. The bodies differ only
    by the ``await`` on the inner call, so both flavours drive this
    accumulator and then apply it to the cursor.
    """

    __slots__ = ("_max_rows", "_pushed", "description", "rows", "total_affected")

    def __init__(self, max_rows: int | None = None) -> None:
        self.total_affected = 0
        self.rows: list[tuple[Any, ...]] = []
        self.description: _Description = None
        # Count of ``push()`` calls: ``apply()`` uses this to distinguish
        # "zero iterations ran" (empty seq_of_parameters) from "every
        # iteration was a no-op DML" so the resulting ``rowcount`` stays
        # at the post-``_reset_execute_state`` baseline of -1 in the
        # former case, matching empty-``execute`` shape.
        self._pushed = 0
        # Cumulative row cap across all executemany iterations. The
        # wire-layer ``max_total_rows`` governor caps a single round-
        # trip only; without this cumulative check, a 10M-element
        # parameter sequence for ``INSERT ... RETURNING`` accumulates
        # unbounded in memory.
        self._max_rows = max_rows

    def push(self, cursor: _ExecuteManyCursor) -> None:
        """Record one iteration's output into the accumulator.

        Branches on the row-returning signal (``_description is not
        None``) so "rows returned" and "rows affected" stay decoupled
        from the shared ``_rowcount`` overload. Today's semantics have
        the execute path set ``_rowcount = len(rows)`` on the
        RETURNING branch — summing ``_rowcount`` and summing
        ``len(rows)`` happen to produce identical results, but a future
        change that makes ``_rowcount`` actually mean "rows affected"
        (distinct from ``len(rows)`` for e.g. ``INSERT ... ON
        CONFLICT ... RETURNING`` where rowcount may include skipped
        rows) would silently double-count without this split.
        """
        # Snapshot the cursor fields BEFORE reading ``_closed``. Under
        # tier-2 (``check_same_thread=False``), a sibling thread's
        # ``_cascade_cursors`` may zero ``_rows`` / ``_description`` /
        # ``_rowcount`` / ``_lastrowid`` between any two attribute
        # reads on this cursor. Snapshot first, then check ``_closed``
        # — if it's True, cascade ran during snapshot and the captured
        # locals may be incoherent (some pre-cascade, some post-);
        # skip the push so the accumulator does not capture mixed
        # state. The next iteration's ``_check_closed`` in
        # ``executemany`` will raise cleanly and the BaseException arm
        # restores pre-batch state. Mirror of the defensive snapshot
        # discipline in ``Cursor._next_row_unlocked``.
        description = cursor._description
        rows = cursor._rows
        rowcount = cursor._rowcount
        if getattr(cursor, "_closed", False):
            return
        self._pushed += 1
        if description is not None:
            # Row-returning iteration: total_affected is the count of
            # rows emitted, which today matches ``len(cursor._rows)``.
            # Using ``len()`` makes the invariant explicit and survives
            # any future decoupling of rowcount semantics on the
            # RETURNING path.
            if self.description is None:
                self.description = description
            self.rows.extend(rows)
            self.total_affected += len(rows)
            if self._max_rows is not None and len(self.rows) > self._max_rows:
                raise DataError(
                    f"executemany accumulated {len(self.rows)} RETURNING rows; "
                    f"exceeds max_total_rows={self._max_rows}"
                )
        elif rowcount >= 0:
            # Plain DML iteration: ``_rowcount`` is the server's
            # sqlite3_changes() for this parameter set.
            self.total_affected += rowcount

    def apply(self, cursor: _ExecuteManyCursor) -> None:
        """Materialise the accumulator's state onto the cursor.

        ``description is None`` means none of the iterations produced a
        result set (plain DML without RETURNING); leave ``_description``
        / ``_rows`` as reset. Inherit the first-seen description
        otherwise.

        No-op if the cursor has been closed concurrently — the async
        ``close()`` contract scrubs ``_rows``/``_description``/
        ``_rowcount``, and re-populating those fields here would
        visibly un-close the result set for any attribute-level
        caller. Sync flavour is immune via the outer threading lock;
        this guard pins the async flavour. The per-iteration guard in
        async ``executemany`` catches the racy close between iterations;
        this guard catches a close that lands after the loop exits
        but before ``apply()`` writes state back.
        """
        if cursor._closed:
            return
        if self._pushed == 0:
            # Empty ``seq_of_parameters``: zero iterations → zero rows
            # affected. stdlib ``sqlite3.Cursor.executemany([])`` and
            # psycopg2 both set ``rowcount = 0`` (not ``-1``) in this
            # case. PEP 249 permits ``-1`` as "undetermined," but zero
            # iterations has a deterministic zero answer — match the
            # stdlib / psycopg2 contract so callers doing
            # ``if cur.rowcount > 0: ...`` after an empty batch see
            # the expected no-rows-affected signal.
            cursor._rowcount = 0
            return
        cursor._rowcount = self.total_affected
        if self.description is not None:
            cursor._description = self.description
            cursor._rows = self.rows
            cursor._row_index = 0


def _is_row_returning(sql: str) -> bool:
    """Heuristic for "does this statement return a result set?"

    Single source of truth for sync and async cursors.
    Matches leading SELECT/VALUES/PRAGMA/EXPLAIN/WITH after stripping
    comments and a single leading ``(``, and catches trailing or
    embedded RETURNING clauses on DML.

    ``VALUES (...)`` and ``(SELECT ...)`` are valid top-level
    row-returning SQLite statements, so they take the query branch.

    Note: ``WITH ... INSERT/UPDATE/DELETE`` (no RETURNING) will be
    misclassified as a query. This is a known limitation of a
    prefix-only check — a full SQL parser is out of scope.
    """
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    if normalized.startswith(_ROW_RETURNING_PREFIXES):
        return True
    # Word-boundary regex: matches RETURNING preceded or followed by
    # any whitespace (space, newline, tab, CR) or punctuation, while
    # NOT matching when RETURNING is part of a larger identifier
    # like ``RETURNING_id``. The noise stripper has already
    # neutralised identifiers and string literals.
    return _RETURNING_WORD_RE.search(normalized) is not None


def _strip_leading_with_clause(normalized: str) -> str:
    """Strip a leading ``WITH cte AS (...)`` clause (and any recursive
    or comma-chained variants) from ``normalized`` so the remainder
    starts with the actual top-level keyword.

    ``normalized`` must already have been processed by
    ``_strip_sql_noise`` (string literals neutralised) and
    ``_strip_leading_comments`` and uppercased so the paren counter
    is reliable and the keywords match without case folding.

    Returns the substring beginning at the first non-CTE keyword, or
    the original string if the leading ``WITH`` cannot be parsed
    (defensive — fall back to the prefix check, do not raise).
    """
    if not normalized.startswith("WITH"):
        return normalized
    # Skip "WITH" and an optional "RECURSIVE".
    pos = len("WITH")
    while pos < len(normalized) and normalized[pos].isspace():
        pos += 1
    if normalized[pos:].startswith("RECURSIVE"):
        pos += len("RECURSIVE")
        while pos < len(normalized) and normalized[pos].isspace():
            pos += 1
    # Each CTE is ``name AS (body)`` or ``name (cols) AS (body)``.
    # Find the ``AS (`` that opens the body — there may be a
    # parenthesised column list before it. Balance parens so we don't
    # mistake the column-list closing ``)`` for the body's.
    while True:
        # Skip the CTE name (and any optional MATERIALIZED keyword
        # between the name and AS, plus a parenthesised column list).
        as_idx = normalized.find(" AS ", pos)
        as_idx_paren = normalized.find(" AS(", pos)
        if as_idx == -1 or (as_idx_paren != -1 and as_idx_paren < as_idx):
            as_idx = as_idx_paren
        if as_idx == -1:
            return normalized  # malformed; fall back
        # Position the body-paren scanner just after AS.
        body_paren = normalized.find("(", as_idx + 3)
        if body_paren == -1:
            return normalized
        depth = 1
        i = body_paren + 1
        while i < len(normalized) and depth > 0:
            if normalized[i] == "(":
                depth += 1
            elif normalized[i] == ")":
                depth -= 1
            i += 1
        if depth != 0:
            return normalized  # unbalanced; fall back
        # ``i`` is just past the closing ``)`` of the body. The CTE
        # may be followed by ``, name AS (...)`` or by the top-level
        # statement.
        pos = i
        while pos < len(normalized) and normalized[pos].isspace():
            pos += 1
        if pos < len(normalized) and normalized[pos] == ",":
            pos += 1
            while pos < len(normalized) and normalized[pos].isspace():
                pos += 1
            continue
        return normalized[pos:]


def _is_dml_with_returning(sql: str) -> bool:
    """True if ``sql`` is admissible to ``executemany``: DML, possibly
    behind a CTE prefix, possibly with a RETURNING clause.

    Used by ``executemany`` to admit:

    * pure DML behind a CTE prefix (``WITH cte AS (...) INSERT ...``),
      where ``_is_row_returning`` would otherwise reject the statement
      because of the leading ``WITH`` keyword; and
    * DML with RETURNING (``INSERT ... RETURNING``), the legitimate
      row-returning DML case.

    Pure SELECT / VALUES / PRAGMA / EXPLAIN — including their
    CTE-prefixed forms — remain rejected: stdlib
    ``sqlite3.Cursor.executemany`` rejects any non-DML statement.

    The historical name (``_is_dml_with_returning``) is preserved for
    grep-friendliness; "with returning" reads as a feature flag,
    "with a leading CTE" as a structural prefix, and the function
    accepts both.
    """
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    body = _strip_leading_with_clause(normalized)
    return body.startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))


_INT64_OVERFLOW_THRESHOLD: Final[int] = 1 << 63
_UINT64_RANGE: Final[int] = 1 << 64


def _to_signed_int64(value: int) -> int:
    """Re-cast a wire ``uint64`` value to signed ``int64``.

    The dqlite wire codec exposes ``ResultResponse.last_insert_id``
    and ``rows_affected`` as unsigned 64-bit integers; the C server
    casts SQLite's signed ``sqlite3_int64`` through ``(uint64_t)``
    before sending. Negative SQLite rowids (legal on
    ``INTEGER PRIMARY KEY`` tables) and the rare negative
    ``rows_affected`` therefore arrive as values above ``2**63``.

    stdlib ``sqlite3.Cursor.lastrowid`` and SA's ``Integer`` type
    both expect signed ``int64``; the Go reference connector mirrors
    that contract via ``int64(r.result.LastInsertID)``. Without this
    cast a negative rowid surfaces as a 19-digit positive integer
    that breaks downstream ``WHERE rowid = ?`` lookups silently.
    """
    if value >= _INT64_OVERFLOW_THRESHOLD:
        return value - _UINT64_RANGE
    return value


def _is_insert_or_replace(sql: str) -> bool:
    """True if ``sql`` is an INSERT (including ``INSERT OR REPLACE`` /
    ``INSERT OR IGNORE``) or a bare REPLACE statement.

    stdlib ``sqlite3.Cursor.lastrowid`` is documented to update only
    on successful INSERT / REPLACE; UPDATE / DELETE / DDL leave the
    previous INSERT's rowid in place. The dqlite wire layer returns
    ``last_insert_id`` on every Exec response — typically 0 for
    non-INSERT paths — so unconditionally writing it into
    ``_lastrowid`` would zero out the sticky value that callers rely
    on. Gate the write on this prefix check to match stdlib.
    """
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    return normalized.startswith(("INSERT", "REPLACE"))


def _is_dml_rowcount_meaningful(sql: str) -> bool:
    """True if ``sql`` is a DML statement for which SQLite's
    ``sqlite3_changes()`` returns a meaningful count: INSERT / UPDATE
    / DELETE / REPLACE.

    Stdlib ``sqlite3`` returns ``-1`` from ``Cursor.rowcount`` for
    every other verb (DDL: CREATE / DROP / ALTER / VACUUM / REINDEX /
    ANALYZE / ATTACH / DETACH; transactional: SAVEPOINT / RELEASE /
    ROLLBACK TO; etc.) — "not determinable" per PEP 249 §6.1.1.

    The dqlite wire returns ``rows_affected = 0`` for those verbs
    (SQLite's ``sqlite3_changes()`` returns 0 for non-DML). Writing
    that 0 into ``_rowcount`` makes ``cur.rowcount == 0`` deterministic
    AND True after a DDL — but stdlib makes it ``-1`` (undetermined,
    False). Cross-driver migration tooling that branches on
    ``if cur.rowcount == 0:`` then takes a different path.

    Gate the ``_rowcount`` write on this predicate; default to ``-1``
    otherwise.
    """
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    return normalized.startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))


class Cursor:
    """PEP 249 compliant database cursor."""

    # Stable attribute set — allocated one per ``Connection.cursor()``
    # call, so dropping the per-instance ``__dict__`` is a measurable
    # win at SA-engine scale. Mirrors ``_ExecuteManyAccumulator``'s
    # existing slots pattern. Subclasses without their own
    # ``__slots__`` retain a ``__dict__`` (stdlib ``datetime`` pattern).
    # ``__weakref__`` is needed so ``Connection._cursors`` (a WeakSet)
    # can hold a reference to the cursor; slotted classes need it
    # declared explicitly.
    __slots__ = (
        "__weakref__",
        "_arraysize",
        "_closed",
        "_completed_iterations",
        "_connection",
        "_description",
        "_lastrowid",
        "_row_factory",
        "_row_index",
        "_rowcount",
        "_rows",
        "messages",
    )

    def __init__(self, connection: "Connection") -> None:
        self._connection = connection
        self._description: _Description = None
        self._rowcount = -1
        self._arraysize = 1
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self._closed = False
        self._lastrowid: int | None = None
        # Count of executemany() iterations that completed
        # successfully on the most recent call. After a normal
        # successful executemany, equals len(seq_of_parameters); after
        # a cancel / error mid-batch, equals the number of iterations
        # that had already committed server-side (since each iteration
        # autocommits when there's no surrounding BEGIN). Lets callers
        # write idempotent compensation after cancel — observability-
        # complement to ``_rowcount`` which resets to PEP 249's
        # "undetermined" sentinel on the cancel path.
        self._completed_iterations: int = 0
        # stdlib ``sqlite3.Cursor.row_factory`` parity. None means
        # "return plain tuples" (PEP 249 default). New cursors inherit
        # the parent Connection's default factory if set. The
        # ``isinstance`` check restricts inheritance to real Connection
        # instances (and user subclasses — common cross-cutting pattern
        # such as ``class TracingConnection(Connection)``) — MagicMock-
        # typed test fakes have an auto-magic ``_row_factory`` attribute
        # that would otherwise silently wrap every row. The import is
        # deferred to call time to break the cursor → connection import
        # cycle (``Connection`` only appears in ``TYPE_CHECKING`` at
        # module scope).
        from dqlitedbapi.connection import Connection as _Connection

        self._row_factory: RowFactory | None = (
            getattr(connection, "_row_factory", None)
            if isinstance(connection, _Connection)
            else None
        )
        # PEP 249 optional extension. Currently no driver path appends
        # to this list; it's here so consumers can rely on the
        # attribute existing and being mutable. The tuple-value type
        # is the ``exception value`` per PEP 249 §13 — an Exception
        # instance, not a string. Tightening the annotation to
        # ``Exception`` prevents future producer drift away from the
        # specification.
        self.messages: list[tuple[type[Exception], Exception]] = []

    @property
    def connection(self) -> "Connection":
        """The Connection this Cursor was created from.

        PEP 249 optional extension. Read-only.

        After ``close()``, ``self._connection`` is swapped for a
        ``weakref.proxy`` so the closed cursor doesn't pin the
        Connection's daemon loop. Once the Connection is itself
        GC'd, attribute access on the proxy raises ``ReferenceError``
        — outside the PEP 249 ``Error`` hierarchy. Catch and re-raise
        as ``InterfaceError`` so cross-driver code wrapping cursor
        introspection in ``except dbapi.Error:`` continues to match.

        **Affinity note**: the getter itself does not run
        ``_check_thread()`` — read-only property reads are documented
        as bypass-permitted (matches the ``description`` / ``rowcount``
        / ``lastrowid`` / etc. discipline at the cursor surface). The
        affinity check fires at the NEXT method call boundary on the
        returned handle (``cur.connection.execute(...)`` triggers
        ``Connection.cursor()``'s ``_check_thread``). Operators
        triaging cross-thread misuse should walk one frame down from
        ``cur.connection.foo()``-style indirection — the traceback
        will point at the called method, not the getter.
        """
        # ``AttributeError`` is included in the catch because the
        # probe reads ``.address`` to force the ``weakref.proxy`` to
        # resolve; on a partially-constructed or mock-typed parent
        # (``Cursor.__new__(Cursor); cur._connection = object()``)
        # the probe raises bare ``AttributeError``, which would escape
        # the PEP 249 ``Error`` hierarchy. The probe's reliance on
        # ``.address`` being side-effect-free is structural — a
        # future refactor adding a thread/loop check to ``.address``
        # would break the probe for live parents on foreign
        # threads/loops; keep ``.address`` side-effect-free.
        try:
            # Touch any attribute to force the proxy to resolve. If
            # the underlying Connection has been GC'd, this raises
            # ReferenceError; otherwise the resolved object is the
            # live Connection.
            _ = self._connection.address
        except ReferenceError as e:
            raise InterfaceError("Cursor's parent Connection has been garbage-collected") from e
        except AttributeError as e:
            raise InterfaceError(
                f"Cursor's parent Connection unavailable: {type(e).__name__}: {e}"
            ) from e
        return self._connection

    @property
    def description(self) -> _Description:
        """Column descriptions for the last query.

        Returns a tuple of 7-tuples:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)

        ``type_code`` is the wire-level ``ValueType`` integer from the first
        result frame (e.g. 10 for ISO8601, 9 for UNIXTIME). The other fields
        are None — dqlite doesn't expose them.

        Returns the same tuple object on each access (matching stdlib
        ``sqlite3.Cursor.description``). A tuple is structurally
        immutable so no defensive copy is needed to keep the cursor's
        internal state safe from caller mutation.

        **Mixed-type columns flatten to the FIRST non-NULL row's wire
        tag.** When row 0's column is NULL but subsequent rows carry
        typed values, ``type_code`` is resolved by scanning forward to
        the first non-NULL row at that column — see the
        ``row_types`` rescue-scan block in ``execute``. SQLite per-cell
        typing (see
        sqlite.org/datatype3.html §3.3) does not constrain per-row
        storage class, so two rows in the same column may carry
        different wire types; the resolver picks the FIRST non-NULL
        and the per-row dispatch via ``row_types[i]`` (the data path)
        still handles the actual conversion per row. Callers using
        ``description`` for parser-shape decisions should NOT assume
        column uniformity; for mixed-type columns the
        per-row ``row_types[i]`` (exposed via ``_convert_row``) is the
        correct source. The all-NULL column case (every row's value
        at that column is NULL) genuinely returns ``type_code=None``
        — the only PEP 249 §6.1.2 unrecoverable case. Stdlib
        ``sqlite3`` always returns ``None`` for ``type_code``; this
        driver is more informative but loses fidelity on mixed-type
        columns.
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """Number of rows affected by the last execute.

        Returns -1 if not applicable or unknown.
        """
        return self._rowcount

    @property
    def completed_iterations(self) -> int:
        """Count of executemany() iterations that completed
        successfully on the most recent call.

        After a normal successful executemany, equals
        ``len(seq_of_parameters)``. After a cancel / error mid-batch,
        retains the count of iterations that already committed
        server-side (since each iteration auto-commits when there is
        no surrounding ``BEGIN``). 0 after a never-executed cursor or
        a single-row execute.

        Observability hook for idempotent compensation: a caller
        recovering from a cancelled executemany can read this to know
        which prefix of ``seq_of_parameters`` already persisted.
        Complements ``rowcount`` which resets to PEP 249's
        "undetermined" sentinel (-1) on the cancel path.

        Resets to 0 at the start of every new executemany call.
        """
        return self._completed_iterations

    @property
    def lastrowid(self) -> int | None:
        """ROWID of this cursor's most-recent successful INSERT.

        Returns ``None`` before the first INSERT runs on this cursor
        and after ``close()`` scrubs the cursor's state.

        Cursor-scoped, matching stdlib ``sqlite3.Cursor.lastrowid``: a
        sibling cursor on the same Connection does NOT observe this
        cursor's last INSERT (each cursor stores its own snapshot
        captured at INSERT time from the underlying connection's
        ``sqlite3_last_insert_rowid``). ROLLBACK / UPDATE / DELETE /
        DDL do NOT clear it (mirroring stdlib), but ``close()``
        scrubs it as part of the closed-cursor "no operation
        performed" surface contract.

        **Not updated for ``INSERT ... RETURNING``** (or any row-returning
        statement). dqlite's wire protocol does not return
        ``last_insert_id`` on row-returning responses (it is only
        populated on Exec responses), so the row-returning execute path
        cannot surface the rowid. Read the id from the returned row
        instead. This IS a divergence from stdlib ``sqlite3.Cursor.
        lastrowid``, which updates after ``INSERT ... RETURNING``.
        """
        return self._lastrowid

    @property
    def rownumber(self) -> int | None:
        """0-based index of the next row in the current result set.

        PEP 249 optional extension: returns ``None`` if no result set is
        active (no query executed, or last statement was DML without
        RETURNING); otherwise returns the index of the row that the next
        ``fetchone()`` would produce.

        **Thread affinity**: runs ``_check_thread()`` so a foreign-
        thread reader does not observe a mid-fetch ``_row_index``
        (mutated by ``fetchone``/``fetchmany`` on the creator
        thread). Mirrors the ``in_transaction`` discipline at the
        connection layer — both are getters consumed by callers to
        drive state decisions (rownumber is the de facto streaming-
        result diagnostic in psycopg2/3 ports), so a stale or
        partially-mutated read is materially wrong, not merely
        suboptimal.

        **Closed-state read returns** ``None`` (not ``Error``). This is
        intentional and ambiguous with the legitimate "no result set
        active" return. ``close()`` scrubs ``_description`` to ``None``
        so the property short-circuits at the same branch as a fresh
        cursor that has not yet executed. Cross-driver code treating
        a ``None`` rownumber as "DML cursor, no rows" will misclassify
        a closed cursor as a healthy DML cursor; gate on the public
        ``cur.closed`` attribute BEFORE consulting ``rownumber`` if
        the distinction matters:

        .. code-block:: python

            if cur.closed:
                ...
            elif cur.rownumber is None:
                # truly no result set
                ...
            else:
                # streaming result; rownumber is the index of next row
                ...

        Matches the bypass discipline of sibling read-only accessors
        ``description`` and ``rowcount`` — none of the three raise on
        a closed cursor. Forward-compat note: a future major version
        may switch these accessors to raising on a closed cursor
        (matching psycopg2/3 semantics); callers wishing to be
        forward-compatible should use the ``cur.closed`` gate today
        rather than relying on the silent-``None`` alias.
        """
        # ``getattr`` defensively in case a ``Cursor.__new__``-built
        # fixture has no ``_connection`` slot yet, or the parent
        # ``Connection`` has been GC'd (the proxy is alive but every
        # attribute access raises ``ReferenceError``).
        conn = getattr(self, "_connection", None)
        try:
            check = getattr(conn, "_check_thread", None)
        except ReferenceError:
            check = None
        if check is not None:
            check()
        if self._description is None:
            return None
        return self._row_index

    @property
    def arraysize(self) -> int:
        """Number of rows to fetch at a time with fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        # PEP 249 §6.4 ``messages`` clear-on-entry: every public
        # state-mutating method clears ``self.messages`` first,
        # before the closed/thread guards. The suppress tolerates
        # ``__new__``-built test fixtures that bypass ``__init__``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        # Reject bools explicitly even though ``bool`` is an ``int``
        # subclass: ``arraysize = True`` silently coercing to 1 is a
        # caller-bug trap, not a useful affordance.
        # PEP 249 §6.1.2: any method on a closed cursor must raise an
        # ``Error`` subclass. Apply the closed-state guard on the
        # SETTER (state-mutating op on a closed cursor); leave the
        # GETTER permissive — peer drivers (sqlite3, psycopg) also
        # let the read pass on a closed cursor as a defensive accessor.
        self._check_closed()
        # State-mutating setter on a Connection-allocated cursor —
        # enforce the threadsafety=1 affinity contract documented on
        # the parent ``Connection`` class. Sibling ``Connection.row_factory.setter``
        # already does this; mirror here so a foreign-thread
        # ``cur.arraysize = 1`` mid-batch is caught at the boundary
        # — UNLESS the connection was opened with
        # ``check_same_thread=False``, which short-circuits the
        # thread check at the Connection layer to opt INTO sharing.
        # Connection sharing is the explicit relaxation that flag
        # enables; cursor sharing has no separate opt-in flag, so
        # under ``check_same_thread=False`` the cursor-per-thread
        # sub-contract becomes the caller's responsibility. Adding a
        # separate cursor-layer ``_creator_thread`` check would
        # re-impose what the user explicitly opted out of, so the
        # delegation to ``_check_thread`` here is intentional. Pin
        # `tests/test_cursor_arraysize_row_factory_setter_thread.py`
        # for the documented behaviour.
        self._connection._check_thread()
        if not isinstance(value, int) or isinstance(value, bool):
            raise ProgrammingError(f"arraysize must be a positive int, got {type(value).__name__}")
        if value < 1:
            raise ProgrammingError(f"arraysize must be >= 1, got {value}")
        self._arraysize = value

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` has been called.

        Peer-driver parity (psycopg, asyncpg). PEP 249 does not
        require it; the underlying flag is already maintained.
        """
        return self._closed

    @property
    def row_factory(self) -> RowFactory | None:
        """stdlib ``sqlite3.Cursor.row_factory`` parity hook.

        Set to a callable ``factory(cursor, row) -> Any`` to wrap each
        fetched tuple before returning. ``None`` (default) returns
        plain tuples per PEP 249.

        **Factory contract**: the factory is called as
        ``factory(cursor, row)`` where ``cursor`` is the dqlite
        ``Cursor`` instance. Factories that require a specific
        ``Cursor`` subclass — notably ``sqlite3.Row``, whose
        C-extension constructor type-checks the first argument to
        be ``pysqlite_CursorType`` — do NOT work with this driver
        and surface ``DataError("row_factory call failed: ...")``
        at the first fetch.

        Working shapes:

        - ``lambda cur, row: dict(zip([c[0] for c in cur.description], row))``
          for a dict factory.
        - ``namedtuple`` builders: ``lambda cur, row: MyTuple._make(row)``
          (ignores the cursor arg).
        - Plain callables that read ``cur.description`` for column
          names.

        New cursors inherit the parent Connection's default factory.
        Setting ``cur.row_factory = ...`` overrides per-cursor.
        """
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        # PEP 249 §6.4 ``messages`` clear-on-entry; see ``arraysize.setter``.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        # PEP 249 §6.1.2: state-mutating ops on a closed cursor raise.
        self._check_closed()
        # Threadsafety=1 affinity contract — see ``arraysize.setter``
        # for the rationale (cross-thread mutation can silently swap
        # the creator-thread's per-row materialisation hook).
        # Same caveat applies: ``_check_thread`` short-circuits under
        # ``check_same_thread=False`` (the Connection-sharing opt-in),
        # so under that flag the cursor-per-thread sub-contract is
        # the caller's responsibility.
        self._connection._check_thread()
        if value is not None and not callable(value):
            raise ProgrammingError(
                f"row_factory must be callable or None, got {type(value).__name__}"
            )
        self._row_factory = value

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError(f"Cursor is closed (id={id(self)})")

    def _reset_execute_state(self) -> None:
        """Clear per-execute state to the "no result set" baseline.

        Matches stdlib ``sqlite3.Cursor.execute``, which resets
        ``description`` to ``None`` *before* preparing the statement so
        that a mid-execute failure cannot leave the cursor reporting
        the prior query's result shape. ``_lastrowid`` is cursor-scoped
        but survives across execute / ROLLBACK / UPDATE / DELETE / DDL
        so callers doing ``INSERT; SELECT last_insert_rowid()`` on the
        same cursor still see the correct value (see the ``lastrowid``
        property docstring). ``close()`` is the single lifecycle event
        that scrubs it, matching the cursor-scoped contract.
        """
        self._description = None
        self._rows = []
        self._row_index = 0
        self._rowcount = -1
        # Reset the per-call completed-iteration counter alongside the
        # other per-execute state. ``executemany`` increments it in
        # its loop body; without the reset here the counter would
        # carry over from a prior ``executemany`` into a subsequent
        # single-row ``execute``, contradicting the
        # ``completed_iterations`` property's documented contract
        # ("0 after a never-executed cursor or a single-row execute").
        self._completed_iterations = 0

    def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> Self:
        """Execute a database operation (query or command).

        Returns ``self`` so callers can chain ``.fetchall()`` etc.
        """
        del self.messages[:]
        # ``_check_closed`` BEFORE ``_check_thread``: ``Cursor.close()``
        # swaps ``self._connection`` for a ``weakref.proxy``; once the
        # parent ``Connection`` is GC'd, ``_connection._check_thread()``
        # raises ``ReferenceError`` — outside the PEP 249 ``Error``
        # hierarchy. ``_check_closed`` reads only ``self._closed`` and
        # raises ``InterfaceError`` (a real ``Error`` subclass).
        self._check_closed()
        self._connection._check_thread()
        # Input-validation rejection FIRST: a non-str ``operation`` is
        # a caller-shape misuse that fires before any SQL parser would
        # touch it, and stdlib ``sqlite3`` preserves the prior result
        # set on this path:
        #     >>> cur.execute("SELECT 1"); cur.execute(123)
        #     # raises TypeError; cur.description is the SELECT's tuple
        # Stdlib only scrubs on prepare-stage rejections (empty SQL,
        # multi-statement, NUL byte, bind-count mismatch). Match that
        # split: ``ProgrammingError`` here surfaces with cursor state
        # intact so a caller's retry-with-coerce idiom can inspect
        # ``cur.description`` to shape the retry. Mirrors the
        # ``executemany`` sibling.
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )

        # Caller-shape rejection BEFORE the reset: passing a Mapping
        # (for qmark), set, str, bytes, etc. for ``parameters`` is a
        # caller-shape misuse symmetric with the non-str ``operation``
        # arm above. Per the documented preservation contract (stdlib
        # parity), the prior result set must survive these rejections
        # so a retry-with-coerce idiom can inspect ``cur.description``.
        _validate_caller_param_shape(parameters)

        # Prepare-stage path: scrub per-execute state (description /
        # rowcount / rows / row_index) so a rejected ``execute`` lands
        # at the stdlib "no result set" baseline rather than reporting
        # the prior query's shape. ``_reset_execute_state`` deliberately
        # does NOT touch ``_lastrowid``, so the preserve-across-rejection
        # contract for lastrowid is unaffected.
        self._reset_execute_state()
        # Pre-flight classification pass: empty SQL → ProgrammingError
        # (PEP 249 §7), multi-statement → ProgrammingError (stdlib
        # parity), wrong ``?``-count vs ``len(parameters)`` →
        # ProgrammingError. All three skip the wire round-trip so a
        # caller bug surfaces with the right class at the user's
        # call site rather than as ``OperationalError`` (server
        # rejection) or silent data loss (multi-statement drop).
        _classify_caller_sql(operation, parameters)

        # Intercept ``PRAGMA busy_timeout`` at the cursor layer BEFORE
        # the wire round-trip — dqlite's VFS authorizer rejects the
        # PRAGMA server-side (see done/dbapi-pragma-deny-list-no-
        # regression-pin.md), so the canonical SQLite escape hatch
        # would otherwise raise ``DatabaseError("not authorized")``.
        # The interception updates the connection's ``_busy_timeout``
        # (setter form) and writes the cursor's result state so
        # ``cur.fetchone()`` returns the new (or current) value as a
        # single-row result, matching stdlib's PRAGMA shape.
        from dqlitedbapi._pragma_intercept import (
            try_intercept_busy_timeout,
            try_rewrite_begin_to_immediate,
        )

        if try_intercept_busy_timeout(self, operation, parameters):
            return self

        # Rewrite bare ``BEGIN`` / ``BEGIN TRANSACTION`` to
        # ``BEGIN IMMEDIATE`` when the connection's session_mode is
        # ``"immediate"`` (the default). This eliminates the
        # ``SQLITE_BUSY_SNAPSHOT (517)`` race for SELECT-then-INSERT
        # transactions: the writer-lock is acquired at BEGIN time so
        # the read snapshot cannot be overtaken by a concurrent
        # committer. Concurrent ``BEGIN IMMEDIATE`` calls contend at
        # the writer-lock and surface as ordinary
        # ``SQLITE_BUSY (5)`` — absorbed transparently by the
        # busy_timeout retry curve below. Other session_mode values
        # (``"deferred"``, ``"exclusive"``, ``"read_only"``) leave
        # bare ``BEGIN`` untouched. Explicit ``BEGIN IMMEDIATE`` /
        # ``BEGIN DEFERRED`` / ``BEGIN EXCLUSIVE`` always pass
        # through. Configure via
        # ``connect(..., session_mode="...")`` or
        # ``DQLITE_SESSION_MODE`` env var.
        rewritten = try_rewrite_begin_to_immediate(
            operation,
            session_mode=getattr(self._connection, "_dqlite_session_mode", "immediate"),
        )
        if rewritten is not None:
            operation = rewritten

        # ``retry_sync_on_busy`` wraps the wire round-trip with the
        # SQLite-curve BUSY retry (stdlib parity for the C-level
        # ``sqlite3_busy_timeout`` callback). The coro-factory shape
        # lets the helper build a fresh coroutine on each retry
        # (coroutines are single-use). ``busy_timeout=0`` short-
        # circuits to a single ``_run_sync`` call with no retry.
        # ``_resolve_busy_timeout_seconds`` gives a MagicMock-safe
        # fall-through (0.0) so test fixtures with a mock connection
        # still drive the no-retry path identical to pre-feature.
        from dqlitedbapi._busy_retry import _resolve_busy_timeout_seconds, retry_sync_on_busy

        retry_sync_on_busy(
            _resolve_busy_timeout_seconds(self._connection),
            self._connection._run_sync,
            lambda: self._execute_async(operation, parameters),
        )
        return self

    async def _execute_async(self, operation: str, parameters: Sequence[Any] | None = None) -> None:
        """Async implementation of execute.

        Routes through DqliteConnection's public API (execute/query_raw_typed)
        which goes through _run_protocol(), providing the _in_use guard,
        connection invalidation on fatal errors, and leader-change detection.
        """
        conn = await self._connection._get_async_connection()
        params = _convert_params(parameters)

        if _is_row_returning(operation):
            columns, column_types, row_types, rows = await _call_client(
                conn.query_raw_typed(operation, params)
            )
            if not columns:
                # ``_is_row_returning`` classifies ``PRAGMA`` as row-
                # returning, which is correct for the read form
                # (``PRAGMA foreign_keys``) but wrong for the write form
                # (``PRAGMA foreign_keys = ON``) which produces no
                # columns. Stdlib ``sqlite3`` sets ``description = None``
                # AND keeps ``rowcount = -1`` (unknown / not applicable)
                # for non-result statements; match both so callers who
                # branch on ``cur.rowcount > 0`` after a PRAGMA write
                # see ``-1`` (stdlib parity) rather than ``0`` (the
                # row-returning branch's literal ``len(rows)``).
                self._description = None
                self._rows = []
                self._row_index = 0
                self._rowcount = -1
                return
            else:
                # PEP 249 §6.1.2 says ``type_code`` "must compare equal
                # to one of Type Objects." ``None`` never compares equal
                # to ``STRING`` / ``NUMBER`` / ``BINARY`` / ``DATETIME`` /
                # ``ROWID`` (the comparison protocol returns
                # ``NotImplemented`` → False), so a fallback of ``None``
                # silently produced PEP-249-illegal descriptions when
                # the wire layer returned fewer type codes than columns.
                #
                # Empty-result deviation from PEP 249 §6.1.2:
                # ``RowsResponse`` derives ``column_types`` from the
                # first row's type header, so a zero-row result set
                # returns ``column_types == []`` and the type
                # information is genuinely unrecoverable from the
                # wire — dqlite's protocol does not carry declared
                # column affinity separately from the per-row type
                # tags. PEP 249 says ``type_code`` "must compare
                # equal to one of Type Objects"; ``None`` does not
                # (NotImplemented → False under every Type Object's
                # ``__eq__``), so emitting ``None`` silently broke
                # the canonical ``type_code == STRING`` introspection
                # idiom. Emit the ``UNKNOWN`` sentinel (a real Type
                # Object with empty ``values``) instead so the
                # contract is satisfied: equality against
                # STRING/BINARY/NUMBER/DATETIME/ROWID cleanly
                # returns False (no TypeError, no spurious True),
                # and callers that want to detect the wire-can't-
                # resolve case explicitly can write
                # ``type_code == UNKNOWN``. Callers that need true
                # column-type introspection on empty result sets
                # should still issue a PRAGMA table_info(...) query
                # separately. For the real anomaly (rows present
                # but short ``column_types``), raise ``DataError``
                # so the wire bug surfaces loudly.
                if len(column_types) == 0 and len(rows) == 0:
                    type_codes: list[int | _DBAPIType] = [_UNKNOWN_TYPE] * len(columns)
                elif len(column_types) != len(columns):
                    raise DataError(
                        f"Wire response has {len(columns)} columns but "
                        f"{len(column_types)} type codes"
                    )
                else:
                    # Map ValueType.NULL (5) to None ONLY when every
                    # row's value at that column index is also NULL —
                    # otherwise scan ``row_types[1:]`` for the first
                    # non-NULL type tag and surface that as the
                    # column's PEP 249 §6.1.2 ``type_code``. The wire
                    # carries per-row type information (used by the
                    # ``_convert_row`` dispatch below); without this
                    # rescue scan, a SELECT whose first row's column
                    # is NULL but whose subsequent rows carry typed
                    # values lost the column-type signal entirely in
                    # ``description`` even though the wire DID carry
                    # the information. PEP 249 says ``type_code``
                    # "must compare equal to one of Type Objects";
                    # ``None`` does not. The narrower fallback only
                    # fires when EVERY row's value at that column is
                    # NULL — a genuinely unrecoverable case.
                    type_codes = []
                    for col_idx, c in enumerate(column_types):
                        if c != ValueType.NULL:
                            type_codes.append(int(c))
                            continue
                        # Scan subsequent rows for the first non-NULL
                        # type tag at this column.
                        resolved: int | _DBAPIType = _UNKNOWN_TYPE
                        for j in range(1, len(row_types)):
                            if col_idx < len(row_types[j]):
                                candidate = row_types[j][col_idx]
                                if candidate != ValueType.NULL:
                                    resolved = int(candidate)
                                    break
                        type_codes.append(resolved)
                self._description = tuple(
                    (name, type_codes[i], None, None, None, None, None)
                    for i, name in enumerate(columns)
                )
            # Per-row dispatch: SQLite's dynamic typing means two rows in
            # the same column can carry different wire types. Use
            # ``row_types[i]`` rather than ``column_types`` so a row
            # whose wire type diverges from row 0 is decoded correctly.
            # ``_convert_rows`` collapses to a tuple-materialisation
            # fast path when no column carries a registered converter
            # (the typical case), skipping the per-cell walk.
            self._rows = _convert_rows(rows, row_types, column_types)
            self._row_index = 0
            # Divergence from stdlib ``sqlite3.Cursor.rowcount`` /
            # psycopg2 / aiosqlite (which all return -1 for SELECT
            # because the cursor cannot know the count without
            # consuming). This driver returns ``len(rows)`` because
            # the wire layer buffers the entire result set up front,
            # so the count is known at execute time. The RETURNING
            # path (INSERT/UPDATE/DELETE ... RETURNING — classified
            # as row-returning by ``_is_row_returning``) relies on
            # this real count for SQLAlchemy's insertmanyvalues
            # contract. PEP 249 §6.1.2 explicitly permits "the
            # number of rows that the last execute*() produced (for
            # DQL statements)". Cross-driver code using
            # ``cur.rowcount > 0`` as a "did SELECT find anything"
            # gate behaves differently against this driver; the
            # portable idiom is ``cur.fetchone() is not None``.
            self._rowcount = len(rows)
        else:
            last_id, affected = await _call_client(conn.execute(operation, params))
            # stdlib-parity: lastrowid only updates on INSERT / REPLACE.
            # UPDATE / DELETE / DDL leave the previous INSERT's rowid
            # in place — the wire returns 0 / stale values on those
            # paths and unconditionally writing would zero the sticky
            # value. See ``_is_insert_or_replace`` for rationale.
            if _is_insert_or_replace(operation):
                self._lastrowid = _to_signed_int64(last_id)
            if _is_dml_rowcount_meaningful(operation):
                self._rowcount = _to_signed_int64(affected)
            else:
                self._rowcount = -1
            self._description = None
            self._rows = []
            # Parity with the SELECT branch and with executemany: every
            # execute must leave the cursor at row 0 of its (possibly
            # empty) result set so a subsequent SELECT iterator starts
            # from a clean state.
            self._row_index = 0

    def executemany(self, operation: str, seq_of_parameters: Iterable[Sequence[Any]], /) -> Self:
        """Execute a database operation multiple times.

        Cancellation atomicity: this driver runs in autocommit-by-default
        mode. Without a surrounding ``BEGIN`` / ``COMMIT``, each
        iteration commits server-side independently. If the call is
        interrupted mid-batch (sync timeout, ``KeyboardInterrupt``,
        etc.), iterations that already completed remain persisted.
        Wrap in an explicit transaction to make the batch atomic. See
        the ``Connection`` class docstring for the autocommit-by-
        default rationale.

        ``lastrowid`` semantics: modern stdlib
        ``sqlite3.Cursor.executemany`` (Python 3.10+) updates
        ``lastrowid`` per iteration so the final value is the
        LAST iteration's row id (the older "left unchanged"
        contract from earlier docs was a behaviour shift, not the
        current discipline — see CPython
        ``Modules/_sqlite/cursor.c::execute_one_iter``). This
        driver clears ``lastrowid`` to ``None`` on the non-empty
        batch path rather than picking an arbitrary
        last-iteration row to surface: the batch's ambiguity
        (which of N inserts is "the" rowid?) makes ANY single
        value misleading. On the empty-batch path (zero
        iterations) the pre-batch snapshot is preserved — a
        deliberate divergence from modern stdlib (which clears
        even on empty) so a caller's prior single-row INSERT's
        ``lastrowid`` survives a follow-on no-op
        ``executemany``. Cross-driver code that needs a rowid
        should use a single-row ``execute`` or read the id from
        ``RETURNING`` rows.

        Rejected calls (transaction-control verbs, non-DML row-returning
        shapes) preserve the prior ``lastrowid`` — no batch ran, so the
        cursor docstring's lifecycle contract holds (only ``close()``
        scrubs ``lastrowid``). This matches the async sibling's rejection
        path.
        """
        del self.messages[:]
        # See ``execute``'s prelude comment for the ordering rationale.
        self._check_closed()
        self._connection._check_thread()
        # Input-validation rejections FIRST: ``None`` seq, bad outer
        # shape (str / dict / set / bytes), non-str operation — these
        # are caller-shape misuses that fire before any SQL parser
        # touches the bytes, and stdlib ``sqlite3`` preserves prior
        # cursor state on these paths. Stdlib only clears on
        # prepare-stage rejections (verb-reject / row-returning /
        # bind-count / empty SQL). Match stdlib's split so a retry-
        # with-coerce idiom can inspect ``cur.description`` after a
        # caught ``ProgrammingError`` from this arm.
        if seq_of_parameters is None:
            raise ProgrammingError(
                "executemany() seq_of_parameters must be a sequence/iterable, not None",
                code=None,
            )
        # Reject outer shapes that would silently iterate over keys
        # (dict) / characters (str / bytes / bytearray / memoryview) or
        # iterate in non-deterministic order (set / frozenset), treating
        # each yielded element as a parameter set. Without this check,
        # ``cur.executemany(sql, "abc")`` mutated cursor state before
        # the inner per-character binding raised the less-actionable
        # ``parameter type str not supported`` message.
        _validate_executemany_seq_shape(seq_of_parameters)
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )
        # Prepare-stage path begins here. Scrub per-execute state so a
        # rejected ``executemany`` (verb-reject / row-returning /
        # PRAGMA) lands at the stdlib "no result set" baseline rather
        # than reporting the prior query's shape. ``_reset_execute_state``
        # deliberately does NOT touch ``_lastrowid``, so the preserve-
        # across-rejection contract for lastrowid is unaffected. The
        # post-loop clear at the end of ``_executemany_async`` handles
        # the documented "clear after success" contract for the
        # admitted-verb path.
        self._reset_execute_state()
        # ``_lastrowid`` clear is intentionally deferred until AFTER the
        # verb-rejection guards below. A rejected ``executemany`` (a
        # transaction-control verb, a row-returning shape) means no
        # batch ran — clearing here would clobber the prior INSERT's
        # rowid, violating both the cursor docstring contract
        # ("ROLLBACK / UPDATE / DELETE / DDL do NOT clear it... close()
        # is the single lifecycle event that scrubs it") and parity with
        # the async sibling's rejection path.
        # Reject transaction-control verbs and pure queries up front so
        # the caller's frame sees the ProgrammingError rather than
        # having it surface deep inside the async helper. stdlib
        # sqlite3.Cursor.executemany does the same.
        # Strip leading semicolons + interleaved whitespace BEFORE the
        # verb extraction so ``";BEGIN ..."`` (semicolon-prefixed) and
        # ``"; ; BEGIN ..."`` (semicolon-whitespace-semicolon) cannot
        # bypass the reject-list. The trailing ``rstrip(";")`` then
        # canonicalises a verb glued to a trailing semicolon
        # (``"BEGIN;"``, ``"COMMIT;"``) into the bare verb. Without
        # both ends, ``executemany(";BEGIN INSERT ...", ...)`` or
        # ``executemany("BEGIN; INSERT ...", ...)`` was silently
        # admitted and re-ran the bare statement N times.
        # Loop comment-strip + ;-strip together so a leading ``;``
        # followed by a comment (``"; /* x */ SAVEPOINT foo"``) does
        # not bypass the reject-list — the original single-pass
        # comment-strip-then-semicolon-loop missed comments that sat
        # AFTER a leading ``;``. Each iteration consumes either a
        # comment or a ``;`` (or both) and re-strips before checking
        # the verb.
        head_normalised = operation
        while True:
            stripped = _strip_leading_comments(head_normalised).lstrip()
            if stripped.startswith(";"):
                head_normalised = stripped[1:]
                continue
            if stripped == head_normalised:
                break
            head_normalised = stripped
        head_normalised = head_normalised.upper()
        first_verb = head_normalised.split(maxsplit=1)[0].rstrip(";") if head_normalised else ""
        if first_verb in _EXECUTEMANY_REJECT_VERBS:
            raise ProgrammingError(
                f"executemany() not supported for {first_verb}; "
                "use execute() instead — transaction-control statements "
                "take no parameters and cannot be batched."
            )
        if _is_row_returning(operation) and not _is_dml_with_returning(operation):
            # Use the already-computed comment-stripped uppercase form
            # so ``-- comment\nPRAGMA foreign_keys`` still routes to the
            # PRAGMA-specific diagnostic. The prior raw lstrip().upper()
            # left the user with the less actionable "use execute() for
            # SELECT / VALUES / PRAGMA / EXPLAIN / WITH" message.
            if head_normalised.startswith("PRAGMA"):
                # Specific guidance for PRAGMA: it has per-call
                # side-effect semantics and is never meaningfully
                # batchable, even when the syntactic shape would fit
                # an executemany loop. The grouped message above
                # would leave the user wondering whether a different
                # PRAGMA would be acceptable.
                raise ProgrammingError(
                    "executemany() does not accept PRAGMA; PRAGMAs have "
                    "per-call semantics and are not batchable. Use "
                    "execute() for each PRAGMA."
                )
            raise ProgrammingError(
                "executemany() can only execute DML statements; "
                "use execute() for SELECT / VALUES / PRAGMA / EXPLAIN / WITH."
            )

        self._connection._run_sync(self._executemany_async(operation, seq_of_parameters))
        return self

    async def _executemany_async(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]]
    ) -> None:
        """Async implementation of executemany.

        An empty ``seq_of_parameters`` must not leave stale SELECT
        state around: reset description / rows to None / empty so
        callers can't confuse an empty executemany with a preceding
        SELECT.

        For statements with a RETURNING clause (or any result-producing
        DML), each iteration's rows are accumulated so ``fetchall`` at
        the end yields every returned row across all parameter sets.
        Without the accumulation, ``_execute_async`` would overwrite
        ``_rows`` on each iteration and only the rows from the last
        parameter set would survive.

        Pure queries (SELECT / VALUES / PRAGMA) are rejected in the
        sync ``executemany`` wrapper before this helper is scheduled.
        """
        # Snapshot ``_completed_iterations`` BEFORE the
        # ``_reset_execute_state()`` call. The reset zeros the counter
        # so the loop body can accumulate per-iteration progress;
        # the snapshot lets the BaseException arm distinguish
        # "input-validation raise (zero in-batch progress)" from
        # "mid-batch raise (partial progress)" and restore the
        # pre-batch value on the former while preserving the
        # partial-progress count on the latter — both observability
        # contracts hold simultaneously.
        completed_iterations_pre_batch = self._completed_iterations
        # Single source of truth for per-execute reset; see
        # ``_reset_execute_state``. Also zeroes ``_rowcount`` to -1 so
        # an empty ``seq_of_parameters`` ends with the same
        # ``rowcount`` shape as empty ``execute``. Reset BEFORE the
        # classifier so a classifier-raise (empty SQL / multi-
        # statement / NUL byte) leaves the cursor at the no-result
        # baseline, matching stdlib's reset-then-prepare ordering
        # and the sibling ``execute`` flow. ``_reset_execute_state``
        # also zeroes ``_completed_iterations`` so the per-call
        # counter is clean before the loop body runs.
        self._reset_execute_state()
        # Hoist ``_classify_caller_sql`` ONCE at the top: empty SQL,
        # multi-statement, NUL-in-SQL, and the SQL parsing/scanning
        # are all invariant across iterations. The placeholder count
        # derived here is then compared per-iteration against
        # ``len(params)`` — a cheap O(1) check vs the per-iteration
        # regex traversal that calling the full classifier per row
        # would cost. ``skip_param_count_check=True`` skips the
        # placeholder length-check (the classifier still runs the
        # empty/multi/NUL guards, which are pure SQL parses).
        # On a classifier raise (empty SQL / multi-statement / NUL
        # byte), restore the pre-batch ``_completed_iterations``
        # snapshot so the observability counter reflects the prior
        # batch's progress instead of the just-zeroed baseline.
        # See the BaseException arm below for the conditional
        # restore that handles the mid-batch raise case.
        try:
            _classify_caller_sql(operation, None, skip_param_count_check=True)
        except BaseException:
            self._completed_iterations = completed_iterations_pre_batch
            raise
        cleaned = _strip_sql_noise(operation)
        placeholder_count = cleaned.count("?")
        acc = _ExecuteManyAccumulator(max_rows=self._connection._max_total_rows)
        # Snapshot the pre-batch lastrowid so a mid-batch cancel can
        # restore it. Without this snapshot, ``_execute_async``'s
        # per-iteration write of ``self._lastrowid`` on
        # INSERT/REPLACE rows leaks into the cursor surface: a caller
        # who read ``cur.lastrowid == 5`` from a prior single-row
        # INSERT, then cancelled an ``executemany`` mid-batch, would
        # observe whichever rowid the last in-batch iteration wrote
        # (e.g. ``6`` or ``7``) instead of the pre-batch ``5``. The
        # BaseException arm below restores this snapshot.
        lastrowid_pre_batch = self._lastrowid
        try:
            for params in seq_of_parameters:
                # Per-iteration structural reject + ``?``-count check.
                # Mirrors ``_classify_caller_sql``'s discipline: the
                # structural-type reject runs BEFORE ``len(params)``
                # so a single string/bytes row surfaces with the
                # sharp structural diagnostic rather than a
                # misleading per-character count. Unsized iterables
                # deliberately skip the count check and fall through
                # to the bind layer's rejection.
                _validate_caller_param_shape(params)
                if params is not None:
                    try:
                        param_count = len(params)
                    except TypeError:
                        param_count = -1
                    if param_count >= 0 and param_count != placeholder_count:
                        raise ProgrammingError(
                            f"Incorrect number of bindings supplied. The "
                            f"current statement uses {placeholder_count}, "
                            f"and there are {param_count} supplied."
                        )
                # Per-iteration BUSY retry (stdlib parity). Wrapping
                # the OUTER ``_run_sync(_executemany_async(...))`` would
                # be wrong because executemany loops N sequential
                # ``_execute_async`` calls — in autocommit-default mode
                # (the documented default) each iteration commits
                # server-side independently, so retrying the entire
                # batch after a mid-loop BUSY would re-insert the
                # already-committed rows. The retry MUST be per-
                # iteration to match what stdlib ``sqlite3`` does
                # internally (the C-level ``sqlite3_busy_timeout``
                # callback fires once per statement).
                from dqlitedbapi._busy_retry import (
                    _resolve_busy_timeout_seconds,
                    retry_async_on_busy,
                )

                # Closure over per-iteration ``params`` only —
                # ``operation`` is loop-invariant. Inner function
                # (vs. bare lambda) so mypy can infer types
                # cleanly. Default-argument capture freezes
                # ``params`` so retries get the right iteration's
                # values (bare closure capture would late-bind).
                _iter_params = params

                def _execute_iter(
                    _op: str = operation,
                    _p: Sequence[Any] = _iter_params,
                ) -> Coroutine[Any, Any, None]:
                    return self._execute_async(_op, _p)

                await retry_async_on_busy(
                    _resolve_busy_timeout_seconds(self._connection),
                    _execute_iter,
                )
                acc.push(self)
                # ``push`` early-returns under tier-2 if a cascade
                # zeroed the cursor mid-iteration. Mirror by only
                # advancing the counter when the cursor is still
                # operable; otherwise the counter would overrun the
                # accumulator and the (count, anchor) invariant on
                # the BaseException arm would break.
                if not self._closed:
                    self._completed_iterations += 1
            # Modern stdlib ``sqlite3.Cursor.executemany`` (Python
            # 3.10+) updates ``lastrowid`` per iteration so the
            # final value is the LAST iteration's row id (older
            # docs claimed "left unchanged" but that was a behaviour
            # shift, not the current discipline — see CPython
            # ``Modules/_sqlite/cursor.c::execute_one_iter``).
            #
            # We clear after a SUCCESSFUL NON-EMPTY loop rather than
            # picking an arbitrary last-iteration row to surface:
            # the per-iteration writes inside ``_execute_async`` would
            # otherwise leak whichever row happened to land last to
            # the caller as if it were "the" canonical
            # last-inserted-row, and that's ambiguous. ``None``
            # forces callers to read the rowid from a single-row
            # ``execute`` before the batch (the only unambiguous
            # path). For the empty-batch case (zero iterations), no
            # per-iteration write happened — restore the pre-batch
            # snapshot so a caller's prior single-row INSERT's
            # ``lastrowid`` survives a follow-on no-op
            # ``executemany``. The sibling
            # ``_ExecuteManyAccumulator.apply()`` already special-
            # cases ``_pushed == 0`` for the rowcount=0 result; the
            # symmetric treatment here closes the lastrowid parity.
            # Empty-batch preserves; stdlib clears even on empty —
            # documented divergence in the public docstring above.
            if self._completed_iterations > 0:
                self._lastrowid = None
            else:
                self._lastrowid = lastrowid_pre_batch
        except BaseException:
            # Mid-batch failure leaves _rowcount at the last
            # iteration's value (which is misleading) and _rows /
            # _description in an inconsistent state. PEP 249 permits
            # rowcount=-1 ("undetermined"); use that signal so callers
            # cannot mistake the last iteration's rowcount for the
            # cumulative count of successfully-applied iterations.
            # ``_lastrowid`` is restored to the pre-batch snapshot so
            # the caller observes the rowid they had before
            # ``executemany`` was called — NOT whichever intra-batch
            # row ``_execute_async`` last wrote. The intent here
            # matches the stdlib ``sqlite3.Cursor.lastrowid`` contract
            # ("the rowid of the last row inserted" — and across the
            # full failed/cancelled batch, no row is the canonical
            # last-inserted-row). The original implementation
            # preserved ``self._lastrowid`` in-place which leaked the
            # intra-batch rowid; snapshotting at loop entry and
            # restoring here is the correct shape. PEP 249 §6.1.1
            # also requires messages be cleared by every cursor
            # method call; clear here so the contract holds even on
            # the BaseException re-raise. ``_completed_iterations``
            # is the observability signal for "how many iterations
            # committed before the failure"; callers reading it
            # after cancel get the count for idempotent
            # compensation. Conditional restore: the
            # ``_classify_caller_sql`` (input-validation) path is
            # already handled by the inner try/except above which
            # restores the snapshot and re-raises BEFORE control
            # reaches this arm; the outer arm here only fires when
            # the iteration loop itself raises — per-iteration
            # structural rejects (str/bytes/Mapping/set/frozenset),
            # placeholder count mismatch, or ``_execute_async``
            # raising before its ``_completed_iterations += 1``
            # runs. If the in-batch counter is still zero
            # (iteration 0 raised before its ``+= 1`` — zero in-batch
            # progress), restore the pre-batch snapshot so a caller
            # who ran ``executemany([A, B, C])`` (completed=3) then
            # triggered a per-iteration reject still observes the
            # prior batch's count. Mid-batch raises (counter > 0)
            # preserve the in-batch progress for idempotent
            # compensation.
            #
            # ``_lastrowid`` restoration is ALIGNED with
            # ``_completed_iterations``: zero in-batch progress
            # restores the pre-batch snapshot; non-zero progress
            # PRESERVES the in-batch lastrowid so the (count, anchor)
            # pair is internally consistent. A caller using
            # ``_completed_iterations`` to know "row N committed"
            # together with ``_lastrowid`` to anchor the resume
            # gets matching observability — both reflect the last
            # successful iteration's outcome, not a torn surface
            # where the count advanced past a rolled-back rowid.
            self._rowcount = -1
            self._rows = []
            self._description = None
            self._row_index = 0
            if self._completed_iterations == 0:
                self._lastrowid = lastrowid_pre_batch
                self._completed_iterations = completed_iterations_pre_batch
            del self.messages[:]
            raise
        acc.apply(self)

    def _check_result_set(self) -> None:
        if self._description is None:
            raise ProgrammingError("no results to fetch; execute a query first")

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set.

        Returns ``None`` when no more rows are available, or when no
        result set is active (DML-only / never-executed cursor).

        Stdlib parity: ``sqlite3.Cursor.fetchone()`` after a DML
        returns ``None`` rather than raising. PEP 249 §6 is silent on
        the case; stdlib's choice is the de facto contract for
        cross-driver portable code (``if cur.fetchone(): ...`` after
        DML works against stdlib, psycopg, MySQL drivers).

        ``fetchmany`` / ``fetchall`` continue to use
        ``_check_result_set`` and raise on no-result-set, matching
        stdlib's distinction.
        """
        del self.messages[:]
        # See ``execute``'s prelude comment for the ordering rationale.
        self._check_closed()
        self._connection._check_thread()
        if self._description is None:
            # No result set active (DML-only / never-executed). Match
            # stdlib by returning None rather than raising. The
            # closed-cursor case is already covered by _check_closed
            # above (raises InterfaceError per PEP 249).
            return None

        return self._next_row_unlocked()

    def _next_row_unlocked(self) -> tuple[Any, ...] | None:
        """Advance one row + apply ``row_factory`` without clearing
        ``messages`` or re-running guards.

        Used by both ``fetchone`` (after ITS prelude clear/guards) and
        ``fetchmany`` (after ITS single prelude clear/guards). PEP 249
        §6.1.1 requires the messages clear once per top-level method
        invocation, NOT once per inner row delivery.

        Snapshot ``_rows`` / ``_row_index`` / ``_row_factory`` to
        locals before the bounds check so a sibling-thread
        ``force_close_transport`` whose ``_cascade_cursors`` rewrites
        ``self._rows = []`` and ``self._row_index = 0`` between the
        bounds check and the indexed read cannot turn the indexed read
        into a bare ``IndexError`` that escapes the ``dbapi.Error``
        hierarchy. The snapshot is atomic w.r.t. the cascade: either
        the snapshot precedes the cascade (we deliver one stale row
        and the next call observes ``_closed`` via the prelude check)
        or it follows the cascade (we observe the empty list and
        return ``None`` cleanly).
        """
        rows = self._rows
        idx = self._row_index
        row_factory = self._row_factory
        if idx >= len(rows):
            return None
        row = rows[idx]
        # Apply row_factory BEFORE advancing ``_row_index`` so a raise
        # inside a custom factory leaves the index unchanged. Without
        # this ordering, ``fetchmany``'s snapshot/restore at
        # ``snapshot + len(result)`` underestimates by 1 for
        # factory-raised rows — silently REPLAYING a row on the next
        # call instead of either retrying or skipping cleanly.
        if row_factory is not None:
            try:
                transformed: tuple[Any, ...] = row_factory(self, row)
            except TypeError as exc:
                # The factory rejected ``self`` as the first argument
                # — typical when the user wired a factory that requires
                # a specific ``Cursor`` subclass (e.g. ``sqlite3.Row``,
                # whose C-extension constructor type-checks
                # ``argument 1`` to be a ``pysqlite_CursorType``).
                # Surface as ``DataError`` (PEP 249 §7 "problems with
                # the processed data") rather than leaking bare
                # ``TypeError`` past ``except dbapi.Error:`` clauses.
                raise DataError(
                    f"row_factory call failed: {exc}",
                    code=None,
                    raw_message=str(exc),
                ) from exc
            self._row_index += 1
            return transformed
        self._row_index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to ``size`` next rows of a query result.

        Returns an empty list when no more rows are available. ``size``
        defaults to ``self.arraysize``.

        Stdlib parity for the "no result set" case:
        ``sqlite3.Cursor.fetchmany()`` after a DML (no result set
        active) returns ``[]`` rather than raising, matching the
        ``fetchone`` parity already in place. Cross-driver code that
        polls ``cur.fetchmany(N) or default`` after a connect-and-
        cursor sequence works on stdlib and dqlite.

        **``size=0`` cross-driver matrix**: dqlite returns ``[]``
        deterministically without consuming rows — same as stdlib
        ``sqlite3`` on Python 3.13+ (the supported floor). The
        divergence is with psycopg3, which treats ``0`` as the
        sentinel "use ``self.arraysize``" since its default IS
        ``size: int = 0``, not ``None``. Cross-driver code ported
        from psycopg that calls ``cur.fetchmany(0)`` thinking it
        requests "default batch" gets an empty list under dqlite
        and stdlib ``sqlite3``.
        """
        del self.messages[:]
        # See ``execute``'s prelude comment for the ordering rationale.
        self._check_closed()
        self._connection._check_thread()
        if self._description is None:
            # No result set active (DML-only / never-executed). Match
            # stdlib by returning ``[]`` rather than raising. Symmetric
            # with ``fetchone`` which returns ``None`` on the same path.
            return []

        if size is None:
            size = self._arraysize
        elif not isinstance(size, int) or isinstance(size, bool):
            # Non-int / bool slip past stdlib's C-level
            # ``PyLong_AsLongAndOverflow`` and produce a bare TypeError
            # outside the dbapi.Error hierarchy. PEP 249 §7 requires
            # cursor methods to raise Error subclasses; bool is
            # rejected explicitly because ``True`` silently coerces
            # to 1 (a common caller-bug trap, not a useful affordance).
            raise ProgrammingError(f"fetchmany expects an int or None, got {type(size).__name__}")
        if size < 0:
            # Stdlib ``sqlite3.Cursor.fetchmany`` rejects negative
            # ``size`` on Python 3.13+ (``ValueError: value must be
            # positive`` on 3.13; ``ValueError: Cannot convert
            # negative int`` on 3.14). The historical "fetch all
            # remaining rows" semantic was settled under an older
            # stdlib that drained on negative; current stdlib raises.
            # PEP 249 §7 requires cursor methods to raise Error
            # subclasses; ``ProgrammingError`` keeps the failure in
            # the dbapi.Error hierarchy, matching the sibling
            # rejection on non-int / bool ``size`` at line 2148.
            raise ProgrammingError(f"fetchmany size must be non-negative; got {size}")

        # Snapshot _row_index before the loop; restore on
        # cancel/exception so partially-iterated rows are not
        # silently consumed. See aio/cursor.py for rationale.
        # Use the ``_next_row_unlocked`` helper so the per-row path
        # does not re-clear ``messages`` (PEP 249 §6.1.1: once per
        # top-level call, not once per row) or re-run the closed /
        # thread guards already validated in this method's prelude.
        snapshot = self._row_index
        result: list[tuple[Any, ...]] = []
        try:
            for _ in range(size):
                row = self._next_row_unlocked()
                if row is None:
                    break
                result.append(row)
        except BaseException:
            self._row_index = snapshot + len(result)
            raise

        return result

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result.

        Returns an empty list when the cursor has no more rows OR
        when no result set is active (DML-only / never-executed
        cursor). Stdlib parity: ``sqlite3.Cursor.fetchall()`` after
        a DML returns ``[]`` rather than raising. Symmetric with
        ``fetchone`` / ``fetchmany`` parity.
        """
        del self.messages[:]
        # See ``execute``'s prelude comment for the ordering rationale.
        self._check_closed()
        self._connection._check_thread()
        if self._description is None:
            # No result set active. Match stdlib by returning ``[]``.
            return []

        result = self._rows[self._row_index :]
        if self._row_factory is not None:
            # Apply factory BEFORE advancing ``_row_index`` so a raise
            # inside the factory leaves the index unchanged. Symmetric
            # with the discipline already in ``fetchone`` /
            # ``fetchmany`` (factory-raise → cursor index preserved →
            # next fetchone returns the same row, the desired retry
            # semantic). Without this snapshot/restore, a fetchall on
            # a row-factory that raises consumed the rows in the index
            # sense AND surfaced no rows to the caller — an asymmetry
            # vs the sibling fetch verbs.
            try:
                transformed = [self._row_factory(self, row) for row in result]
            except TypeError as exc:
                # See ``fetchone`` for rationale — wrap as DataError so
                # a factory that rejects this cursor (e.g.
                # ``sqlite3.Row``'s C-extension type check) surfaces
                # inside the PEP 249 ``Error`` hierarchy. Index is
                # NOT advanced — the snapshot-restore discipline above.
                raise DataError(
                    f"row_factory call failed: {exc}",
                    code=None,
                    raw_message=str(exc),
                ) from exc
            except BaseException:
                # Other raises (factory ValueError, programmer bugs)
                # propagate so the snapshot-restore discipline still
                # applies. Index unchanged.
                raise
            self._row_index = len(self._rows)
            return transformed
        self._row_index = len(self._rows)
        return result

    def close(self) -> None:
        """Close the cursor.

        Idempotent: a second call is a no-op. PEP 249 mandates that
        operations on a closed cursor raise an Error, but the close
        itself is permitted to be repeated.

        Does NOT enforce the Connection's thread-affinity check. Close
        is a cleanup primitive: it writes only to this cursor's own
        in-memory fields and does not touch the wire. If the enclosing
        ``with cursor() as c:`` body moves threads (``to_thread``, an
        executor callback), ``__exit__`` is still allowed to close the
        cursor without masking the body's original exception under a
        thread-check ``ProgrammingError``. Matches stdlib
        ``sqlite3.Cursor.close`` — close is always safe to call.

        **Divergence from stdlib sqlite3 on post-close attribute
        state**: this implementation scrubs ``description`` /
        ``rowcount`` / ``lastrowid`` / ``_rows`` / ``_row_index``
        for a consistent "no operation performed" surface (per
        the project rationale: avoid stale-state reads on a closed
        cursor). Stdlib ``sqlite3.Cursor.close()`` leaves
        ``description`` populated (the last query's tuple-of-7-tuples)
        and leaves ``lastrowid`` at its prior value; only ``rowcount``
        is reset to ``-1`` there. Cross-driver code that introspects
        ``cur.description`` AFTER ``close()`` (e.g. relying on
        context-manager exit to close before reading metadata) sees
        ``None`` here vs the populated tuple on stdlib. PEP 249 is
        silent on post-close attribute state; the scrub-for-consistency
        choice is deliberate and documented.

        **``arraysize`` is deliberately NOT scrubbed**: it is a
        caller-set configuration *hint* (PEP 249 §6.1.2 default ``1``;
        used by ``fetchmany()`` when ``size`` is omitted), not
        result-set state. Stdlib ``sqlite3.Cursor`` and psycopg2 both
        retain ``arraysize`` across ``close()``; this driver matches
        that parity. ``arraysize`` is therefore the single PEP 249
        §6.1.2 attribute outside the scrub set above. Cross-driver
        code that re-uses a closed cursor reference for any reason
        sees the caller-set arraysize, not the default ``1`` reset
        — by design.
        """
        # PEP 249 §6.1.2: ``Cursor.messages`` is cleared "prior to
        # executing the call" on every standard cursor method. Every
        # other method on this class clears it as the first statement;
        # close() must too. Suppress ``AttributeError`` symmetric with
        # the setter precedent (``arraysize.setter`` / ``row_factory.setter``):
        # a subclass / test fixture that strips ``messages`` must not
        # cause close() — invoked from ``__exit__`` after a body
        # exception — to raise and supplant the body's exception
        # (PEP 343 default behaviour). The SA-adapter execute finally
        # at sqlalchemy-dqlite/aio.py likewise suppresses close errors
        # so the primary execute exception wins; this is the same
        # discipline applied at the close site rather than the call
        # site.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        if self._closed:
            return
        self._closed = True
        self._rows = []
        self._description = None
        # Scrub the remaining state fields so every post-close reader
        # sees a consistent "no operation performed" surface. Prior
        # behaviour left ``_rowcount`` and ``_lastrowid`` at their
        # last-operation values — inconsistent with ``description``
        # which close() clears.
        self._rowcount = -1
        self._lastrowid = None
        # Reset ``_row_index`` too. The closed-state gate prevents any
        # accessor from reading it in practice, but leaving it at the
        # last-fetched offset contradicts the "consistent no-op
        # surface" the other scrubbed fields commit to.
        self._row_index = 0
        # Drop the strong back-reference to the parent Connection so a
        # closed cursor the user retains (debugger frame, class-level
        # cache, pytest fixture cache) does not pin the Connection
        # — and its daemon event-loop thread, ``weakref.finalize``
        # registration, and asyncio primitives — past the user's
        # intended lifetime. The connection's ``_cursors`` is already
        # a ``WeakSet`` (cursor falls out cleanly when dropped); the
        # reverse direction needs the same decoupling on close.
        # ``weakref.proxy`` preserves the public ``cursor.connection``
        # API while the Connection is alive; once the Connection is
        # actually GC'd, ``cur.connection.<anything>`` raises
        # ``ReferenceError`` — strictly better than the prior
        # behaviour where a closed cursor silently kept the daemon
        # loop thread alive forever.
        with contextlib.suppress(
            TypeError
        ):  # pragma: no cover - Connection always supports weakref
            self._connection = weakref.proxy(self._connection)

    def setinputsizes(self, sizes: Sequence[Any] | None, /) -> None:
        """Set input sizes (no-op for dqlite).

        PEP 249 §6.1.1 names ``setinputsizes`` among the methods that
        clear the ``messages`` list; we do so even though the method
        itself does no work.

        ``sizes`` accepts ``Sequence[Any]`` per PEP 249 §6.2 — items
        may be a Type Object (``STRING`` / ``BINARY`` / ``NUMBER`` /
        ``DATETIME`` / ``ROWID``), an int (max length hint), or
        ``None``. dqlite ignores the value either way; the wide
        annotation matches the spec shape.
        """
        # PEP 249 §6.1.1 — clear "prior to executing the call" so the
        # contract holds even on the cross-thread-rejection path. The
        # six primary methods (execute / executemany / fetchone /
        # fetchmany / fetchall / close) all clear before any guard for
        # the same reason; this method and its four secondary-method
        # siblings keep the same ordering.
        del self.messages[:]
        # Validate input shape at the public boundary even though the
        # body is a no-op. PEP 249 §6.2 expressly permits this method
        # to do nothing, but the project's input-validation discipline
        # (e.g. ``arraysize.setter`` rejecting ``bool``) argues for
        # raising on a misshapen call so a caller-side bug surfaces
        # at the call site instead of being silently absorbed.
        # ``str`` and ``bytes`` are ``Sequence`` instances at the ABC
        # level but are caller-bug shapes (passing a single string for
        # an N-element sizes list is the canonical mistake). Reject
        # them explicitly.
        # PEP 249 §6.2 says implementations are "free to have this
        # method do nothing" — including on closed cursors. The
        # closed short-circuit MUST run BEFORE the input-shape
        # validators so closed-state behaviour does not depend on
        # argument shape. Pre-fix, a closed cursor + good arg
        # returned silently while a closed cursor + bad arg raised
        # ``ProgrammingError`` — surprising contract.
        # Skip BOTH the closed-cursor check AND the thread check on a
        # closed cursor: ``Cursor.close()`` swaps ``self._connection``
        # for a ``weakref.proxy``; once the parent ``Connection`` is
        # GC'd, ``_connection._check_thread()`` raises
        # ``ReferenceError`` — outside the PEP 249 ``Error`` hierarchy.
        # Also short-circuit on a closed parent connection so the
        # ``_check_thread`` call doesn't run against a connection
        # mid-tear-down — symmetric with the async sibling.
        if self._closed or self._connection._closed:
            return
        # On the open-cursor path, fire thread-affinity BEFORE shape
        # validation so a cross-thread caller with a misshapen ``sizes``
        # gets the affinity diagnostic, not the shape diagnostic.
        # Mirrors the ordering of ``nextset`` / ``scroll`` /
        # ``executescript`` / ``callproc`` (check-thread before
        # input-validation). The closed-permissive-return above
        # remains BEFORE the affinity check so closed-state behaviour
        # is shape-independent: a closed cursor + good arg and a
        # closed cursor + bad arg both return silently.
        self._connection._check_thread()
        # PEP 249 §6.2: "implementations are free to have this method
        # do nothing." Stdlib ``sqlite3``, aiosqlite, psycopg, and
        # asyncpg all accept ``None`` silently. Treating ``None`` as a
        # no-op preserves cross-driver portability for the common
        # defensive idiom ``cur.setinputsizes(None)`` while keeping
        # the strict rejection below for genuinely invalid types.
        if sizes is None:
            return
        # Validate input shape — runs only after the closed-cursor
        # short-circuit and the affinity check so a closed-cursor
        # cleanup helper can call setinputsizes / setoutputsize without
        # a raise regardless of argument shape.
        if isinstance(sizes, (str, bytes, bytearray, memoryview)):
            # ``memoryview`` satisfies ``collections.abc.Sequence`` so
            # it would slip past the explicit-rejection arm and reach
            # the ABC arm below. Quartet-rejection keeps this validator
            # aligned with the sibling ``_reject_non_sequence_params``
            # (str/bytes/bytearray/memoryview).
            raise ProgrammingError(
                f"setinputsizes expects a sequence of size hints, got {type(sizes).__name__}"
            )
        if not isinstance(sizes, Sequence):
            # PEP 249 §6.2: ``sizes`` is "specified as a sequence".
            # PEP 249 §7 requires every error originating from the
            # driver to be a subclass of ``Error``. Bare ``TypeError``
            # would escape ``except dbapi.Error:`` in cross-driver
            # code; wrap as ``ProgrammingError`` to match the rest of
            # the validator surface. Loosened from ``(list, tuple)``
            # to the structural ``Sequence`` ABC so cross-driver
            # callers passing a ``deque`` / ``range`` / custom Sequence
            # subclass — accepted by stdlib + psycopg2 — work here too.
            raise ProgrammingError(f"setinputsizes expects a Sequence, got {type(sizes).__name__}")

    def setoutputsize(self, size: int | None, column: int | None = None, /) -> None:
        """Set output size (no-op for dqlite). See ``setinputsizes``."""
        del self.messages[:]
        # PEP 249 §6.2 — see ``setinputsizes`` rationale. Closed
        # short-circuit runs BEFORE the validators so closed-state
        # behaviour is independent of argument shape. Symmetric with
        # the async sibling.
        if self._closed or self._connection._closed:
            return
        # Affinity-before-shape on the open-cursor path; see
        # ``setinputsizes`` for the full rationale.
        self._connection._check_thread()
        # PEP 249 §6.2: "implementations are free to have this method
        # do nothing." Stdlib ``sqlite3``, aiosqlite, psycopg, and
        # asyncpg all accept ``None`` silently. Treating ``None`` as
        # a no-op preserves cross-driver portability for the common
        # defensive idiom ``cur.setoutputsize(None)`` while keeping
        # the strict rejection below for genuinely invalid types.
        # Mirrors the existing ``setinputsizes`` discipline.
        if size is None:
            return
        # Validate input shape — see ``setinputsizes`` rationale.
        # ``ProgrammingError`` keeps the failure inside the
        # ``dbapi.Error`` hierarchy per PEP 249 §7.
        if not isinstance(size, int) or isinstance(size, bool):
            raise ProgrammingError(f"setoutputsize expects an int, got {type(size).__name__}")
        if column is not None and (not isinstance(column, int) or isinstance(column, bool)):
            raise ProgrammingError(
                f"setoutputsize column expects an int or None, got {type(column).__name__}"
            )

    def callproc(self, procname: str, parameters: Sequence[Any] | None = None, /) -> NoReturn:
        """PEP 249 optional extension — not supported.

        dqlite (and SQLite) have no stored-procedure concept. Annotated
        ``NoReturn`` because the body unconditionally raises
        ``NotSupportedError`` — symmetric with ``nextset`` below.

        **Note for cross-driver code porting from stdlib ``sqlite3``:**
        ``hasattr(cur, "callproc")`` returns ``True`` here because the
        method is defined (it just unconditionally raises). Stdlib
        ``sqlite3.Cursor`` has no ``callproc`` / ``nextset`` / ``scroll``
        attributes, so ``hasattr`` returns ``False`` there. Use
        ``try / except NotSupportedError`` for portable feature-
        detection; ``hasattr`` is misleading on this driver.
        """
        # PEP 249 §6.1.1 names ``callproc`` among the cursor methods
        # that clear ``Connection.messages`` / ``Cursor.messages``.
        # Clear before any guard so the contract holds even on the
        # cross-thread-rejection path. Mirrors ``nextset`` below.
        del self.messages[:]
        # PEP 249 §6.1.2 — closed-cursor ops raise. ``_check_closed``
        # BEFORE ``_check_thread``: see ``execute``'s prelude comment
        # for the GC'd-proxy ``ReferenceError`` rationale.
        self._check_closed()
        self._connection._check_thread()
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> NoReturn:
        """PEP 249 optional extension — not supported.

        dqlite's wire protocol does not return multiple result sets.
        """
        # PEP 249 §6.1.1 names ``nextset`` among the cursor methods
        # that clear ``Connection.messages``; clear before any guard
        # so the contract holds even on the cross-thread-rejection
        # path.
        del self.messages[:]
        # PEP 249 §6.1.2 — closed-cursor operations raise.
        # ``_check_closed`` first; see ``execute`` for rationale.
        self._check_closed()
        self._connection._check_thread()
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative", /) -> NoReturn:
        """PEP 249 optional extension — not supported.

        The dqlite cursor is forward-only; rows are buffered from a
        streamed wire response.
        """
        # PEP 249 §6.1.1 lists ``nextset`` but not ``scroll`` in the
        # explicit-clear set; we clear here too for sibling consistency
        # with ``nextset`` / ``callproc`` / ``setinputsizes`` /
        # ``setoutputsize``. Cheap and removes a latent foot-gun for
        # future code that starts populating ``messages``. Order
        # matches the secondary-method family: clear before any guard.
        del self.messages[:]
        # ``_check_closed`` first; see ``execute`` for rationale.
        self._check_closed()
        self._connection._check_thread()
        # PEP 249 §6.1.1 documents ``value`` as an integer offset.
        # Validate the value-type alongside the mode enumeration so the
        # "surfaces as a caller-side bug" argument applies symmetrically
        # to both parameters. ``bool`` is-a ``int`` in Python; explicit
        # rejection matches the project standard from
        # ``arraysize.setter``.
        if not isinstance(value, int) or isinstance(value, bool):
            raise ProgrammingError(
                f"scroll value must be an integer offset, got {type(value).__name__}"
            )
        # PEP 249 §6.1.1 enumerates ``mode`` ∈ {"relative", "absolute"}.
        # Validate before the unconditional NotSupportedError so a
        # caller typo (``cur.scroll(5, "absolutely")``) surfaces as a
        # caller-side bug rather than being masked by the same
        # diagnostic a correct call would produce. ProgrammingError
        # stays inside the dbapi.Error hierarchy.
        if mode not in ("relative", "absolute"):
            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', got {mode!r}")
        raise NotSupportedError("dqlite cursors are not scrollable")

    def executescript(self, sql_script: str, /) -> NoReturn:
        """stdlib ``sqlite3.Cursor``-parity stub. dqlite has no
        multi-statement-script primitive on the wire; raises
        ``NotSupportedError`` rather than escaping ``dbapi.Error``
        as ``AttributeError``. Same shape as the
        ``Connection.executescript`` stub."""
        del self.messages[:]
        # ``_check_closed`` first; see ``execute`` for rationale.
        self._check_closed()
        self._connection._check_thread()
        raise NotSupportedError(
            "dqlite does not support stdlib sqlite3 executescript; "
            "split the script and execute each statement individually"
        )

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        # Include the parent connection's address and ``id(self)`` so
        # the repr is self-disambiguating in logs that fan multiple
        # cursors across pooled connections. ``getattr`` with ``'?'``
        # fallback tolerates mock-backed test fixtures whose stub
        # connection lacks ``_address``. Route through
        # ``sanitize_for_log`` for sibling-discipline parity with the
        # client / dbapi ``Connection`` reprs: attacker-influenced
        # bidi / zero-width / line-separator codepoints render as
        # ``?`` rather than ``\uXXXX``, matching everywhere else the
        # address appears in logs.
        # Defence against ``ReferenceError``: ``close()`` swaps
        # ``self._connection`` to a ``weakref.proxy``. If the parent
        # ``Connection`` is GC'd thereafter, every attribute access
        # on the proxy raises ``ReferenceError`` BEFORE the
        # ``getattr`` default is consulted. ``ReferenceError`` is
        # outside the ``dbapi.Error`` hierarchy — every
        # ``except dbapi.Error:`` block misses it, and ``repr()``
        # crashes debugging tooling. Fall back to ``'?'`` (matching
        # the existing missing-attribute placeholder) so ``repr()``
        # stays safe under the GC'd-proxy condition. Mirrors the
        # sibling ``Cursor.connection`` property's discipline.
        try:
            address = sanitize_for_log(str(getattr(self._connection, "_address", "?")))
        except ReferenceError:
            address = "?"
        return f"<Cursor address={address!r} rowcount={self._rowcount} {state} at 0x{id(self):x}>"

    def __reduce__(self) -> NoReturn:
        # Cursors hold a back-reference to a Connection that owns a
        # live socket and an event-loop thread; none of that survives
        # pickling. Surface a clear driver-level TypeError instead of
        # leaking the underlying ``cannot pickle '_thread.lock'``
        # message that the default pickle walk produces.
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — cursors "
            "hold a reference to a live driver Connection; use "
            "fetchall()/fetchmany() to materialise rows before crossing "
            "a process boundary"
        )

    def __iter__(self) -> Self:
        # PEP 249 §6.4 messages-clear contract: every public cursor
        # method clears ``messages`` "prior to executing the call".
        # ``__iter__`` is the iter-protocol entry point; sibling no-op
        # methods (``nextset`` / ``callproc`` / ``scroll`` /
        # ``setinputsizes`` / ``setoutputsize``) all clear first.
        # ``__next__`` itself dispatches to ``fetchone`` (which clears
        # at its own entry, including on the terminating call that
        # returns None and surfaces ``StopIteration``), so the iter-
        # protocol's clear obligation is satisfied at every step. The
        # clear here is for the ``iter(cur)`` entry point itself —
        # generic consumer code that calls ``iter(cur)`` and then never
        # advances would otherwise observe stale messages from a prior
        # operation on the cursor.
        #
        # Cross-thread affinity is verified by the first ``__next__``
        # call (which dispatches to ``fetchone`` → ``_check_thread``),
        # not at iter() time, mirroring stdlib ``sqlite3.Cursor.__iter__``
        # which is also a bare ``return self``. The async sibling
        # ``AsyncCursor.__aiter__`` deliberately fails fast at iter
        # time because ``async for`` is the only common idiom that
        # crosses event loops; the divergence is rooted in the lazy
        # loop-bind contract and PEP 234's ``iter(x) is x`` requirement
        # on closed iterables.
        del self.messages[:]
        return self

    def __next__(self) -> tuple[Any, ...]:
        """Advance the cursor by one row.

        **Row-factory raise behaviour (divergence from stdlib
        ``sqlite3`` convention):** if a custom ``row_factory`` raises
        a non-``StopIteration`` exception while transforming the next
        row, the cursor's row index is NOT advanced. A subsequent
        ``__next__`` will retry the SAME row, re-running the factory
        and re-raising. This is deliberate — ``fetchmany``'s
        snapshot/restore retry semantic (``snapshot + len(result)``
        on cancel) requires the un-delivered row to remain
        pointed-to. The shared helper ``_next_row_unlocked`` applies
        the factory BEFORE advancing the index so a raise leaves the
        index unchanged, and the iterator path inherits that
        property.

        To skip past a bad row, drop and re-fetch via a fresh
        ``execute``, or wrap the iteration in a try/except that
        breaks out on the raise. The cursor is NOT "terminated" by a
        factory raise — it is "wedged on the bad row" until the next
        ``execute`` resets the row buffer. The async sibling
        ``AsyncCursor.__anext__`` has the same shape for sync/async
        consistency.
        """
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Self:
        # PEP 249 §6.4 messages-clear contract — unconditional,
        # mirroring the sibling __iter__ above which also clears
        # regardless of _closed state. Every secondary entry point
        # in the cursor surface (nextset / callproc / scroll /
        # setinputsizes / setoutputsize / __iter__ / __aiter__ /
        # __aenter__) clears unconditionally; the closed-cursor case
        # is admitted because a future driver path that appends to
        # messages from a cross-thread background producer must not
        # be observed by ``with cur:`` on a closed cursor.
        del self.messages[:]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
