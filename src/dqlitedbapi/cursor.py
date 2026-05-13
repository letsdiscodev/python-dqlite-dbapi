"""PEP 249 Cursor implementation for dqlite."""

import contextlib
import re
import weakref
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence, Sized
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, NoReturn, Protocol, Self

import dqliteclient.exceptions as _client_exc
import dqlitewire.exceptions as _wire_exc
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
    _convert_bind_param,
    _datetime_from_iso8601,
    _datetime_from_unixtime,
    _Description,
)
from dqlitewire.constants import (
    DQLITE_NOTFOUND,
    DQLITE_PARSE,
    DQLITE_PROTO,
    SQLITE_AUTH,
    SQLITE_CONSTRAINT,
    SQLITE_CORRUPT,
    SQLITE_FORMAT,
    SQLITE_INTERNAL,
    SQLITE_MISMATCH,
    SQLITE_MISUSE,
    SQLITE_NOLFS,
    SQLITE_NOMEM,
    SQLITE_NOTADB,
    SQLITE_NOTFOUND,
    SQLITE_NOTICE,
    SQLITE_RANGE,
    SQLITE_TOOBIG,
    SQLITE_WARNING,
    ValueType,
    primary_sqlite_code,
)

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
_CODE_TO_EXCEPTION: dict[
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
] = {
    SQLITE_CONSTRAINT: IntegrityError,
    SQLITE_INTERNAL: InternalError,
    SQLITE_TOOBIG: DataError,
    SQLITE_MISMATCH: IntegrityError,
    SQLITE_RANGE: InterfaceError,
    SQLITE_MISUSE: InterfaceError,
    SQLITE_NOTFOUND: InternalError,
    SQLITE_NOMEM: InternalError,
    SQLITE_CORRUPT: DatabaseError,
    SQLITE_FORMAT: DatabaseError,
    SQLITE_NOTADB: DatabaseError,
    # Codes that intentionally fall through to the OperationalError
    # default (no explicit entry needed): BUSY, LOCKED, READONLY,
    # IOERR, FULL, CANTOPEN, EMPTY, SCHEMA, PROTOCOL (15), PERM,
    # ABORT, INTERRUPT, ERROR. These are stdlib's OperationalError
    # codes; we used to enumerate PROTOCOL explicitly as
    # "documentary" but that just invited symmetry pressure to add
    # 12 more no-op entries.
    # CPython stdlib parity — see the primary-code constants above.
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
            f"Cluster policy rejection; {e}",
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
    except _wire_exc.EncodeError as e:
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
    except (TypeError, ValueError) as e:
        # PEP 249 §7 mandates ``DataError`` for "problems with the
        # processed data". The wire encoder in ``dqlitewire.types``
        # raises ``TypeError`` / ``ValueError`` when a bind parameter is
        # not one of the accepted primitives (bool/int/float/str/bytes/
        # None). Without this wrap, those exceptions leak past ``except
        # dqlitedbapi.Error`` boundaries. ``_convert_bind_param`` handles
        # datetime / date / time up front; everything else — Decimal,
        # UUID, Path, Enum, arbitrary user classes — reaches the wire
        # encoder and lands here. Callers who want to support those
        # types should register an adapter (stdlib sqlite3 convention).
        raise DataError(f"cannot bind parameter: {e}", code=None, raw_message=str(e)) from e


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
_RESULT_CONVERTERS: dict[int, Callable[[Any], Any]] = {
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
_SQL_NOISE_RE = re.compile(
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
        # Mappings, str, bytes are rejected by the binding layer
        # later — skip the count check for them since len() doesn't
        # mean what we want.
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
        self._pushed += 1
        if cursor._description is not None:
            # Row-returning iteration: total_affected is the count of
            # rows emitted, which today matches ``len(cursor._rows)``.
            # Using ``len()`` makes the invariant explicit and survives
            # any future decoupling of rowcount semantics on the
            # RETURNING path.
            if self.description is None:
                self.description = cursor._description
            self.rows.extend(cursor._rows)
            self.total_affected += len(cursor._rows)
            if self._max_rows is not None and len(self.rows) > self._max_rows:
                raise DataError(
                    f"executemany accumulated {len(self.rows)} RETURNING rows; "
                    f"exceeds max_total_rows={self._max_rows}"
                )
        elif cursor._rowcount >= 0:
            # Plain DML iteration: ``_rowcount`` is the server's
            # sqlite3_changes() for this parameter set.
            self.total_affected += cursor._rowcount

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
        # the parent Connection's default factory if set. The class-
        # name check below restricts inheritance to real Connection
        # instances — MagicMock-typed test fakes have an auto-magic
        # ``_row_factory`` attribute that would otherwise silently
        # wrap every row. Use class-name comparison rather than
        # ``isinstance`` to avoid the cursor → connection import
        # cycle.
        self._row_factory: Any = (
            getattr(connection, "_row_factory", None)
            if type(connection).__name__ == "Connection"
            else None
        )
        # PEP 249 optional extension. Currently no driver path appends
        # to this list; it's here so consumers can rely on the
        # attribute existing and being mutable.
        self.messages: list[tuple[type[Exception], Exception | str]] = []

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
        """
        try:
            # Touch any attribute to force the proxy to resolve. If
            # the underlying Connection has been GC'd, this raises
            # ReferenceError; otherwise the resolved object is the
            # live Connection.
            _ = self._connection.address
        except ReferenceError as e:
            raise InterfaceError("Cursor's parent Connection has been garbage-collected") from e
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
        """
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
        # rather than silently changing the creator-thread's next
        # ``fetchmany`` size.
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
    def row_factory(self) -> Any:
        """stdlib ``sqlite3.Cursor.row_factory`` parity hook.

        Set to a callable ``factory(cursor, row) -> Any`` to wrap each
        fetched tuple before returning. ``None`` (default) returns
        plain tuples per PEP 249. Common factories:

        - ``sqlite3.Row`` (stdlib): tuple-like with index AND
          column-name access.
        - ``lambda cur, row: dict(zip([c[0] for c in cur.description], row))``:
          dict-of-column-name-to-value.

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

    def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> Self:
        """Execute a database operation (query or command).

        Returns ``self`` so callers can chain ``.fetchall()`` etc.
        """
        del self.messages[:]
        # PEP 249 §7: errors raised by the module subclass ``Error``.
        # A non-str ``operation`` would later raise bare ``AttributeError``
        # (``None.lstrip``) or ``TypeError`` (``bytes.lstrip("﻿")``)
        # from ``_strip_leading_comments`` inside ``_classify_caller_sql``,
        # escaping the dbapi exception hierarchy. Symmetric with the
        # ``executemany() seq_of_parameters=None`` guard below: surface
        # ``ProgrammingError`` up front so cross-driver
        # ``except dbapi.Error`` clauses catch the misuse.
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )
        # ``_check_closed`` BEFORE ``_check_thread``: ``Cursor.close()``
        # swaps ``self._connection`` for a ``weakref.proxy``; once the
        # parent ``Connection`` is GC'd, ``_connection._check_thread()``
        # raises ``ReferenceError`` — outside the PEP 249 ``Error``
        # hierarchy. ``_check_closed`` reads only ``self._closed`` and
        # raises ``InterfaceError`` (a real ``Error`` subclass).
        self._check_closed()
        self._connection._check_thread()
        # Clear after the guards but before the wire call so a caller
        # who executes on a closed cursor still sees the sharp
        # ``InterfaceError("Cursor is closed")`` without a state-clobber
        # side effect; a caller whose call raises mid-execute sees a
        # cursor in the "no result set" baseline, not one reporting the
        # previous query's description / rows.
        self._reset_execute_state()

        # Pre-flight classification pass: empty SQL → ProgrammingError
        # (PEP 249 §7), multi-statement → ProgrammingError (stdlib
        # parity), wrong ``?``-count vs ``len(parameters)`` →
        # ProgrammingError. All three skip the wire round-trip so a
        # caller bug surfaces with the right class at the user's
        # call site rather than as ``OperationalError`` (server
        # rejection) or silent data loss (multi-statement drop).
        _classify_caller_sql(operation, parameters)

        self._connection._run_sync(self._execute_async(operation, parameters))
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
                # equal to one of Type Objects"; ``None`` does not.
                # We emit ``None`` here as a documented deviation:
                # any synthesised value (e.g. always TEXT) would be
                # misleading in a different direction (a SELECT
                # against an INTEGER column with no rows would
                # advertise the wrong type), and a wire extension
                # is a feature outside the scope of dbapi
                # correctness. Callers that need column-type
                # introspection on empty result sets should issue a
                # PRAGMA table_info(...) query separately. For the
                # real anomaly (rows present but short
                # ``column_types``), raise ``DataError`` so the
                # wire bug surfaces loudly.
                if len(column_types) == 0 and len(rows) == 0:
                    type_codes: list[int | None] = [None] * len(columns)
                elif len(column_types) != len(columns):
                    raise DataError(
                        f"Wire response has {len(columns)} columns but "
                        f"{len(column_types)} type codes"
                    )
                else:
                    # Map ValueType.NULL (5) to None: PEP 249 §6.1.2
                    # says type_code "must compare equal to one of
                    # Type Objects defined below" and NULL is not one
                    # of the five Type Objects (STRING / BINARY /
                    # NUMBER / DATETIME / ROWID). Surfacing None
                    # instead matches the documented empty-result-set
                    # deviation already in this module.
                    type_codes = [None if c == ValueType.NULL else int(c) for c in column_types]
                self._description = tuple(
                    (name, type_codes[i], None, None, None, None, None)
                    for i, name in enumerate(columns)
                )
            # Per-row dispatch: SQLite's dynamic typing means two rows in
            # the same column can carry different wire types. Use
            # ``row_types[i]`` rather than ``column_types`` so a row
            # whose wire type diverges from row 0 is decoded correctly.
            self._rows = [
                _convert_row(row, row_types[i] if i < len(row_types) else column_types)
                for i, row in enumerate(rows)
            ]
            self._row_index = 0
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
            self._rowcount = _to_signed_int64(affected)
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

        ``lastrowid`` semantics: **divergence from stdlib
        ``sqlite3.Cursor``.** stdlib's documented contract is
        "``lastrowid`` is left unchanged after executemany" — the
        prior INSERT's id stays sticky across the batch. This driver
        clears ``lastrowid`` to ``None`` after a successful batch
        (including empty batches): the batch's ambiguity (which of N
        inserts is "the" rowid?) makes any sticky value misleading,
        so we surface ``None`` to force callers to read the rowid from
        a single-row INSERT before the batch. Cross-driver code that
        relies on the stdlib "left unchanged" contract should not call
        executemany on this driver to obtain a rowid; use a single-row
        execute instead, or read the id from RETURNING rows.

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
        # PEP 249 §7: errors raised by the module subclass ``Error``.
        # ``seq_of_parameters=None`` would later leak a bare ``TypeError``
        # ("'NoneType' object is not iterable") from the iteration site,
        # escaping the dbapi exception hierarchy. ``None`` for the outer
        # iterable has no defensible "no params" reading (unlike
        # ``execute(sql, None)``); mirror the project's existing strict
        # input-validation discipline (str/bytes/Mapping/set rejection)
        # and surface ``ProgrammingError`` up front. Same treatment in
        # the async sibling.
        if seq_of_parameters is None:
            raise ProgrammingError(
                "executemany() seq_of_parameters must be a sequence/iterable, not None",
                code=None,
            )
        # PEP 249 §7: surface non-str ``operation`` as a ``dbapi.Error``
        # subclass up front so cross-driver ``except dbapi.Error:`` catches
        # the misuse. Without this guard, downstream calls (e.g.
        # ``_strip_leading_comments(operation)``) raise bare
        # ``TypeError`` / ``AttributeError`` that escape the hierarchy.
        # Mirrors the canonical sibling guard on ``Cursor.execute``.
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )
        # ``_lastrowid`` clear is intentionally deferred until AFTER the
        # verb-rejection guards below. A rejected ``executemany`` (a
        # transaction-control verb, a row-returning shape) means no
        # batch ran — clearing here would clobber the prior INSERT's
        # rowid, violating both the cursor docstring contract
        # ("ROLLBACK / UPDATE / DELETE / DDL do NOT clear it... close()
        # is the single lifecycle event that scrubs it") and parity with
        # the async sibling's rejection path. The post-loop clear at the
        # end of ``_executemany_async`` handles the documented "clear
        # after success" contract for the admitted-verb path.
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
            head_upper = operation.lstrip().upper()
            if head_upper.startswith("PRAGMA"):
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
        # Hoist ``_classify_caller_sql`` ONCE at the top: empty SQL,
        # multi-statement, NUL-in-SQL, and the SQL parsing/scanning
        # are all invariant across iterations. The placeholder count
        # derived here is then compared per-iteration against
        # ``len(params)`` — a cheap O(1) check vs the per-iteration
        # regex traversal that calling the full classifier per row
        # would cost. ``skip_param_count_check=True`` skips the
        # placeholder length-check (the classifier still runs the
        # empty/multi/NUL guards, which are pure SQL parses).
        _classify_caller_sql(operation, None, skip_param_count_check=True)
        cleaned = _strip_sql_noise(operation)
        placeholder_count = cleaned.count("?")
        # Single source of truth for per-execute reset; see
        # ``_reset_execute_state``. Also zeroes ``_rowcount`` to -1 so
        # an empty ``seq_of_parameters`` ends with the same
        # ``rowcount`` shape as empty ``execute``.
        self._reset_execute_state()
        # Reset the per-call completed-iteration counter. After a
        # successful executemany, equals len(seq_of_parameters).
        # After cancel / mid-batch raise, retains the count of
        # iterations that already committed server-side — observability
        # signal for idempotent compensation.
        self._completed_iterations = 0
        acc = _ExecuteManyAccumulator(max_rows=self._connection._max_total_rows)
        try:
            for params in seq_of_parameters:
                # Per-iteration ``?``-count vs ``len(params)``. Mappings,
                # str, bytes are rejected by the binding layer later —
                # skip the count check for them since len() doesn't
                # mean what we want. Mirrors ``_classify_caller_sql``'s
                # late check on a per-row basis.
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
                await self._execute_async(operation, params)
                acc.push(self)
                self._completed_iterations += 1
            # stdlib ``sqlite3.Cursor.executemany`` does NOT update
            # ``lastrowid`` — the value reflects no single row across
            # the batch and is "left unchanged" per the docs. We clear
            # after a successful loop (including the empty-batch case)
            # so per-iteration writes inside ``_execute_async`` don't
            # leak the last batch row's id to the caller. Keeps cross-
            # driver code that reads ``cur.lastrowid`` after executemany
            # observing ``None``. Rejection paths in the sync wrapper
            # short-circuit before reaching this point and preserve the
            # prior ``lastrowid`` per the docstring contract.
            self._lastrowid = None
        except BaseException:
            # Mid-batch failure leaves _rowcount at the last
            # iteration's value (which is misleading) and _rows /
            # _description in an inconsistent state. PEP 249 permits
            # rowcount=-1 ("undetermined"); use that signal so callers
            # cannot mistake the last iteration's rowcount for the
            # cumulative count of successfully-applied iterations.
            # ``_lastrowid`` is intentionally NOT reset — stdlib
            # ``sqlite3.Cursor.lastrowid`` is documented as "the
            # rowid of the last row inserted" and is NOT cleared by
            # a failed/cancelled subsequent operation. A user who
            # saw ``cur.lastrowid`` after an INSERT, then ran an
            # ``executemany`` that failed mid-batch, should still see
            # the prior INSERT's rowid (per the lastrowid property
            # docstring: "close() is the single lifecycle event that
            # scrubs it"). PEP 249 §6.1.1 also requires messages be
            # cleared by every cursor method call; clear here so the
            # contract holds even on the BaseException re-raise.
            # ``_completed_iterations`` is intentionally PRESERVED
            # — it's the observability signal for "how many iterations
            # committed before the failure"; callers reading it after
            # cancel get the count for idempotent compensation.
            self._rowcount = -1
            self._rows = []
            self._description = None
            self._row_index = 0
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
        """
        if self._row_index >= len(self._rows):
            return None
        row = self._rows[self._row_index]
        # Apply row_factory BEFORE advancing ``_row_index`` so a raise
        # inside a custom factory leaves the index unchanged. Without
        # this ordering, ``fetchmany``'s snapshot/restore at
        # ``snapshot + len(result)`` underestimates by 1 for
        # factory-raised rows — silently REPLAYING a row on the next
        # call instead of either retrying or skipping cleanly.
        if self._row_factory is not None:
            transformed: tuple[Any, ...] = self._row_factory(self, row)
            self._row_index += 1
            return transformed
        self._row_index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to ``size`` next rows of a query result.

        Returns an empty list when no more rows are available. ``size``
        defaults to ``self.arraysize``.

        Stdlib parity: ``sqlite3.Cursor.fetchmany()`` after a DML
        (no result set active) returns ``[]`` rather than raising,
        matching the ``fetchone`` parity already in place. Cross-
        driver code that polls ``cur.fetchmany(N) or default`` after
        a connect-and-cursor sequence works on stdlib and dqlite.

        **Divergence from psycopg3**: an explicit ``size=0`` returns
        ``[]`` here (stdlib ``sqlite3`` parity). psycopg3 treats
        ``size=0`` as the sentinel meaning "use ``self.arraysize``"
        (its default IS ``size: int = 0``, not ``None``). Cross-
        driver code ported from psycopg that calls
        ``cur.fetchmany(0)`` thinking it requests "default batch"
        gets an empty list under dqlite. Pass ``None`` or omit
        ``size`` to default to ``self.arraysize``.
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
            # Stdlib ``sqlite3.Cursor.fetchmany`` documents negative
            # ``size`` as "fetch all remaining rows"; mirror that
            # behaviour for cross-driver parity. The ``arraysize``
            # SETTER still validates >= 1 (it's a stored-state
            # invariant), but the per-call argument follows stdlib.
            return self.fetchall()

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
            except BaseException:
                # Index unchanged; raise propagates the factory error.
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
        # close() must too.
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

    def setinputsizes(self, sizes: Sequence[Any]) -> None:
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
        # remains BEFORE the affinity check (closed-state is shape-
        # independent — see the sibling ``done/`` issue that fixed the
        # closed-vs-validator ordering).
        self._connection._check_thread()
        # Validate input shape — runs only after the closed-cursor
        # short-circuit and the affinity check so a closed-cursor
        # cleanup helper can call setinputsizes / setoutputsize without
        # a raise regardless of argument shape.
        if isinstance(sizes, (str, bytes, bytearray)):
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

    def setoutputsize(self, size: int, column: int | None = None) -> None:
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
        # Validate input shape — see ``setinputsizes`` rationale.
        # ``ProgrammingError`` keeps the failure inside the
        # ``dbapi.Error`` hierarchy per PEP 249 §7.
        if not isinstance(size, int) or isinstance(size, bool):
            raise ProgrammingError(f"setoutputsize expects an int, got {type(size).__name__}")
        if column is not None and (not isinstance(column, int) or isinstance(column, bool)):
            raise ProgrammingError(
                f"setoutputsize column expects an int or None, got {type(column).__name__}"
            )

    def callproc(self, procname: str, parameters: Sequence[Any] | None = None) -> NoReturn:
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

    def scroll(self, value: int, mode: str = "relative") -> NoReturn:
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
        # connection lacks ``_address``.
        address = getattr(self._connection, "_address", "?")
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
        # Without this, a future driver path that populates messages
        # would let ``for row in cur:`` observe stale messages on an
        # empty result set (``__next__`` raises ``StopIteration``
        # without calling ``fetchone``'s clear).
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
