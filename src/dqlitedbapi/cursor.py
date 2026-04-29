"""PEP 249 Cursor implementation for dqlite."""

import re
from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, NoReturn, Protocol

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
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
    ValueType,
    primary_sqlite_code,
)
from dqlitewire.constants import SQLITE_CORRUPT as _SQLITE_CORRUPT
from dqlitewire.constants import SQLITE_FORMAT as _SQLITE_FORMAT
from dqlitewire.constants import SQLITE_NOTADB as _SQLITE_NOTADB

__all__ = ["Cursor"]


# SQLite primary error code 19 (SQLITE_CONSTRAINT) plus its extended
# family (SQLITE_CONSTRAINT_CHECK = 275, UNIQUE = 2067, NOT_NULL = 1299,
# FOREIGN_KEY = 787, etc.) all share ``code & 0xFF == 19``. PEP 249
# mandates IntegrityError for these, so map them here rather than
# leaving every caller to inspect the code themselves.
_SQLITE_CONSTRAINT: Final[int] = 19

# SQLite primary error code 2 (SQLITE_INTERNAL). stdlib ``sqlite3``
# routes this to ``sqlite3.InternalError`` (CPython's
# ``_pysqlite_seterror``); PEP 249 defines ``InternalError`` for exactly
# this purpose — "internal errors of the database, e.g. the cursor is
# not valid anymore".
_SQLITE_INTERNAL: Final[int] = 2

# SQLITE_TOOBIG (18) — "value exceeds size limit" — is the canonical
# PEP 249 ``DataError`` case ("problems with the processed data").
_SQLITE_TOOBIG: Final[int] = 18

# SQLITE_MISMATCH (20) — datatype mismatch on STRICT tables. CPython
# stdlib ``sqlite3`` (``Modules/_sqlite/util.c::_pysqlite_seterror``)
# groups this with ``SQLITE_CONSTRAINT`` under ``IntegrityError``;
# ``aiosqlite`` inherits. Both ``DataError`` and ``IntegrityError``
# are defensible PEP 249 readings for STRICT-table datatype mismatch,
# but callers porting between stdlib and dqlite expect the stdlib
# grouping — so align to it.
_SQLITE_MISMATCH: Final[int] = 20

# SQLITE_RANGE (25) — "bind index out of range" — is a caller-side
# parameter-binding error; PEP 249 ``ProgrammingError`` is the right
# fit ("bad parameter ... wrong number of parameters") rather than
# ``DataError``.
_SQLITE_RANGE: Final[int] = 25

# SQLITE_NOMEM (7) — server-side allocation failure. CPython stdlib
# ``sqlite3`` raises ``MemoryError`` (system-level, bypasses PEP 249);
# we route through ``InternalError`` so callers stay inside the
# PEP 249 hierarchy and ``except dbapi.Error:`` continues to catch.
_SQLITE_NOMEM: Final[int] = 7

# SQLITE_CORRUPT (11), SQLITE_FORMAT (24), SQLITE_NOTADB (26) — the
# server-side database file is malformed / wrong format / not a
# SQLite database. CPython routes all three to ``DatabaseError``
# (the umbrella PEP 249 class). Callers porting between stdlib and
# dqlite use ``except DatabaseError:`` to handle these uniformly.
# The constants are imported from ``dqlitewire.constants`` (alongside
# the other SQLite primaries the wire layer already exports) so the
# SA dialect, the dbapi, and any future caller all reference the
# same source of truth.

# SQLITE_PROTOCOL (15) — file-locking protocol error inside SQLite's
# WAL machinery. CPython routes to ``OperationalError``; we already
# default unmapped codes to OperationalError so this entry is
# documentary — it pins the contract so a future audit shows the
# code was considered.
_SQLITE_PROTOCOL: Final[int] = 15

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
    _SQLITE_CONSTRAINT: IntegrityError,
    _SQLITE_INTERNAL: InternalError,
    _SQLITE_TOOBIG: DataError,
    _SQLITE_MISMATCH: IntegrityError,
    _SQLITE_RANGE: ProgrammingError,
    _SQLITE_NOMEM: InternalError,
    _SQLITE_CORRUPT: DatabaseError,
    _SQLITE_FORMAT: DatabaseError,
    _SQLITE_NOTADB: DatabaseError,
    _SQLITE_PROTOCOL: OperationalError,
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


async def _call_client[T](coro: Coroutine[Any, Any, T]) -> T:
    """Await a client-layer coroutine, mapping its exceptions into the
    PEP 249 hierarchy. Preserves the original via ``from``.

    Mapping:
      client.OperationalError (constraint code) → dbapi.IntegrityError
      client.OperationalError (other codes)     → dbapi.OperationalError
      client.DqliteConnectionError → dbapi.OperationalError (network flavor)
      client.ClusterError          → dbapi.OperationalError
      client.ProtocolError         → dbapi.InterfaceError
      client.DataError             → dbapi.DataError
      client.InterfaceError        → dbapi.InterfaceError
      any other DqliteError        → dbapi.InterfaceError

    Every ``dqliteclient`` exception is a subclass of ``DqliteError``;
    the trailing catch-all ensures a new client exception class cannot
    bypass PEP 249 wrapping. PEP 249 requires all database-sourced
    errors to surface as ``Error`` subclasses — without the fallback, a
    future ``dqliteclient.CircuitOpenError`` or similar would leak past
    ``except dqlitedbapi.Error`` boundaries.
    """
    try:
        return await coro
    except _client_exc.OperationalError as e:
        # Classify by SQLite extended error code. Constraint violations
        # (primary code 19) become IntegrityError per PEP 249; everything
        # else stays OperationalError so callers that branch on
        # leader-change / busy codes (``_is_no_transaction_error``, the
        # SQLAlchemy dialect's ``is_disconnect``) continue to work.
        # ``e.message`` (not ``str(e)``) — client.OperationalError's
        # ``__str__`` prefixes ``[code]`` so using ``str(e)`` would put
        # the code in the message text AND as the ``code=`` attribute.
        exc_cls = _classify_operational(e.code)
        # Plumb the full server text through ``raw_message`` so callers
        # that want the un-truncated diagnostic (operators reading
        # logs, structured-error tooling) don't have to walk
        # ``__cause__`` for it. ``message`` stays truncated for safe
        # default ``str(exc)``.
        #
        # ``InterfaceError`` does not subclass ``DatabaseError`` and
        # therefore does not accept ``code`` / ``raw_message`` —
        # dqlite-namespace ``DQLITE_PROTO`` codes route to
        # InterfaceError per PEP 249 §6 (protocol misuse rather than
        # database-operation failure). The original code is still
        # reachable via ``__cause__`` (the chained ``e``); the
        # interface-error message keeps the code visible inline so
        # callers don't have to walk the cause for routine
        # diagnostics.
        if issubclass(exc_cls, DatabaseError):
            raise exc_cls(e.message, code=e.code, raw_message=e.raw_message) from e
        raise exc_cls(f"{e.message} (code={e.code})") from e
    except _client_exc.DqliteConnectionError as e:
        # DqliteConnectionError carries no SQLite code today — pass
        # code=None explicitly so the signature matches the sibling
        # OperationalError handler above. Downstream disconnect
        # detection reaches the original via ``__cause__`` (set by the
        # ``from e``), so no information is lost.
        raise OperationalError(str(e), code=None) from e
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
        raise InterfaceError(f"Cluster policy rejection; {e}") from e
    except _client_exc.ClusterError as e:
        # Non-policy ClusterError — transient, code=None.
        raise OperationalError(str(e), code=None) from e
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
        raise OperationalError(str(e), code=None) from e
    except _client_exc.DataError as e:
        # client.DataError carries no server code today (encode-side
        # error surface), but plumb code=None explicitly so the
        # signature stays symmetric with the coded branches above.
        raise DataError(str(e), code=None) from e
    except _client_exc.InterfaceError as e:
        raise InterfaceError(str(e)) from e
    except _client_exc.DqliteError as e:
        # Catch-all for any future subclass of DqliteError not enumerated
        # above. Surface as InterfaceError rather than leaking to the
        # caller as a non-DBAPI exception.
        raise InterfaceError(f"unrecognized client error ({type(e).__name__}): {e}") from e
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
        raise DataError(f"cannot bind parameter: {e}") from e


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


def _convert_params(params: Sequence[Any] | None) -> list[Any] | None:
    """Convert driver-level bind parameters (e.g. datetime) to wire primitives."""
    _reject_non_sequence_params(params)
    if params is None:
        return None
    return [_convert_bind_param(p) for p in params]


def _strip_leading_comments(sql: str) -> str:
    """Strip leading SQL comments (-- and /* */) and whitespace."""
    s = sql.strip()
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


_ROW_RETURNING_PREFIXES: Final[tuple[str, ...]] = ("SELECT", "VALUES", "PRAGMA", "EXPLAIN", "WITH")

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
    | --[^\n]*              # line comment
    | /\*.*?\*/             # block comment
    """,
    re.VERBOSE | re.DOTALL,
)


def _strip_sql_noise(sql: str) -> str:
    """Replace string literals / identifiers / comments with a space.

    Preserves keyword boundaries so the downstream ``" RETURNING "``
    scan still sees a spaced match in the cleaned text. The space
    substitution is important: collapsing to empty would fuse adjacent
    tokens into identifiers that could themselves trigger false
    positives.
    """
    return _SQL_NOISE_RE.sub(" ", sql)


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
    return " RETURNING " in normalized or normalized.endswith(" RETURNING")


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
        "_connection",
        "_description",
        "_lastrowid",
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
        # PEP 249 optional extension. Currently no driver path appends
        # to this list; it's here so consumers can rely on the
        # attribute existing and being mutable.
        self.messages: list[tuple[type[Exception], Exception | str]] = []

    @property
    def connection(self) -> "Connection":
        """The Connection this Cursor was created from.

        PEP 249 optional extension. Read-only.
        """
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
    def lastrowid(self) -> int | None:
        """ROWID of this cursor's most-recent successful INSERT.

        Returns ``None`` before the first INSERT runs on this cursor
        and after ``close()`` scrubs the cursor's state.

        Unlike ``sqlite3.Cursor.lastrowid``, the value is scoped to the
        cursor, not the underlying Connection: a sibling cursor on the
        same Connection will not observe this cursor's last INSERT.
        The scrub on ``close()`` is consistent with that scope — ROLLBACK
        / UPDATE / DELETE / DDL do NOT clear it (mirroring stdlib), but
        closing the cursor does.

        **Not updated for ``INSERT ... RETURNING``** (or any row-returning
        statement). dqlite's wire protocol does not return
        ``last_insert_id`` on row-returning responses (it is only
        populated on Exec responses), so the row-returning execute path
        cannot surface the rowid. Read the id from the returned row
        instead. This is a known divergence from ``sqlite3.Cursor.
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
        # Reject bools explicitly even though ``bool`` is an ``int``
        # subclass: ``arraysize = True`` silently coercing to 1 is a
        # caller-bug trap, not a useful affordance.
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

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError("Cursor is closed")

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

    def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> "Cursor":
        """Execute a database operation (query or command).

        Returns ``self`` so callers can chain ``.fetchall()`` etc.
        """
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()
        # Clear after the guards but before the wire call so a caller
        # who executes on a closed cursor still sees the sharp
        # ``InterfaceError("Cursor is closed")`` without a state-clobber
        # side effect; a caller whose call raises mid-execute sees a
        # cursor in the "no result set" baseline, not one reporting the
        # previous query's description / rows.
        self._reset_execute_state()

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
                # for non-result statements; match that so callers who
                # branch on ``description is None`` to detect
                # non-queries behave consistently.
                self._description = None
            else:
                # PEP 249 §6.1.2 says ``type_code`` "must compare equal
                # to one of Type Objects." ``None`` never compares equal
                # to ``STRING`` / ``NUMBER`` / ``BINARY`` / ``DATETIME`` /
                # ``ROWID`` (the comparison protocol returns
                # ``NotImplemented`` → False), so a fallback of ``None``
                # silently produced PEP-249-illegal descriptions when
                # the wire layer returned fewer type codes than columns.
                #
                # Empty-result legitimate case: ``RowsResponse`` derives
                # ``column_types`` from the first row's type header, so
                # a zero-row result set returns ``column_types == []``.
                # PEP 249 permits ``type_code=None`` when the type is
                # not determinable; emit it in that specific case only.
                # For the real anomaly (rows present but short
                # ``column_types``), raise ``DataError`` so the wire
                # bug surfaces loudly.
                if len(column_types) == 0 and len(rows) == 0:
                    type_codes: list[Any] = [None] * len(columns)
                elif len(column_types) != len(columns):
                    raise DataError(
                        f"Wire response has {len(columns)} columns but "
                        f"{len(column_types)} type codes"
                    )
                else:
                    type_codes = list(column_types)
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

    def executemany(self, operation: str, seq_of_parameters: Iterable[Sequence[Any]]) -> "Cursor":
        """Execute a database operation multiple times.

        Cancellation atomicity: this driver runs in autocommit-by-default
        mode. Without a surrounding ``BEGIN`` / ``COMMIT``, each
        iteration commits server-side independently. If the call is
        interrupted mid-batch (sync timeout, ``KeyboardInterrupt``,
        etc.), iterations that already completed remain persisted.
        Wrap in an explicit transaction to make the batch atomic. See
        the ``Connection`` class docstring for the autocommit-by-
        default rationale.
        """
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()
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
        # Single source of truth for per-execute reset; see
        # ``_reset_execute_state``. Also zeroes ``_rowcount`` to -1 so
        # an empty ``seq_of_parameters`` ends with the same
        # ``rowcount`` shape as empty ``execute``.
        self._reset_execute_state()
        acc = _ExecuteManyAccumulator(max_rows=self._connection._max_total_rows)
        try:
            for params in seq_of_parameters:
                await self._execute_async(operation, params)
                acc.push(self)
        except BaseException:
            # Mid-batch failure leaves _rowcount at the last
            # iteration's value (which is misleading) and _rows /
            # _description in an inconsistent state. PEP 249 permits
            # rowcount=-1 ("undetermined"); use that signal so callers
            # cannot mistake the last iteration's rowcount for the
            # cumulative count of successfully-applied iterations.
            self._rowcount = -1
            self._rows = []
            self._description = None
            self._lastrowid = None
            self._row_index = 0
            raise
        acc.apply(self)

    def _check_result_set(self) -> None:
        if self._description is None:
            raise ProgrammingError("no results to fetch; execute a query first")

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set.

        Returns ``None`` when no more rows are available.
        """
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()
        self._check_result_set()
        # PEP 249 §6.1.1 — ``Connection.messages`` is cleared by the
        # cursor fetch methods. Done after the closed-check so a
        # closed cursor's fetch raises ``InterfaceError`` without
        # first perturbing the connection's diagnostic list. Defensive
        # against test mocks that pre-date the PEP 249 messages surface.
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]

        if self._row_index >= len(self._rows):
            return None

        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch up to ``size`` next rows of a query result.

        Returns an empty list when no more rows are available. ``size``
        defaults to ``self.arraysize``.
        """
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()
        self._check_result_set()
        # PEP 249 §6.1.1 — clear Connection.messages too. Defensive
        # against test mocks that pre-date the PEP 249 messages
        # surface.
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]

        if size is None:
            size = self._arraysize
        if size < 0:
            # Previously ``range(-5)`` silently returned [] — hid caller
            # bugs. ``arraysize`` setter already validates >= 1; mirror
            # that here.
            raise ProgrammingError(f"fetchmany size must be non-negative, got {size}")

        # Snapshot _row_index before the loop; restore on
        # cancel/exception so partially-iterated rows are not
        # silently consumed. See aio/cursor.py for rationale.
        snapshot = self._row_index
        result: list[tuple[Any, ...]] = []
        try:
            for _ in range(size):
                row = self.fetchone()
                if row is None:
                    break
                result.append(row)
        except BaseException:
            self._row_index = snapshot + len(result)
            raise

        return result

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result.

        Returns an empty list when the cursor has no more rows.
        """
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()
        self._check_result_set()
        # PEP 249 §6.1.1 — clear Connection.messages too. Defensive
        # against test mocks that pre-date the PEP 249 messages
        # surface.
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]

        result = self._rows[self._row_index :]
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

    def setinputsizes(self, sizes: Sequence[int | None]) -> None:
        """Set input sizes (no-op for dqlite).

        PEP 249 §6.1.1 names ``setinputsizes`` among the methods that
        clear the ``messages`` list; we do so even though the method
        itself does no work.
        """
        # PEP 249 §6.1.1 — clear "prior to executing the call" so the
        # contract holds even on the cross-thread-rejection path. The
        # six primary methods (execute / executemany / fetchone /
        # fetchmany / fetchall / close) all clear before
        # ``_check_thread`` for the same reason; this method and its
        # four secondary-method siblings keep the same ordering.
        del self.messages[:]
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]
        self._connection._check_thread()
        # PEP 249 §6.1.2 — operations on a closed cursor raise.
        self._check_closed()

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Set output size (no-op for dqlite). See ``setinputsizes``."""
        del self.messages[:]
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]
        self._connection._check_thread()
        # PEP 249 §6.1.2 — operations on a closed cursor raise.
        self._check_closed()

    def callproc(self, procname: str, parameters: Sequence[Any] | None = None) -> NoReturn:
        """PEP 249 optional extension — not supported.

        dqlite (and SQLite) have no stored-procedure concept. Annotated
        ``NoReturn`` because the body unconditionally raises
        ``NotSupportedError`` — symmetric with ``nextset`` below.
        """
        # PEP 249 §6.1.1 names ``callproc`` among the cursor methods
        # that clear ``Connection.messages`` / ``Cursor.messages``.
        # Clear before any guard so the contract holds even on the
        # cross-thread-rejection path. Mirrors ``nextset`` below.
        del self.messages[:]
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]
        self._connection._check_thread()
        # PEP 249 §6.1.2 — closed-cursor ops raise. Order: check
        # closed-state before raising NotSupported so the diagnostic
        # reflects the root cause.
        self._check_closed()
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
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]
        self._connection._check_thread()
        # PEP 249 §6.1.2 — closed-cursor operations raise.
        self._check_closed()
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative") -> None:
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
        conn_messages = getattr(self._connection, "messages", None)
        if conn_messages is not None:
            del conn_messages[:]
        self._connection._check_thread()
        self._check_closed()
        raise NotSupportedError("dqlite cursors are not scrollable")

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

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
