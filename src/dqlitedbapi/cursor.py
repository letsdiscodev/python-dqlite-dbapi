"""PEP 249 Cursor implementation for dqlite."""

import asyncio
import contextlib
import re
import weakref
from collections.abc import Awaitable, Callable, Coroutine, Iterable, Mapping, Sequence, Sized
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, NoReturn, Protocol, Self

import dqliteclient.exceptions as _client_exc
from dqliteclient.connection import _split_top_level_statements
from dqlitedbapi._constants import _is_int_not_bool, cluster_policy_rejection_message
from dqlitedbapi.exceptions import (
    AMBIGUOUS_COMMIT_CODES,
    AmbiguousCommitError,
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


# Registry of primary-code → PEP 249 class, mirroring stdlib sqlite3's
# util.c::get_exception_class. Extended SQLITE_CONSTRAINT_* codes share
# code & 0xFF == 19; the call site masks via primary_sqlite_code first.
# Keep the OperationalError default outside the dict so adding a code is
# one line.
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
    # Slot-fatal "bare DatabaseError" routes (CORRUPT / FORMAT / NOTADB).
    # Derived from the wire SSOT so this stays aligned with the SA
    # adapter's _BARE_DBE_DISCONNECT_CODES if the set grows.
    **{code: DatabaseError for code in BARE_DATABASE_ERROR_CODES},
    # Defensive pass-through entries NOT in the wire SSOT (dqlite-server
    # does not currently emit them): route to DatabaseError so a future
    # server release does not land on the OperationalError default.
    SQLITE_NOLFS: DatabaseError,
    SQLITE_AUTH: DatabaseError,
    SQLITE_NOTICE: DatabaseError,
    SQLITE_WARNING: DatabaseError,
    # dqlite-namespace codes (>= 1000): primary_sqlite_code passes them
    # through unchanged, so keys match the wire code.
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
    """Pick a PEP 249 exception class from a SQLite/dqlite code; unknown → OperationalError."""
    if code is None:
        return OperationalError
    return _CODE_TO_EXCEPTION.get(primary_sqlite_code(code), OperationalError)


async def _call_client[T](coro: Awaitable[T]) -> T:
    """Await a client coroutine, mapping its exceptions into the PEP 249 hierarchy via ``from``.

    Catch order matters: ClusterPolicyError before ClusterError
    (subclass first); the trailing DqliteError catch-all wraps any
    new client class as DatabaseError (server/cluster-state, not
    driver-misuse) so ``except DatabaseError:`` keeps catching it.
    """
    try:
        return await coro
    except _client_exc.OperationalError as e:
        # Constraint codes (primary 19) → IntegrityError; everything else
        # stays OperationalError so callers branching on leader-change /
        # busy codes still work. ``e.message`` (not str(e)) avoids the
        # code re-appearing in the text if __str__ ever re-adds a prefix.
        exc_cls = _classify_operational(e.code)
        raise exc_cls(e.message, code=e.code, raw_message=e.raw_message) from e
    except _client_exc.DqliteConnectionError as e:
        # Thread the optional leader-change code so SA's is_disconnect
        # code classifier fires on connect and query paths alike.
        code = getattr(e, "code", None)
        raw_msg = e.raw_message or str(e)
        raise OperationalError(str(e), code=code, raw_message=raw_msg) from e
    except _client_exc.ClusterPolicyError as e:
        # Deterministic policy rejection (client excludes it from retry).
        # InterfaceError so SA's is_disconnect invalidates the permanent-
        # reject slot without retrying against the policy wall. The
        # "Cluster policy rejection;" prefix goes on message only, not
        # raw_message (reserved for verbatim server text).
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
        # Wire desync (socket alive but stream lost): OperationalError,
        # paired with the "wire decode failed" substring in SA's
        # disconnect-message list so the pool slot invalidates.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise OperationalError(str(e), code=None, raw_message=raw_msg) from e
    except _client_exc.DataError as e:
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise DataError(str(e), code=None, raw_message=raw_msg) from e
    except _WireEncodeError as e:
        # Encode failure that escaped the client's _run_protocol. It is
        # NOT a _client_exc.ProtocolError (inheritance runs the other
        # way), so without this arm it would leak past except Error.
        raise DataError(f"wire encode failed: {e}", code=None, raw_message=str(e)) from e
    except _client_exc.InterfaceError as e:
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise InterfaceError(str(e), code=None, raw_message=raw_msg) from e
    except _client_exc.DqliteError as e:
        # Catch-all for future DqliteError subclasses: DatabaseError, not
        # InterfaceError, so a server-sourced error is not mis-classified
        # as driver misuse.
        raw_msg = getattr(e, "raw_message", None) or str(e)
        raise DatabaseError(
            f"unrecognized client error ({type(e).__name__}): {e}",
            code=None,
            raw_message=raw_msg,
        ) from e
    # No ``(TypeError, ValueError)`` blanket catch: the wire encoder
    # raises EncodeError (caught above) for bind-shape rejections, so a
    # bare coro-internal TypeError/ValueError propagates raw rather than
    # being mis-attributed to caller bind values as DataError.
    except BaseExceptionGroup as eg:
        # PEP 654: BaseExceptionGroup inherits from BaseException, so it
        # slips past every ``except Exception``/``except Error`` clause.
        # Split out CancelledError/KeyboardInterrupt/SystemExit children
        # (re-raise as their own group so the structured-concurrency
        # parent sees the cancel signal); wrap only the Exception
        # remainder as DatabaseError per PEP 249 §7.
        cancel_group, remainder = eg.split(
            lambda e: isinstance(e, (asyncio.CancelledError, KeyboardInterrupt, SystemExit))
        )
        if cancel_group is not None:
            # ``from None``: the cancel children are the signal we are
            # forwarding, not an error during exception handling.
            raise cancel_group from None
        # remainder cannot be None here under the split contract, but
        # narrow via ``if`` (not assert) so ``python -O`` can't turn the
        # .exceptions access into an AttributeError.
        if remainder is None:
            raise eg
        # Preserve remainder on __cause__ so SA's _walk_cause_chain /
        # is_disconnect can descend to the leaf exceptions.
        child_classes = {type(c).__name__ for c in remainder.exceptions}
        raise DatabaseError(
            f"aggregate {type(remainder).__name__} with {len(remainder.exceptions)} child(ren) "
            f"of class(es) {sorted(child_classes)}",
            code=None,
            raw_message=str(remainder),
        ) from remainder


if TYPE_CHECKING:
    from dqlitedbapi.connection import Connection


# Per-wire-type result converters; unrecognized types pass through. The
# wire layer is authoritative on type, so a mismatched value surfaces as
# DataError from the converter rather than being isinstance-guarded here.
_RESULT_CONVERTERS: Final[dict[int, Callable[[Any], Any]]] = {
    int(ValueType.ISO8601): _datetime_from_iso8601,
    int(ValueType.UNIXTIME): _datetime_from_unixtime,
}


def _convert_row(row: Sequence[Any], row_types: Sequence[int]) -> tuple[Any, ...]:
    """Apply result-side converters to a row using its per-row wire types.

    ``row_types`` must be this row's types, not ``column_types`` (row 0
    only): SQLite's dynamic typing lets rows in one column carry
    different ValueType tags (UNION, CASE, COALESCE, typeof()).
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
    """Build the cursor's ``_rows`` from a fetched result set.

    Skips the per-cell converter walk and materialises tuples directly
    when no converter type is present. The probe inspects per-row types
    too, since a row may diverge from row 0's column_types.
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


# Element count above which the async converters chunk work with a
# periodic ``await asyncio.sleep(0)``; smaller inputs take the sync fast
# path. Chunk picked to keep per-chunk cost under ~16 ms.
_LARGE_RESULT_ROW_THRESHOLD: Final[int] = 4096
_CONVERT_ROWS_YIELD_EVERY: Final[int] = 4096


async def _convert_rows_async(
    rows: Sequence[Sequence[Any]],
    row_types: Sequence[Sequence[int]],
    column_types: Sequence[int],
) -> list[tuple[Any, ...]]:
    """Async sibling of :func:`_convert_rows` that yields to the loop between row batches.

    Only the async cursor surface calls this; the sync surface calls
    :func:`_convert_rows` directly because it runs on the dbapi's own
    daemon loop where monopolisation is by design.
    """
    if not rows:
        return []
    if len(rows) < _LARGE_RESULT_ROW_THRESHOLD:
        return _convert_rows(rows, row_types, column_types)
    # Probe column_types once up-front; fold the per-row probe into the
    # per-chunk loop so the first yield is not delayed by an O(rows×cols)
    # walk (~100 ms on a 100k×32 fetch) before any sleep(0).
    column_types_hit = any(t in _RESULT_CONVERTERS for t in column_types)
    result: list[tuple[Any, ...]] = []
    rt_len = len(row_types)
    n_rows = len(rows)
    chunk = _CONVERT_ROWS_YIELD_EVERY
    for chunk_start in range(0, n_rows, chunk):
        chunk_end = min(chunk_start + chunk, n_rows)
        # Per-chunk probe preserving the converter-free tuple(row) fast
        # path at chunk granularity.
        chunk_hit = column_types_hit or any(
            t in _RESULT_CONVERTERS
            for j in range(chunk_start, min(chunk_end, rt_len))
            for t in row_types[j]
        )
        if chunk_hit:
            for i in range(chunk_start, chunk_end):
                types = row_types[i] if i < rt_len else column_types
                result.append(_convert_row(rows[i], types))
        else:
            for i in range(chunk_start, chunk_end):
                result.append(tuple(rows[i]))
        await asyncio.sleep(0)
    return result


def _reject_non_sequence_params(params: Any) -> None:
    """Reject mappings, unordered containers, and str/bytes per PEP 249 qmark rules.

    set/frozenset scramble positional order; str/bytes/bytearray/
    memoryview would explode into per-character binds.
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
    # Reject non-Sized iterables (generators/iterators): the leader-flip
    # retry path may re-issue the statement, which a single-pass iterable
    # cannot satisfy.
    if not isinstance(params, Sized):
        raise ProgrammingError(
            f"qmark paramstyle requires a sized sequence (tuple/list); got "
            f"{type(params).__name__!r}. Generators and other non-sized "
            f"iterables are rejected because the driver may need to "
            f"re-execute on leader-flip retry, which a single-pass "
            f"iterable cannot satisfy. Materialise into a tuple/list "
            f"explicitly."
        )


# Outer shapes executemany must reject before iterating: str/bytes/
# bytearray/memoryview (iterate per-character), set/frozenset (unordered).
# dict is rejected separately below by exact-type check so dict
# SUBCLASSES (OrderedDict, etc.) remain accepted as ordered iterables.
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

    Shared by the cursor and connection-shortcut entry points so all
    four agree on the accept/reject contract.
    """
    if isinstance(seq_of_parameters, _REJECTED_EXECUTEMANY_SEQ_TYPES):
        raise ProgrammingError(
            f"executemany seq_of_parameters must be an iterable of "
            f"parameter sets (e.g. list of tuples), not "
            f"{type(seq_of_parameters).__name__}. Iterating a "
            f"{type(seq_of_parameters).__name__} parameter-set is "
            f"almost certainly a bug."
        )
    # Exact-type check for dict only — subclasses iterate keys in
    # insertion order and are legitimate ordered parameter-set iterables.
    if type(seq_of_parameters) is dict:
        raise ProgrammingError(
            "executemany seq_of_parameters must be an iterable of "
            "parameter sets (e.g. list of tuples), not dict. Iterating "
            "a dict parameter-set is almost certainly a bug; wrap as "
            "[params] for a single-row batch."
        )


def _convert_params(params: Sequence[Any] | None) -> list[Any] | None:
    """Convert driver-level bind parameters (e.g. datetime) to wire primitives."""
    _reject_non_sequence_params(params)
    if params is None:
        return None
    converted: list[Any] = []
    for p in params:
        converted.append(_convert_one_bind_param(p))
    return converted


def _convert_one_bind_param(p: Any) -> Any:
    """Convert a single bind parameter to a wire primitive.

    Error subclasses propagate unchanged; a __conform__ raise tagged
    ``_dqlite_conform_propagate`` propagates unwrapped (programmer bug
    must not be swallowed); other adapter failures wrap as DataError.
    """
    try:
        return _convert_bind_param(p)
    except Error:
        raise
    except Exception as e:
        if getattr(e, "_dqlite_conform_propagate", False):
            raise
        raise DataError(
            f"adapter for {type(p).__name__} failed: {e}",
            code=None,
        ) from e


async def _convert_params_async(params: Sequence[Any] | None) -> list[Any] | None:
    """Async sibling of :func:`_convert_params` that yields to the loop between bind batches.

    Only the async cursor surface calls this; the sync surface calls
    :func:`_convert_params` directly (runs on the dbapi's own daemon
    loop). A single execute may carry ~32k binds (SA insertmanyvalues).
    """
    _reject_non_sequence_params(params)
    if params is None:
        return None
    if len(params) < _LARGE_RESULT_ROW_THRESHOLD:
        return [_convert_one_bind_param(p) for p in params]
    converted: list[Any] = []
    for i, p in enumerate(params):
        converted.append(_convert_one_bind_param(p))
        if (i + 1) % _CONVERT_ROWS_YIELD_EVERY == 0:
            await asyncio.sleep(0)
    return converted


def _strip_leading_comments(sql: str) -> str:
    """Strip leading SQL comments (-- and /* */), whitespace, and a leading BOM.

    Strips the BOM because SQLite skips it before tokenisation but
    str.strip() does not treat \\ufeff as whitespace. Must NOT consume
    bare ``;`` separators — _is_multi_statement's tail walk relies on a
    trailing ``;`` being detected (use the _and_separators sibling when
    ``;`` should be consumed).
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
        else:
            break
    return s


def _strip_leading_comments_and_separators(sql: str) -> str:
    """Like :func:`_strip_leading_comments` but also consumes bare ``;`` separators.

    Used ONLY by _classify_caller_sql's empty-statement check; do NOT
    use in _is_multi_statement's tail walk (it relies on ``;`` detection).
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

# Word-boundary scan for RETURNING (run on noise-stripped, uppercased
# text). A literal-space substring scan missed RETURNING on its own line
# and silently classified DML-with-RETURNING as exec-only — data-loss-grade.
_RETURNING_WORD_RE: Final[re.Pattern[str]] = re.compile(r"\bRETURNING\b")

# Verbs that take no parameters and cannot drive executemany; admitting
# them would silently re-run the bare statement N times.
_EXECUTEMANY_REJECT_VERBS: Final[frozenset[str]] = frozenset(
    {"SAVEPOINT", "RELEASE", "ROLLBACK", "BEGIN", "COMMIT", "END"}
)

# SQL noise the RETURNING scan must skip: string literals, quoted
# identifiers, and comments. Without it, a RETURNING inside a string
# literal or identifier misclassifies the statement as row-returning.
_SQL_NOISE_RE: Final[re.Pattern[str]] = re.compile(
    r"""
    '(?:[^']|'')*'          # single-quoted string literal
    | "(?:[^"]|"")*"        # double-quoted identifier
    | \[[^\]]*\]            # bracket-quoted identifier
    | `(?:[^`]|``)*`        # backtick-quoted identifier (MySQL-compat)
    | --[^\n]*              # line comment
    | /\*.*?\*/             # block comment
    """,
    re.VERBOSE | re.DOTALL,
)


def _strip_sql_noise(sql: str) -> str:
    """Replace string literals / identifiers / comments with a space.

    A space (not empty) so adjacent tokens don't fuse into identifiers
    that could trigger false positives in the downstream keyword scan.
    """
    return _SQL_NOISE_RE.sub(" ", sql)


_CREATE_TRIGGER_PREFIX_RE: Final = re.compile(
    r"^\s*CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?TRIGGER\b",
    re.IGNORECASE,
)


def _is_multi_statement(sql: str) -> bool:
    """Return ``True`` if ``sql`` contains more than one statement.

    The server's prepare path silently keeps only the first statement,
    so an unguarded multi-statement drops everything past the first ``;``.
    A trailing ``;`` (plus whitespace/comments) is fine. CREATE TRIGGER
    DDL defers to the client's BEGIN..END-aware tokenizer because its
    body legitimately contains ``;`` the flat scan would miscount.
    """
    cleaned = _strip_sql_noise(sql)
    if _CREATE_TRIGGER_PREFIX_RE.match(cleaned):
        return len(_split_top_level_statements(sql)) > 1
    semicolon = cleaned.find(";")
    while semicolon != -1:
        # Everything past the ``;`` must reduce to whitespace / comments.
        tail = _strip_leading_comments(cleaned[semicolon + 1 :])
        if tail:
            return True
        next_idx = cleaned.find(";", semicolon + 1)
        if next_idx == -1:
            return False
        semicolon = next_idx
    return False


def _validate_caller_param_shape(parameters: Sequence[Any] | None) -> None:
    """Reject structural outer-shape mistakes in caller parameters.

    Hoisted out of _classify_caller_sql so callers can run it BEFORE
    _reset_execute_state — prior result-set state must survive a
    caller-shape misuse so a retry-with-coerce idiom can read
    cur.description.
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
    """Pre-flight classification of caller SQL, raising ProgrammingError.

    Catches three caller-side mistakes that would otherwise cause silent
    data loss or a misleading server error class: empty/comment-only SQL,
    multi-statement SQL (server keeps only the first), and ``?``-count vs
    len(parameters) mismatch (saves a wire RTT). Each detector neutralises
    quoted tokens / comments via _strip_sql_noise first.
    """
    # NUL-in-SQL: pre-flight as ProgrammingError (the wire would reject as
    # DataError, the wrong class for malformed caller SQL).
    if "\x00" in operation:
        raise ProgrammingError("the query contains a null character")
    # Empty: blank, comment-only, or bare ``;`` separators all count as
    # zero statements. Stdlib raises ProgrammingError for ""; match.
    if not _strip_leading_comments_and_separators(operation):
        raise ProgrammingError("empty statement")
    # Multi-statement: stdlib raises with this exact wording.
    if _is_multi_statement(operation):
        raise ProgrammingError("You can only execute one statement at a time.")
    # executemany passes skip_param_count_check (per-iteration check runs
    # in its loop). For execute, parameters=None is treated as count 0.
    if skip_param_count_check:
        return
    if parameters is not None:
        # Re-run the structural gate so the executemany per-iteration path
        # gets the same diagnostic without bypassing it.
        _validate_caller_param_shape(parameters)
        try:
            param_count = len(parameters)
        except TypeError:
            return  # caller will trip the binding-layer rejection
    else:
        param_count = 0
    cleaned = _strip_sql_noise(operation)
    placeholder_count = cleaned.count("?")
    if placeholder_count != param_count:
        raise ProgrammingError(
            f"Incorrect number of bindings supplied. The current "
            f"statement uses {placeholder_count}, and there are "
            f"{param_count} supplied."
        )


class _ExecuteManyCursor(Protocol):
    """Structural shape of Cursor / AsyncCursor consumed by _ExecuteManyAccumulator.

    The two cursor classes share no base and a Union would need a
    circular import; structural typing avoids both.
    """

    _rowcount: int
    _description: _Description
    _rows: list[tuple[Any, ...]]
    _row_index: int
    _closed: bool


class _ExecuteManyAccumulator:
    """Shared state for the RETURNING-aware ``executemany`` loop.

    For RETURNING statements, rows from each iteration must accumulate
    so a final fetchall yields every returned row across parameter sets.
    """

    __slots__ = ("_max_rows", "_pushed", "description", "rows", "total_affected")

    def __init__(self, max_rows: int | None = None) -> None:
        self.total_affected = 0
        self.rows: list[tuple[Any, ...]] = []
        self.description: _Description = None
        # push() count: lets apply() distinguish "zero iterations ran"
        # (empty seq) from "every iteration was no-op DML".
        self._pushed = 0
        # Cumulative row cap across iterations; the wire max_total_rows
        # governor caps a single round-trip only.
        self._max_rows = max_rows

    def push(self, cursor: _ExecuteManyCursor) -> None:
        """Record one iteration's output into the accumulator.

        Branches on _description so "rows returned" and "rows affected"
        stay decoupled even if _rowcount's meaning later diverges from
        len(rows) (e.g. INSERT ... ON CONFLICT ... RETURNING).
        """
        # Snapshot fields BEFORE reading _closed: under
        # check_same_thread=False a sibling's _cascade_cursors may zero
        # them mid-read. If _closed is then True the locals may be
        # incoherent, so skip the push.
        description = cursor._description
        rows = cursor._rows
        rowcount = cursor._rowcount
        if getattr(cursor, "_closed", False):
            return
        self._pushed += 1
        if description is not None:
            # Row-returning: count emitted rows via len() so this survives
            # any future decoupling of rowcount on the RETURNING path.
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
            self.total_affected += rowcount

    def apply(self, cursor: _ExecuteManyCursor) -> None:
        """Materialise the accumulator's state onto the cursor.

        No-op if the cursor closed concurrently: re-populating _rows /
        _description would visibly un-close the result set. Guards the
        async flavour (sync is covered by the threading lock).
        """
        if cursor._closed:
            return
        if self._pushed == 0:
            # Empty seq: deterministic zero. stdlib/psycopg2 set
            # rowcount=0 (not -1) here so ``if cur.rowcount > 0`` works.
            cursor._rowcount = 0
            return
        cursor._rowcount = self.total_affected
        if self.description is not None:
            cursor._description = self.description
            cursor._rows = self.rows
            cursor._row_index = 0


def _is_row_returning(sql: str) -> bool:
    """Heuristic for "does this statement return a result set?" (SSOT for both cursors).

    A leading SELECT/VALUES/PRAGMA/EXPLAIN returns rows; a leading WITH (CTE) is
    resolved to its top-level statement so ``WITH ... INSERT`` (no RETURNING)
    routes as DML, while ``WITH ... SELECT`` stays a query; an embedded RETURNING
    on any DML makes it row-returning.
    """
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    body = _strip_leading_with_clause(normalized) if normalized.startswith("WITH") else normalized
    # If the CTE could not be parsed, _strip_leading_with_clause returns the
    # original (still WITH-prefixed); preserve the safe "treat as query" default.
    if body.startswith("WITH"):
        return True
    if body.startswith(("SELECT", "VALUES", "PRAGMA", "EXPLAIN")):
        return True
    return _RETURNING_WORD_RE.search(normalized) is not None


def _strip_leading_with_clause(normalized: str) -> str:
    """Strip a leading ``WITH cte AS (...)`` clause (incl. recursive / comma-chained).

    ``normalized`` must already be noise-stripped, comment-stripped, and
    uppercased. Returns the substring at the first non-CTE keyword, or
    the original string if the WITH cannot be parsed (defensive).
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
    # Each CTE is ``name [(cols)] AS (body)``. Balance parens so a
    # column-list ``)`` isn't mistaken for the body's closer.
    while True:
        as_idx = normalized.find(" AS ", pos)
        as_idx_paren = normalized.find(" AS(", pos)
        if as_idx == -1 or (as_idx_paren != -1 and as_idx_paren < as_idx):
            as_idx = as_idx_paren
        if as_idx == -1:
            return normalized  # malformed; fall back
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
        # i is just past the body's closing ``)``; the CTE may be
        # followed by ``, name AS (...)`` or the top-level statement.
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
    """True if ``sql`` is DML admissible to ``executemany`` (incl. CTE prefix / RETURNING).

    Admits ``WITH cte AS (...) INSERT ...`` (which _is_row_returning
    rejects on the WITH) and ``INSERT ... RETURNING``; pure SELECT /
    VALUES / PRAGMA / EXPLAIN stay rejected.
    """
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    body = _strip_leading_with_clause(normalized)
    return body.startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))


_INT64_OVERFLOW_THRESHOLD: Final[int] = 1 << 63
_UINT64_RANGE: Final[int] = 1 << 64


def _to_signed_int64(value: int) -> int:
    """Re-cast a wire ``uint64`` value to signed ``int64``.

    The wire sends last_insert_id / rows_affected as uint64, so negative
    rowids (legal on INTEGER PRIMARY KEY) arrive above 2**63; without
    this cast they surface as 19-digit positives that break rowid lookups.
    """
    if value >= _INT64_OVERFLOW_THRESHOLD:
        return value - _UINT64_RANGE
    return value


def _is_insert_or_replace(sql: str) -> bool:
    """True if ``sql`` is INSERT (incl. INSERT OR REPLACE/IGNORE) or bare REPLACE.

    Gates the _lastrowid write to stdlib's "only INSERT/REPLACE update
    lastrowid" rule; the wire returns last_insert_id (often 0) on every
    Exec response, so an unconditional write would zero the sticky value.
    """
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    body = _strip_leading_with_clause(normalized) if normalized.startswith("WITH") else normalized
    return body.startswith(("INSERT", "REPLACE"))


def _is_dml_rowcount_meaningful(sql: str) -> bool:
    """True if ``sql`` is DML with a meaningful sqlite3_changes() count.

    INSERT/UPDATE/DELETE/REPLACE. Gates the _rowcount write: the wire
    returns rows_affected=0 for DDL, but stdlib reports -1
    (undetermined), so ``rowcount == 0`` would diverge. Default to -1.
    """
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    body = _strip_leading_with_clause(normalized) if normalized.startswith("WITH") else normalized
    return body.startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))


def _is_pragma(sql: str) -> bool:
    """True if ``sql`` is a PRAGMA. stdlib sqlite3 reports rowcount=-1 for all
    PRAGMA (read or write); a read-form PRAGMA otherwise lands in the
    row-returning branch and would pick up len(rows)."""
    cleaned = _strip_sql_noise(sql)
    normalized = _strip_leading_comments(cleaned).upper().lstrip("(")
    return normalized.startswith("PRAGMA")


_COMMIT_STMT_RE: Final[re.Pattern[str]] = re.compile(
    r"^(?:COMMIT|END)\b",
    re.IGNORECASE,
)


def _is_commit_statement(sql: str) -> bool:
    """True if ``sql`` is an explicit COMMIT (or its ``END`` synonym). Used to give the
    cursor COMMIT path the same in-doubt classification as Connection.commit()."""
    normalized = _strip_leading_comments(_strip_sql_noise(sql)).lstrip()
    return _COMMIT_STMT_RE.match(normalized) is not None


class Cursor:
    """PEP 249 compliant database cursor."""

    # __weakref__ is declared so Connection._cursors (a WeakSet) can
    # reference the cursor — slotted classes need it explicit.
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
        self._completed_iterations: int = 0
        # Inherit the parent Connection's default row_factory. The
        # isinstance check excludes MagicMock fakes whose auto-magic
        # _row_factory would otherwise wrap every row. Deferred import
        # breaks the cursor → connection cycle.
        from dqlitedbapi.connection import Connection as _Connection

        self._row_factory: RowFactory | None = (
            getattr(connection, "_row_factory", None)
            if isinstance(connection, _Connection)
            else None
        )
        # PEP 249 optional extension; no driver path appends today.
        self.messages: list[tuple[type[Exception], Exception]] = []

    @property
    def connection(self) -> "Connection":
        """The Connection this Cursor was created from (PEP 249 optional, read-only).

        After close() this is a weakref.proxy; once the Connection is
        GC'd the proxy raises ReferenceError, which we re-raise as
        InterfaceError to stay in the dbapi.Error hierarchy. The getter
        does not run _check_thread (read-only bypass, like description /
        rowcount).
        """
        # AttributeError is also caught: the probe reads .address (must
        # stay side-effect-free) and a mock-typed parent raises bare
        # AttributeError, which would escape the Error hierarchy.
        try:
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
        """Column descriptions for the last query, as a tuple of 7-tuples.

        ``type_code`` is the wire ValueType int from the first frame;
        other fields are None (dqlite doesn't expose them). Mixed-type
        columns flatten to the first non-NULL row's tag; all-NULL /
        empty-result columns resolve to the UNKNOWN Type Object (not
        None) so ``type_code == STRING`` etc. stays valid per PEP 249
        §6.1.2. For per-row types use ``row_types[i]`` via _convert_row.
        """
        return self._description

    @property
    def rowcount(self) -> int:
        """Rows affected (DML) or, for SELECT, rows produced (dqlite buffers the
        full result, unlike stdlib's -1); -1 if unknown / not applicable.
        """
        return self._rowcount

    @property
    def completed_iterations(self) -> int:
        """Iterations that completed on the most recent executemany; 0 otherwise.

        After a mid-batch cancel, retains the count already committed
        server-side (autocommit-per-iteration) so a caller can write
        idempotent compensation. Resets to 0 each executemany call.
        """
        return self._completed_iterations

    @property
    def lastrowid(self) -> int | None:
        """ROWID of this cursor's most-recent successful INSERT; None before any.

        Cursor-scoped (a sibling cursor does not observe it). ROLLBACK /
        UPDATE / DELETE / DDL / close() do not clear it. NOT updated for
        INSERT ... RETURNING (the wire omits last_insert_id on
        row-returning responses) — a divergence from stdlib; read the id
        from the returned row instead.
        """
        return self._lastrowid

    @property
    def rownumber(self) -> int | None:
        """0-based index of the next row; None if no result set is active (PEP 249 optional).

        Runs _check_thread so a foreign-thread reader can't observe a
        mid-fetch _row_index. A closed cursor returns None too (ambiguous
        with "no result set"); gate on cur.closed first if the
        distinction matters.
        """
        # getattr defensively for __new__-built fixtures and the GC'd
        # weakref.proxy parent (every access raises ReferenceError).
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
        # PEP 249 §6.4: clear messages on entry, before the guards.
        # Suppress tolerates __new__-built fixtures bypassing __init__.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        # Closed-state guard on the SETTER only; the getter stays
        # permissive like peer drivers.
        self._check_closed()
        # Enforce the default per-thread affinity. _check_thread short-circuits
        # under check_same_thread=False, where the cursor-per-thread
        # sub-contract becomes the caller's responsibility.
        self._connection._check_thread()
        # Reject bool explicitly: ``arraysize = True`` coercing to 1 is a
        # caller-bug trap.
        if not _is_int_not_bool(value):
            raise ProgrammingError(
                f"arraysize must be a non-negative int, got {type(value).__name__}"
            )
        if value < 0:
            raise ProgrammingError(f"arraysize must be non-negative, got {value}")
        self._arraysize = value

    @property
    def closed(self) -> bool:
        """``True`` once :meth:`close` has been called (peer-driver parity)."""
        return self._closed

    @property
    def row_factory(self) -> RowFactory | None:
        """stdlib row_factory parity: a callable ``factory(cursor, row)`` wrapping each row.

        None (default) returns plain tuples. ``sqlite3.Row`` does NOT
        work here — its C constructor type-checks the cursor arg — and
        surfaces DataError at first fetch. New cursors inherit the
        Connection's default factory.
        """
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        # See ``arraysize.setter`` for the clear / closed / affinity prelude.
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        self._check_closed()
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

        Resets before prepare (stdlib parity) so a mid-execute failure
        cannot leave the prior query's shape exposed. Deliberately does
        NOT touch _lastrowid (cursor-scoped, survives across statements).
        """
        self._description = None
        self._rows = []
        self._row_index = 0
        self._rowcount = -1
        # Also reset here so it can't carry from a prior executemany into
        # a subsequent single-row execute.
        self._completed_iterations = 0

    def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> Self:
        """Execute a database operation (query or command).

        Returns ``self`` so callers can chain ``.fetchall()`` etc.
        """
        del self.messages[:]
        # _check_closed BEFORE _check_thread: close() swaps _connection
        # for a weakref.proxy whose _check_thread raises ReferenceError
        # (outside the Error hierarchy) once the parent is GC'd.
        self._check_closed()
        self._connection._check_thread()
        # Non-str operation is a caller-shape misuse: reject WITHOUT
        # scrubbing prior result state (stdlib parity) so a
        # retry-with-coerce idiom can inspect cur.description.
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )

        # Caller-shape rejection BEFORE the reset (same preservation
        # contract as the non-str operation arm above).
        _validate_caller_param_shape(parameters)

        # Scrub per-execute state so a rejected execute lands at the
        # "no result set" baseline (does NOT touch _lastrowid).
        self._reset_execute_state()
        # Pre-flight empty / multi-statement / ?-count so caller bugs
        # surface with the right class and no wire RTT.
        _classify_caller_sql(operation, parameters)

        # Intercept PRAGMA busy_timeout here — dqlite's VFS authorizer
        # rejects it server-side, so the SQLite escape hatch would
        # otherwise raise DatabaseError("not authorized").
        from dqlitedbapi._pragma_intercept import (
            try_intercept_busy_timeout,
            try_rewrite_begin_to_immediate,
        )

        if try_intercept_busy_timeout(self, operation, parameters):
            return self

        # Rewrite bare BEGIN → BEGIN IMMEDIATE under session_mode
        # "immediate" (default) to avoid the SQLITE_BUSY_SNAPSHOT (517)
        # race: the writer lock is taken at BEGIN so the read snapshot
        # can't be overtaken. Explicit BEGIN <mode> passes through.
        rewritten = try_rewrite_begin_to_immediate(
            operation,
            session_mode=getattr(self._connection, "_dqlite_session_mode", "immediate"),
        )
        if rewritten is not None:
            operation = rewritten

        # BUSY retry wrap (stdlib busy_timeout parity). The coro-factory
        # rebuilds a fresh coroutine per retry; busy_timeout=0 means no
        # retry.
        from dqlitedbapi._busy_retry import _resolve_busy_timeout_seconds, retry_sync_on_busy

        retry_sync_on_busy(
            _resolve_busy_timeout_seconds(self._connection),
            self._connection._run_sync,
            lambda: self._execute_async(operation, parameters),
        )
        return self

    async def _execute_async(self, operation: str, parameters: Sequence[Any] | None = None) -> None:
        """Async implementation of execute.

        Routes through the connection's public API so the _in_use guard,
        fatal-error invalidation, and leader-change detection all apply.
        """
        conn = await self._connection._get_async_connection()
        params = _convert_params(parameters)

        if _is_row_returning(operation):
            columns, column_types, row_types, rows = await _call_client(
                conn.query_raw_typed(operation, params)
            )
            if not columns:
                # PRAGMA write form (``PRAGMA foreign_keys = ON``)
                # produces no columns. Match stdlib: description=None and
                # rowcount=-1 (not 0) so ``cur.rowcount > 0`` behaves.
                self._description = None
                self._rows = []
                self._row_index = 0
                self._rowcount = -1
                return
            else:
                # PEP 249 §6.1.2 requires type_code to compare equal to a
                # Type Object; None does not, so emit the UNKNOWN sentinel
                # (not None) when the wire can't resolve a type. Empty
                # result sets carry no type info (column_types == []); a
                # short column_types on a non-empty result is a wire bug.
                if len(column_types) == 0 and len(rows) == 0:
                    type_codes: list[int | _DBAPIType] = [_UNKNOWN_TYPE] * len(columns)
                elif len(column_types) != len(columns):
                    raise DataError(
                        f"Wire response has {len(columns)} columns but "
                        f"{len(column_types)} type codes"
                    )
                else:
                    # For a NULL column type, rescue-scan row_types[1:]
                    # for the first non-NULL tag (row 0 may be NULL while
                    # later rows are typed); fall to UNKNOWN only if every
                    # row is NULL at that column.
                    type_codes = []
                    for col_idx, c in enumerate(column_types):
                        if c != ValueType.NULL:
                            type_codes.append(int(c))
                            continue
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
            self._rows = _convert_rows(rows, row_types, column_types)
            self._row_index = 0
            # rowcount = len(rows) for SELECT (the wire buffers the whole
            # result up front, so the count is known) — a divergence from
            # stdlib's -1, relied on by SA insertmanyvalues. Portable
            # "did SELECT find anything" idiom is ``fetchone() is not None``.
            # PRAGMA reads are the exception: stdlib reports -1 for ALL PRAGMA,
            # so match that (and the busy_timeout interceptor) rather than len.
            self._rowcount = -1 if _is_pragma(operation) else len(rows)
        else:
            try:
                last_id, affected = await _call_client(conn.execute(operation, params))
            except OperationalError as e:
                # Mirror Connection.commit(): an explicit COMMIT that loses leadership after
                # the entry was submitted leaves the write in doubt. A not-leader rejection is
                # a clean pre-apply failure and stays OperationalError.
                if _is_commit_statement(operation) and e.code in AMBIGUOUS_COMMIT_CODES:
                    raise AmbiguousCommitError(
                        "ambiguous commit: leadership lost during COMMIT; "
                        "the write may or may not have been persisted. "
                        f"Original: {e}",
                        code=e.code,
                        raw_message=getattr(e, "raw_message", None),
                    ) from e
                raise
            if _is_insert_or_replace(operation):
                self._lastrowid = _to_signed_int64(last_id)
            if _is_dml_rowcount_meaningful(operation):
                self._rowcount = _to_signed_int64(affected)
            else:
                self._rowcount = -1
            self._description = None
            self._rows = []
            self._row_index = 0

    def executemany(self, operation: str, seq_of_parameters: Iterable[Sequence[Any]], /) -> Self:
        """Execute a database operation multiple times.

        Autocommit-by-default: without a surrounding BEGIN/COMMIT each
        iteration commits independently, so a mid-batch interrupt leaves
        completed iterations persisted — wrap in a transaction for
        atomicity.

        lastrowid is preserved across executemany — the pre-batch value is
        kept, matching stdlib sqlite3, which does not update lastrowid for
        executemany. Rejected calls preserve it too, since no batch ran.
        """
        del self.messages[:]
        # See ``execute``'s prelude comment for the ordering rationale.
        self._check_closed()
        self._connection._check_thread()
        # Input-validation rejections FIRST (None seq / bad outer shape /
        # non-str operation): caller-shape misuse that preserves prior
        # cursor state (stdlib parity).
        if seq_of_parameters is None:
            raise ProgrammingError(
                "executemany() seq_of_parameters must be a sequence/iterable, not None",
                code=None,
            )
        _validate_executemany_seq_shape(seq_of_parameters)
        if not isinstance(operation, str):
            raise ProgrammingError(
                f"operation must be a str SQL statement, got {type(operation).__name__}",
                code=None,
            )
        # Scrub per-execute state (does NOT touch _lastrowid, which is
        # cleared after a successful loop in _executemany_async).
        self._reset_execute_state()
        # Reject transaction-control verbs / pure queries up front so the
        # ProgrammingError surfaces at the caller's frame. Loop
        # comment-strip + leading-;-strip together (and rstrip a trailing
        # ``;``) so neither ``;BEGIN ...`` nor ``BEGIN; INSERT ...`` nor a
        # comment after a ``;`` can bypass the reject-list.
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
            # Use the comment-stripped uppercase form so a leading
            # comment still routes to the PRAGMA-specific diagnostic.
            if head_normalised.startswith("PRAGMA"):
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

        Accumulates per-iteration rows for RETURNING/result-producing DML
        so the final fetchall sees every parameter set's rows (otherwise
        each iteration would overwrite _rows). Pure queries are rejected
        in the sync wrapper before this runs.
        """
        # Snapshot before the reset zeros the counter, so the
        # BaseException arm can tell an input-validation raise (restore
        # pre-batch) from a mid-batch raise (keep partial progress).
        completed_iterations_pre_batch = self._completed_iterations
        # Reset BEFORE the classifier so a classifier-raise leaves the
        # no-result baseline (stdlib reset-then-prepare ordering).
        self._reset_execute_state()
        # Hoist the classifier once (empty/multi/NUL are loop-invariant);
        # the per-iteration ?-count check is then a cheap O(1).
        # skip_param_count_check defers that count to the loop body.
        try:
            _classify_caller_sql(operation, None, skip_param_count_check=True)
        except BaseException:
            self._completed_iterations = completed_iterations_pre_batch
            raise
        cleaned = _strip_sql_noise(operation)
        placeholder_count = cleaned.count("?")
        acc = _ExecuteManyAccumulator(max_rows=self._connection._max_total_rows)
        # Snapshot pre-batch lastrowid so a mid-batch cancel can restore
        # it instead of leaking an in-batch iteration's rowid.
        lastrowid_pre_batch = self._lastrowid
        try:
            for params in seq_of_parameters:
                # Structural reject BEFORE len() so a single string/bytes
                # row gets the sharp diagnostic, not a per-character count.
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
                # BUSY retry must be per-iteration, not around the whole
                # batch: in autocommit mode each iteration commits
                # independently, so retrying the batch would re-insert
                # already-committed rows.
                from dqlitedbapi._busy_retry import (
                    _resolve_busy_timeout_seconds,
                    retry_async_on_busy,
                )

                # Default-argument capture freezes params so retries get
                # this iteration's values (bare closure would late-bind).
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
                # push() early-returns if a tier-2 cascade zeroed the
                # cursor mid-iteration; advance the counter only when
                # still operable to keep the (count, anchor) invariant.
                if not self._closed:
                    self._completed_iterations += 1
            # stdlib sqlite3 leaves lastrowid unchanged across executemany;
            # restore the pre-batch value rather than exposing an in-batch
            # iteration's rowid (which no single iteration owns canonically).
            self._lastrowid = lastrowid_pre_batch
        except BaseException:
            # Mid-batch failure: rowcount=-1 ("undetermined") so the last
            # iteration's count isn't mistaken for the cumulative total.
            # lastrowid restores to pre-batch unconditionally — matching the
            # success path, the docstring, and stdlib, which leave lastrowid
            # unchanged across executemany. _completed_iterations is the
            # separate partial-progress anchor (surfaced via rownumber) and
            # is restored only on zero in-batch progress. (The classifier-raise
            # path is handled by the inner try above.) messages cleared per
            # PEP 249 §6.1.1.
            self._rowcount = -1
            self._rows = []
            self._description = None
            self._row_index = 0
            self._lastrowid = lastrowid_pre_batch
            if self._completed_iterations == 0:
                self._completed_iterations = completed_iterations_pre_batch
            del self.messages[:]
            raise
        acc.apply(self)

    def _check_result_set(self) -> None:
        if self._description is None:
            raise ProgrammingError("no results to fetch; execute a query first")

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row, or None when exhausted or no result set is active.

        Stdlib parity: fetchone() after DML returns None rather than
        raising (the de facto portable contract).
        """
        del self.messages[:]
        # See ``execute``'s prelude comment for the ordering rationale.
        self._check_closed()
        self._connection._check_thread()
        if self._description is None:
            # No result set active: match stdlib by returning None.
            return None

        return self._next_row_unlocked()

    def _next_row_unlocked(self) -> tuple[Any, ...] | None:
        """Advance one row + apply ``row_factory`` without clearing messages or re-running guards.

        Snapshot _rows/_row_index/_row_factory to locals before the
        bounds check so a sibling-thread _cascade_cursors rewriting
        self._rows between check and read can't raise a bare IndexError
        outside the Error hierarchy.
        """
        rows = self._rows
        idx = self._row_index
        row_factory = self._row_factory
        if idx >= len(rows):
            return None
        row = rows[idx]
        # Apply factory BEFORE advancing _row_index so a factory raise
        # leaves the index unchanged (fetchmany's snapshot+len restore
        # depends on it).
        if row_factory is not None:
            try:
                transformed: tuple[Any, ...] = row_factory(self, row)
            except TypeError as exc:
                # Factory rejected the cursor arg (e.g. sqlite3.Row's
                # C type-check). Surface as DataError, not bare TypeError.
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
        """Fetch up to ``size`` rows (default ``arraysize``); [] when exhausted or no result set.

        size=0 returns [] without consuming (stdlib parity; diverges
        from psycopg3 which reads it as "use arraysize").
        """
        del self.messages[:]
        # See ``execute``'s prelude comment for the ordering rationale.
        self._check_closed()
        self._connection._check_thread()
        if self._description is None:
            # No result set active: match stdlib by returning [].
            return []

        if size is None:
            size = self._arraysize
        elif not _is_int_not_bool(size):
            # Reject non-int/bool: keep it in the Error hierarchy and
            # avoid True coercing to 1.
            raise ProgrammingError(f"fetchmany expects an int or None, got {type(size).__name__}")
        if size < 0:
            # Stdlib 3.13+ rejects negative size; older stdlib drained.
            raise ProgrammingError(f"fetchmany size must be non-negative; got {size}")

        # Snapshot _row_index; restore on cancel so partial iteration is
        # not silently consumed. _next_row_unlocked avoids re-clearing
        # messages / re-running guards per row.
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
        """Fetch all remaining rows; [] when exhausted or no result set (stdlib parity)."""
        del self.messages[:]
        # See ``execute``'s prelude comment for the ordering rationale.
        self._check_closed()
        self._connection._check_thread()
        if self._description is None:
            return []

        result = self._rows[self._row_index :]
        if self._row_factory is not None:
            # Apply factory BEFORE advancing _row_index so a raise leaves
            # the index unchanged (retry semantic, like fetchone).
            try:
                transformed = [self._row_factory(self, row) for row in result]
            except TypeError as exc:
                # See fetchone: wrap a cursor-rejecting factory as DataError.
                raise DataError(
                    f"row_factory call failed: {exc}",
                    code=None,
                    raw_message=str(exc),
                ) from exc
            except BaseException:
                # Other raises propagate; index unchanged.
                raise
            self._row_index = len(self._rows)
            return transformed
        self._row_index = len(self._rows)
        return result

    def close(self) -> None:
        """Close the cursor (idempotent).

        Does NOT run the thread-affinity check: close touches only
        in-memory fields, so __exit__ can close after a thread-moving
        body without masking the body's exception. Clears the result-set
        surface (description / _rows / _row_index) but PRESERVES rowcount
        and lastrowid (stdlib parity; SA reads lastrowid lazily after
        close). arraysize is also kept (it is a config hint). description
        becoming None is the one divergence from stdlib.
        """
        # PEP 249 §6.1.2: clear messages first. Suppress AttributeError
        # so close() from __exit__ after a body exception cannot supplant
        # it (PEP 343).
        with contextlib.suppress(AttributeError):
            del self.messages[:]
        if self._closed:
            return
        self._closed = True
        self._rows = []
        self._description = None
        self._row_index = 0
        # Swap _connection for a weakref.proxy so a retained closed cursor
        # does not pin the Connection's daemon loop. Access after the
        # Connection is GC'd raises ReferenceError (handled at call sites).
        with contextlib.suppress(
            TypeError
        ):  # pragma: no cover - Connection always supports weakref
            self._connection = weakref.proxy(self._connection)

    def setinputsizes(self, sizes: Sequence[Any] | None, /) -> None:
        """Set input sizes (no-op for dqlite)."""
        del self.messages[:]
        # Closed short-circuit BEFORE the validators so closed-state
        # behaviour is shape-independent, and BEFORE _check_thread so the
        # GC'd weakref.proxy parent can't raise ReferenceError. Also
        # short-circuit on a closed parent connection.
        if self._closed or self._connection._closed:
            return
        # Affinity before shape so a cross-thread caller gets the affinity
        # diagnostic (mirrors nextset / scroll / callproc).
        self._connection._check_thread()
        # None is a no-op (cross-driver parity for cur.setinputsizes(None)).
        if sizes is None:
            return
        if isinstance(sizes, (str, bytes, bytearray, memoryview)):
            # memoryview is a Sequence, so reject the quartet explicitly.
            raise ProgrammingError(
                f"setinputsizes expects a sequence of size hints, got {type(sizes).__name__}"
            )
        if not isinstance(sizes, Sequence):
            # ProgrammingError (not bare TypeError) to stay in the Error
            # hierarchy; the structural Sequence ABC admits deque/range.
            raise ProgrammingError(f"setinputsizes expects a Sequence, got {type(sizes).__name__}")

    def setoutputsize(self, size: int | None, column: int | None = None, /) -> None:
        """Set output size (no-op for dqlite). See ``setinputsizes``."""
        del self.messages[:]
        # See ``setinputsizes`` for the closed / affinity / None ordering.
        if self._closed or self._connection._closed:
            return
        self._connection._check_thread()
        if size is None:
            return
        if not _is_int_not_bool(size):
            raise ProgrammingError(f"setoutputsize expects an int, got {type(size).__name__}")
        if column is not None and not _is_int_not_bool(column):
            raise ProgrammingError(
                f"setoutputsize column expects an int or None, got {type(column).__name__}"
            )

    def callproc(self, procname: str, parameters: Sequence[Any] | None = None, /) -> NoReturn:
        """PEP 249 optional extension — not supported (no stored procedures).

        Defined (not absent), so it raises NotSupportedError rather than
        AttributeError; use try/except NotSupportedError for feature
        detection, not hasattr.
        """
        del self.messages[:]
        # _check_closed before _check_thread; see ``execute``'s prelude.
        self._check_closed()
        self._connection._check_thread()
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> NoReturn:
        """PEP 249 optional extension — not supported (no multiple result sets)."""
        del self.messages[:]
        self._check_closed()
        self._connection._check_thread()
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative", /) -> NoReturn:
        """PEP 249 optional extension — not supported (forward-only cursor)."""
        del self.messages[:]
        self._check_closed()
        self._connection._check_thread()
        # Validate value/mode before the unconditional raise so a caller
        # typo surfaces as a distinct caller-side bug. bool rejected
        # explicitly (project standard).
        if not _is_int_not_bool(value):
            raise ProgrammingError(
                f"scroll value must be an integer offset, got {type(value).__name__}"
            )
        if mode not in ("relative", "absolute"):
            raise ProgrammingError(f"scroll mode must be 'relative' or 'absolute', got {mode!r}")
        raise NotSupportedError("dqlite cursors are not scrollable")

    def executescript(self, sql_script: str, /) -> NoReturn:
        """stdlib-parity stub; dqlite has no script primitive, so it raises NotSupportedError."""
        del self.messages[:]
        self._check_closed()
        self._connection._check_thread()
        raise NotSupportedError(
            "dqlite does not support stdlib sqlite3 executescript; "
            "split the script and execute each statement individually"
        )

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        # sanitize_for_log neutralises attacker bidi/zero-width
        # codepoints in the address. Catch ReferenceError because the
        # GC'd weakref.proxy raises it BEFORE the getattr default,
        # outside the Error hierarchy.
        try:
            address = sanitize_for_log(str(getattr(self._connection, "_address", "?")))
        except ReferenceError:
            address = "?"
        return f"<Cursor address={address!r} rowcount={self._rowcount} {state} at 0x{id(self):x}>"

    def __reduce__(self) -> NoReturn:
        # Cursors reference a live Connection (socket + loop thread) that
        # can't be pickled; surface a clear TypeError instead of the
        # default "cannot pickle '_thread.lock'".
        raise TypeError(
            f"cannot pickle {type(self).__name__!r} object — cursors "
            "hold a reference to a live driver Connection; use "
            "fetchall()/fetchmany() to materialise rows before crossing "
            "a process boundary"
        )

    def __iter__(self) -> Self:
        # Clear messages for the iter(cur) entry point itself. Affinity
        # is checked at first __next__ (→ fetchone → _check_thread), like
        # stdlib's bare ``return self``.
        del self.messages[:]
        return self

    def __next__(self) -> tuple[Any, ...]:
        """Advance the cursor by one row.

        A row_factory raise does NOT advance the index, so the next
        __next__ retries the SAME row (the cursor wedges on it until a
        fresh execute) — required by fetchmany's snapshot+len retry.
        """
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> Self:
        # Clear messages unconditionally (even when closed), like __iter__.
        del self.messages[:]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()
