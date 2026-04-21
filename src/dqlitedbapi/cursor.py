"""PEP 249 Cursor implementation for dqlite."""

from collections.abc import Callable, Coroutine, Iterable, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.exceptions import (
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
from dqlitewire.constants import ValueType

__all__ = ["Cursor"]


# SQLite primary error code 19 (SQLITE_CONSTRAINT) plus its extended
# family (SQLITE_CONSTRAINT_CHECK = 275, UNIQUE = 2067, NOT_NULL = 1299,
# FOREIGN_KEY = 787, etc.) all share ``code & 0xFF == 19``. PEP 249
# mandates IntegrityError for these, so map them here rather than
# leaving every caller to inspect the code themselves.
_SQLITE_CONSTRAINT = 19

# SQLite primary error code 2 (SQLITE_INTERNAL). stdlib ``sqlite3``
# routes this to ``sqlite3.InternalError`` (CPython's
# ``_pysqlite_seterror``); PEP 249 defines ``InternalError`` for exactly
# this purpose — "internal errors of the database, e.g. the cursor is
# not valid anymore".
_SQLITE_INTERNAL = 2

# Registry of primary-code → PEP 249 class. Keep the default
# (OperationalError) outside the dict so adding a code is one line.
_CODE_TO_EXCEPTION: dict[int, type[OperationalError | IntegrityError | InternalError]] = {
    _SQLITE_CONSTRAINT: IntegrityError,
    _SQLITE_INTERNAL: InternalError,
}


def _classify_operational(
    code: int | None,
) -> type[OperationalError | IntegrityError | InternalError]:
    """Pick a PEP 249 exception class from a SQLite error code.

    Returns OperationalError for unknown / unmapped codes so the
    existing "anything can surface as OperationalError" contract
    holds; returns IntegrityError for the SQLITE_CONSTRAINT family and
    InternalError for the SQLITE_INTERNAL family.
    """
    if code is None:
        return OperationalError
    return _CODE_TO_EXCEPTION.get(code & 0xFF, OperationalError)


async def _call_client(coro: Coroutine[Any, Any, Any]) -> Any:
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
        exc_cls = _classify_operational(e.code)
        raise exc_cls(str(e), code=e.code) from e
    except _client_exc.DqliteConnectionError as e:
        raise OperationalError(str(e)) from e
    except _client_exc.ClusterError as e:
        raise OperationalError(str(e)) from e
    except _client_exc.ProtocolError as e:
        raise InterfaceError(str(e)) from e
    except _client_exc.DataError as e:
        raise DataError(str(e)) from e
    except _client_exc.InterfaceError as e:
        raise InterfaceError(str(e)) from e
    except _client_exc.DqliteError as e:
        # Catch-all for any future subclass of DqliteError not enumerated
        # above. Surface as InterfaceError rather than leaking to the
        # caller as a non-DBAPI exception.
        raise InterfaceError(f"unrecognized client error ({type(e).__name__}): {e}") from e


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
                return s
            s = s[end + 2 :].strip()
        else:
            break
    return s


_ROW_RETURNING_PREFIXES = ("SELECT", "VALUES", "PRAGMA", "EXPLAIN", "WITH")


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

    __slots__ = ("total_affected", "rows", "description")

    def __init__(self) -> None:
        self.total_affected = 0
        self.rows: list[tuple[Any, ...]] = []
        self.description: _Description = None

    def push(self, cursor: Any) -> None:
        """Record one iteration's output into the accumulator.

        Accepts either :class:`Cursor` or :class:`AsyncCursor`; both
        expose the same ``_rowcount`` / ``_description`` / ``_rows``
        attributes.
        """
        if cursor._rowcount >= 0:
            self.total_affected += cursor._rowcount
        if cursor._description is not None:
            if self.description is None:
                self.description = cursor._description
            self.rows.extend(cursor._rows)

    def apply(self, cursor: Any) -> None:
        """Materialise the accumulator's state onto the cursor.

        ``description is None`` means none of the iterations produced a
        result set (plain DML without RETURNING); leave ``_description``
        / ``_rows`` as reset. Inherit the first-seen description
        otherwise.
        """
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
    normalized = _strip_leading_comments(sql).upper().lstrip("(")
    if normalized.startswith(_ROW_RETURNING_PREFIXES):
        return True
    return " RETURNING " in normalized or normalized.endswith(" RETURNING")


class Cursor:
    """PEP 249 compliant database cursor."""

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
        self.messages: list[tuple[type, Any]] = []

    @property
    def connection(self) -> "Connection":
        """The Connection this Cursor was created from.

        PEP 249 optional extension. Read-only.
        """
        return self._connection

    @property
    def description(self) -> _Description:
        """Column descriptions for the last query.

        Returns a list of 7-tuples:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)

        ``type_code`` is the wire-level ``ValueType`` integer from the first
        result frame (e.g. 10 for ISO8601, 9 for UNIXTIME). The other fields
        are None — dqlite doesn't expose them.

        Returns a fresh shallow copy each call so that a caller
        mutating the list (e.g. ``cursor.description.clear()``) can't
        corrupt the cursor's internal state.
        """
        if self._description is None:
            return None
        return list(self._description)

    @property
    def rowcount(self) -> int:
        """Number of rows affected by the last execute.

        Returns -1 if not applicable or unknown.
        """
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        """ROWID of the most recent successful INSERT on the connection.

        Returns ``None`` before the first statement runs on this cursor.
        Per SQLite semantics the value reflects the *connection*'s last
        INSERT — it is not cleared by UPDATE / DELETE / DDL, nor is it
        scoped to this cursor. Matches :attr:`sqlite3.Cursor.lastrowid`.

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

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError("Cursor is closed")

    def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> "Cursor":
        """Execute a database operation (query or command).

        Returns ``self`` so callers can chain ``.fetchall()`` etc.
        """
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()

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
            self._description = [
                (
                    name,
                    column_types[i] if i < len(column_types) else None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )
                for i, name in enumerate(columns)
            ]
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
            self._lastrowid = last_id
            self._rowcount = affected
            self._description = None
            self._rows = []
            # Parity with the SELECT branch and with executemany: every
            # execute must leave the cursor at row 0 of its (possibly
            # empty) result set so a subsequent SELECT iterator starts
            # from a clean state.
            self._row_index = 0

    def executemany(self, operation: str, seq_of_parameters: Iterable[Sequence[Any]]) -> "Cursor":
        """Execute a database operation multiple times."""
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()

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
        """
        self._description = None
        self._rows = []
        self._row_index = 0
        acc = _ExecuteManyAccumulator()
        for params in seq_of_parameters:
            await self._execute_async(operation, params)
            acc.push(self)
        acc.apply(self)

    def _check_result_set(self) -> None:
        if self._description is None:
            raise InterfaceError("No result set: execute a query before fetching")

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set.

        Returns ``None`` when no more rows are available.
        """
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()
        self._check_result_set()

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

        if size is None:
            size = self._arraysize
        if size < 0:
            # Previously ``range(-5)`` silently returned [] — hid caller
            # bugs. ``arraysize`` setter already validates >= 1; mirror
            # that here.
            raise ProgrammingError(f"fetchmany size must be non-negative, got {size}")

        result: list[tuple[Any, ...]] = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result.

        Returns an empty list when the cursor has no more rows.
        """
        del self.messages[:]
        self._connection._check_thread()
        self._check_closed()
        self._check_result_set()

        result = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return result

    def close(self) -> None:
        """Close the cursor.

        Idempotent: a second call is a no-op. PEP 249 mandates that
        operations on a closed cursor raise an Error, but the close
        itself is permitted to be repeated.
        """
        if self._closed:
            return
        self._connection._check_thread()
        self._closed = True
        self._rows = []
        self._description = None

    def setinputsizes(self, sizes: Sequence[int | None]) -> None:
        """Set input sizes (no-op for dqlite)."""
        pass

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Set output size (no-op for dqlite)."""
        pass

    def callproc(
        self, procname: str, parameters: Sequence[Any] | None = None
    ) -> Sequence[Any] | None:
        """PEP 249 optional extension — not supported.

        dqlite (and SQLite) have no stored-procedure concept.
        """
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> bool | None:
        """PEP 249 optional extension — not supported.

        dqlite's wire protocol does not return multiple result sets.
        """
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative") -> None:
        """PEP 249 optional extension — not supported.

        The dqlite cursor is forward-only; rows are buffered from a
        streamed wire response.
        """
        raise NotSupportedError("dqlite cursors are not scrollable")

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<Cursor rowcount={self._rowcount} {state}>"

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
