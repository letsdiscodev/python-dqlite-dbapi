"""Async cursor."""

import asyncio
import contextlib
from collections.abc import Callable, Iterable, Iterator, Sequence
from functools import partial
from types import TracebackType
from typing import TYPE_CHECKING, Any, Final, NoReturn, Self

from dqlitedbapi import _sql
from dqlitedbapi._busy import retry_on_busy
from dqlitedbapi.exceptions import (
    AMBIGUOUS_COMMIT_CODES,
    AmbiguousCommitError,
    DataError,
    InterfaceError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    call,
)
from dqlitedbapi.types import (
    UNKNOWN,
    DBAPIType,
    Description,
    RowFactory,
    adapt_bind_param,
    datetime_from_iso8601,
    datetime_from_unixtime,
    is_int_not_bool,
)
from dqlitewire import ValueType

if TYPE_CHECKING:
    from dqliteclient import DqliteConnection
    from dqlitedbapi.aio.connection import AsyncConnection

__all__ = ["AsyncCursor"]

_CONVERTERS: Final[dict[int, Callable[[Any], Any]]] = {
    int(ValueType.ISO8601): datetime_from_iso8601,
    int(ValueType.UNIXTIME): datetime_from_unixtime,
}
_YIELD_EVERY: Final[int] = 4096
_INT64_SIGN_BIT: Final[int] = 1 << 63


def _signed(value: int) -> int:
    """The wire carries rowids and counts as uint64; negative values arrive wrapped."""
    return value - (1 << 64) if value >= _INT64_SIGN_BIT else value


def _adapt(params: Sequence[Any] | None) -> list[Any] | None:
    return None if params is None else [adapt_bind_param(p) for p in params]


def _type_codes(
    ncols: int, column_types: Sequence[int], row_types: Sequence[Sequence[int]]
) -> list[int | DBAPIType]:
    """Per-column type code: the first non-NULL tag down the column, else ``UNKNOWN``."""
    codes: list[int | DBAPIType] = []
    for i in range(ncols):
        code: int | DBAPIType = UNKNOWN
        for types in (column_types, *row_types):
            if i < len(types) and types[i] != ValueType.NULL:
                code = int(types[i])
                break
        codes.append(code)
    return codes


async def _convert_rows(
    rows: Sequence[Sequence[Any]], row_types: Sequence[Sequence[int]]
) -> list[tuple[Any, ...]]:
    converter_codes = _CONVERTERS.keys()
    out: list[tuple[Any, ...]] = []
    for i, row in enumerate(rows):
        types = row_types[i] if i < len(row_types) else ()
        if converter_codes.isdisjoint(types):
            out.append(tuple(row))
        else:
            out.append(
                tuple(
                    _CONVERTERS[t](v) if v is not None and t in _CONVERTERS else v
                    for v, t in zip(row, types, strict=False)
                )
            )
        if (i + 1) % _YIELD_EVERY == 0:
            await asyncio.sleep(0)
    return out


class AsyncCursor:
    """Cursor over an :class:`AsyncConnection`. Results are fully buffered at execute time."""

    __slots__ = (
        "__weakref__",
        "_arraysize",
        "_closed",
        "_completed_iterations",
        "_connection",
        "_description",
        "_executing",
        "_index",
        "_lastrowid",
        "_row_factory",
        "_rowcount",
        "_rows",
        "messages",
    )

    def __init__(self, connection: "AsyncConnection") -> None:
        self._connection = connection
        self._description: Description = None
        self._rows: list[tuple[Any, ...]] = []
        self._index = 0
        self._rowcount = -1
        self._lastrowid: int | None = None
        self._arraysize = 1
        self._closed = False
        self._completed_iterations = 0
        self._executing = False
        self._row_factory: RowFactory | None = connection.row_factory
        self.messages: list[tuple[type[Exception], Exception]] = []

    # -- state -----------------------------------------------------------------

    @property
    def connection(self) -> "AsyncConnection":
        return self._connection

    @property
    def description(self) -> Description:
        return self._description

    @property
    def rowcount(self) -> int:
        """Rows affected by DML, rows returned by a query, else -1."""
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        """Rowid of the last INSERT/REPLACE on this cursor; sticky across other statements."""
        return self._lastrowid

    @property
    def rownumber(self) -> int | None:
        return None if self._description is None else self._index

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        if not is_int_not_bool(value) or value < 0:
            raise ProgrammingError(f"arraysize must be a non-negative int, got {value!r}")
        self._arraysize = value

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def row_factory(self) -> RowFactory | None:
        return self._row_factory

    @row_factory.setter
    def row_factory(self, value: object) -> None:
        if value is not None and not callable(value):
            raise ProgrammingError(
                f"row_factory must be callable or None, got {type(value).__name__}"
            )
        self._row_factory = value

    @property
    def completed_iterations(self) -> int:
        """``executemany`` iterations that completed during the last call."""
        return self._completed_iterations

    def _check_open(self) -> None:
        if self._closed:
            raise InterfaceError(f"Cursor is closed (id={id(self)})")

    def _reset(self) -> None:
        self._description = None
        self._rows = []
        self._index = 0
        self._rowcount = -1
        self._completed_iterations = 0

    # -- execution -------------------------------------------------------------

    async def execute(self, operation: str, parameters: Sequence[Any] | None = None, /) -> Self:
        self._check_open()
        sql = _sql.validate_operation(operation)
        _sql.validate_parameters(parameters)
        with self._executing_guard():
            self._reset()
            await self._execute(sql, parameters)
        return self

    @contextlib.contextmanager
    def _executing_guard(self) -> Iterator[None]:
        if self._executing:
            raise InterfaceError(
                f"cursor is already executing in another task (id={id(self)}); "
                "use one cursor per task"
            )
        self._executing = True
        try:
            yield
        finally:
            self._executing = False

    async def _execute(self, sql: str, parameters: Sequence[Any] | None) -> None:
        conn = self._connection
        matched, new_ms = _sql.busy_timeout_pragma(sql)
        if matched:
            if new_ms is not None:
                conn.busy_timeout = new_ms / 1000
            self._description = (
                ("busy_timeout", int(ValueType.INTEGER), None, None, None, None, None),
            )
            self._rows = [(round(conn.busy_timeout * 1000),)]
            return
        _sql.check_placeholder_count(sql, parameters)
        sql = _sql.rewrite_begin(sql, conn.session_mode)
        statement = _sql.classify(sql)
        params = _adapt(parameters)

        async def attempt() -> None:
            async with conn._operation() as client:
                self._check_open()
                await self._run(client, statement, sql, params)

        await retry_on_busy(conn.busy_timeout, attempt)

    async def executemany(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]], /
    ) -> Self:
        """Run ``operation`` once per parameter set. ``RETURNING`` rows accumulate across
        iterations; ``rowcount`` is the total. Iterations autocommit individually unless a
        transaction is open."""
        self._check_open()
        _sql.validate_executemany_sequence(seq_of_parameters)
        sql = _sql.validate_operation(operation)
        with self._executing_guard():
            self._reset()
            await self._executemany(sql, seq_of_parameters)
        return self

    async def _executemany(self, sql: str, seq_of_parameters: Iterable[Sequence[Any]]) -> None:
        statement = _sql.classify(sql)
        if statement.is_tx_control:
            raise ProgrammingError(
                f"executemany() does not accept {statement.keyword}; "
                "transaction control takes no parameters, use execute()"
            )
        if not statement.is_dml:
            raise ProgrammingError(
                "executemany() only accepts INSERT / UPDATE / DELETE / REPLACE; "
                "use execute() for queries and PRAGMA"
            )
        expected = _sql.placeholder_count(sql)
        conn = self._connection
        lastrowid = self._lastrowid
        description: Description = None
        rows: list[tuple[Any, ...]] = []
        total = 0
        ran = 0
        try:
            async with conn._operation() as client:
                for params in seq_of_parameters:
                    self._check_open()
                    _sql.validate_parameters(params)
                    supplied = 0 if params is None else len(params)
                    if supplied != expected:
                        raise ProgrammingError(
                            f"Incorrect number of bindings supplied. The current statement "
                            f"uses {expected}, and there are {supplied} supplied."
                        )
                    adapted = _adapt(params)
                    self._description = None
                    self._rows = []
                    await retry_on_busy(
                        conn.busy_timeout, partial(self._run, client, statement, sql, adapted)
                    )
                    if self._description is not None:
                        description = description or self._description
                        rows.extend(self._rows)
                        total += len(self._rows)
                        cap = conn.max_total_rows
                        if cap is not None and len(rows) > cap:
                            raise DataError(
                                f"executemany accumulated {len(rows)} RETURNING rows; "
                                f"exceeds max_total_rows={cap}"
                            )
                    elif self._rowcount >= 0:
                        total += self._rowcount
                    ran += 1
                    self._completed_iterations = ran
        except BaseException:
            self._description = None
            self._rows = []
            self._index = 0
            self._rowcount = -1
            self._lastrowid = lastrowid
            raise
        self._lastrowid = lastrowid
        self._description = description
        self._rows = rows
        self._index = 0
        self._rowcount = total if ran else 0

    async def _run(
        self,
        client: "DqliteConnection",
        statement: _sql.Statement,
        sql: str,
        params: list[Any] | None,
    ) -> None:
        if statement.returns_rows:
            columns, column_types, row_types, rows = await call(client.query_raw_typed(sql, params))
            if not columns:  # write-form PRAGMA: no result set
                return
            codes = _type_codes(len(columns), column_types, row_types)
            self._description = tuple(
                (name, codes[i], None, None, None, None, None) for i, name in enumerate(columns)
            )
            self._rows = await _convert_rows(rows, row_types)
            self._index = 0
            self._rowcount = -1 if statement.is_pragma else len(rows)
            return
        try:
            last_id, affected = await call(client.execute(sql, params))
        except OperationalError as exc:
            if statement.is_commit and exc.code in AMBIGUOUS_COMMIT_CODES:
                raise AmbiguousCommitError(
                    "ambiguous commit: leadership lost during COMMIT; the write may or may "
                    f"not have been persisted. Original: {exc}",
                    code=exc.code,
                    raw_message=exc.raw_message,
                ) from exc
            raise
        if statement.is_insert:
            self._lastrowid = _signed(last_id)
        self._rowcount = _signed(affected) if statement.is_dml else -1

    # -- fetching (sync internals shared with the sync cursor) -------------------

    def _fetchone(self, owner: object) -> Any:
        if self._description is None or self._index >= len(self._rows):
            return None
        row: Any = self._rows[self._index]
        if self._row_factory is not None:
            try:
                row = self._row_factory(owner, row)
            except TypeError as exc:
                raise DataError(f"row_factory call failed: {exc}") from exc
        self._index += 1
        return row

    def _fetchmany(self, owner: object, size: int | None) -> list[Any]:
        if size is None:
            size = self._arraysize
        elif not is_int_not_bool(size) or size < 0:
            raise ProgrammingError(f"fetchmany size must be a non-negative int, got {size!r}")
        out: list[Any] = []
        for _ in range(size):
            row = self._fetchone(owner)
            if row is None:
                break
            out.append(row)
        return out

    def _fetchall(self, owner: object) -> list[Any]:
        out: list[Any] = []
        while (row := self._fetchone(owner)) is not None:
            out.append(row)
        return out

    async def fetchone(self) -> Any:
        """Next row, or ``None`` when exhausted or when there is no result set."""
        self._check_open()
        return self._fetchone(self)

    async def fetchmany(self, size: int | None = None) -> list[Any]:
        self._check_open()
        return self._fetchmany(self, size)

    async def fetchall(self) -> list[Any]:
        self._check_open()
        return self._fetchall(self)

    def drain_rows(self) -> list[tuple[Any, ...]]:
        """Hand over the raw row buffer (no ``row_factory``) and leave the cursor empty."""
        rows, self._rows, self._index = self._rows, [], 0
        return rows

    # -- lifecycle -------------------------------------------------------------

    def close(self) -> None:
        """Close the cursor; ``rowcount`` and ``lastrowid`` stay readable. Idempotent."""
        self._closed = True
        self._description = None
        self._rows = []
        self._index = 0

    async def aclose(self) -> None:
        self.close()

    def setinputsizes(self, sizes: Sequence[Any] | None, /) -> None:
        """Accepted for PEP 249 parity; dqlite takes no size hints."""
        if sizes is None:
            return
        if isinstance(sizes, str | bytes | bytearray | memoryview):
            raise ProgrammingError(
                f"setinputsizes expects a sequence of size hints, got {type(sizes).__name__}"
            )
        if not isinstance(sizes, Sequence):
            raise ProgrammingError(f"setinputsizes expects a Sequence, got {type(sizes).__name__}")

    def setoutputsize(self, size: int | None, column: int | None = None, /) -> None:
        """Accepted for PEP 249 parity; dqlite takes no size hints."""
        if size is not None and not is_int_not_bool(size):
            raise ProgrammingError(f"setoutputsize expects an int, got {type(size).__name__}")
        if column is not None and not is_int_not_bool(column):
            raise ProgrammingError(
                f"setoutputsize column expects an int, got {type(column).__name__}"
            )

    def callproc(self, procname: str, parameters: Sequence[Any] | None = None, /) -> NoReturn:
        self._check_open()
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> NoReturn:
        self._check_open()
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative", /) -> NoReturn:
        self._check_open()
        raise NotSupportedError("dqlite cursors are not scrollable")

    def executescript(self, sql_script: str, /) -> NoReturn:
        self._check_open()
        raise NotSupportedError(
            "executescript() is not supported: there is no multi-statement primitive; "
            "execute one statement at a time"
        )

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> Any:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<AsyncCursor rowcount={self._rowcount} {state} at 0x{id(self):x}>"

    def __reduce__(self) -> NoReturn:
        raise TypeError(
            f"cannot pickle {type(self).__name__!r}: cursors reference a live connection; "
            "fetch the rows and pickle those instead"
        )
