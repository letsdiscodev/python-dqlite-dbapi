"""Async cursor implementation for dqlite."""

from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

from dqlitedbapi.cursor import (
    _call_client,
    _convert_params,
    _convert_row,
    _ExecuteManyAccumulator,
    _is_row_returning,
)
from dqlitedbapi.exceptions import InterfaceError, NotSupportedError, ProgrammingError

if TYPE_CHECKING:
    from dqlitedbapi.aio.connection import AsyncConnection


__all__ = ["AsyncCursor"]


class AsyncCursor:
    """Async database cursor."""

    def __init__(self, connection: "AsyncConnection") -> None:
        self._connection = connection
        self._description: list[tuple[str, int | None, None, None, None, None, None]] | None = None
        self._rowcount = -1
        self._arraysize = 1
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self._closed = False
        self._lastrowid: int | None = None
        # PEP 249 optional extension; see Cursor.messages.
        self.messages: list[tuple[type, Any]] = []

    @property
    def connection(self) -> "AsyncConnection":
        """The AsyncConnection this cursor was created from.

        PEP 249 optional extension. Read-only.
        """
        return self._connection

    @property
    def description(
        self,
    ) -> list[tuple[str, int | None, None, None, None, None, None]] | None:
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
        """Row ID of the last inserted row."""
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
        if value < 1:
            raise ProgrammingError(f"arraysize must be >= 1, got {value}")
        self._arraysize = value

    def _check_closed(self) -> None:
        if self._closed:
            raise InterfaceError("Cursor is closed")

    async def execute(
        self, operation: str, parameters: Sequence[Any] | None = None
    ) -> "AsyncCursor":
        """Execute a database operation (query or command).

        Routes through DqliteConnection's public API (execute/query_raw_typed)
        which goes through _run_protocol(), providing the _in_use guard,
        connection invalidation on fatal errors, and leader-change detection.
        The _op_lock serializes operations on the same connection.
        """
        # Fast-path guard outside the lock so we fail quickly on an
        # already-closed cursor without taking the lock.
        self._check_closed()

        is_query = _is_row_returning(operation)
        params = _convert_params(parameters)
        _, op_lock = self._connection._ensure_locks()
        async with op_lock:
            # Re-check after acquiring the lock so that a concurrent
            # ``cursor.close()`` / ``connection.close()`` that reaches the
            # closed flag first wins deterministically. Without the
            # re-check, a cursor closed between the fast-path guard and
            # the lock acquisition reports the race as
            # "connection has been invalidated" or "protocol is None"
            # rather than the sharper "Cursor is closed" / "Connection
            # is closed" that the caller expects.
            self._check_closed()
            conn = await self._connection._ensure_connection()
            # ``_ensure_connection`` awaits, so close() can still race
            # against this window. Re-check once more before touching
            # the wire.
            self._check_closed()
            if is_query:
                columns, column_types, rows = await _call_client(
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
                self._rows = [_convert_row(row, column_types) for row in rows]
                self._row_index = 0
                self._rowcount = len(rows)
            else:
                last_id, affected = await _call_client(conn.execute(operation, params))
                self._lastrowid = last_id
                self._rowcount = affected
                self._description = None
                self._rows = []
                # Parity with the SELECT branch and with executemany:
                # every execute must leave the cursor at row 0 of its
                # (possibly empty) result set so a subsequent SELECT
                # iterator starts from a clean state.
                self._row_index = 0

        return self

    async def executemany(
        self, operation: str, seq_of_parameters: Iterable[Sequence[Any]]
    ) -> "AsyncCursor":
        """Execute a database operation multiple times.

        An empty ``seq_of_parameters`` must not leave stale SELECT
        state around: reset description / rows so callers can't
        confuse an empty executemany with a preceding SELECT.

        For statements with a RETURNING clause, rows produced by each
        iteration are accumulated into ``_rows`` so a subsequent
        ``fetchall`` yields every returned row across parameter sets.
        """
        self._check_closed()

        self._description = None
        self._rows = []
        self._row_index = 0
        acc = _ExecuteManyAccumulator()
        for params in seq_of_parameters:
            await self.execute(operation, params)
            acc.push(self)
        acc.apply(self)
        return self

    def _check_result_set(self) -> None:
        if self._description is None:
            raise InterfaceError("No result set: execute a query before fetching")

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set."""
        self._check_closed()
        self._check_result_set()

        if self._row_index >= len(self._rows):
            return None

        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows of a query result."""
        self._check_closed()
        self._check_result_set()

        if size is None:
            size = self._arraysize
        if size < 0:
            # See sync Cursor.fetchmany: silently returning [] on a
            # negative size hides caller bugs.
            raise ProgrammingError(f"fetchmany size must be non-negative, got {size}")

        result: list[tuple[Any, ...]] = []
        for _ in range(size):
            row = await self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result."""
        self._check_closed()
        self._check_result_set()

        result = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return result

    async def close(self) -> None:
        """Close the cursor.

        Idempotent: a second call is a no-op.
        """
        if self._closed:
            return
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

        Sync despite the cursor being async: the method raises
        unconditionally, so wrapping it in a coroutine has no value and
        would diverge from the sync siblings (``nextset`` / ``scroll``)
        and from the SQLAlchemy adapter (``sqlalchemy-dqlite``), which
        both expose these as plain methods.
        """
        raise NotSupportedError("dqlite does not support stored procedures")

    def nextset(self) -> bool | None:
        """PEP 249 optional extension — not supported."""
        raise NotSupportedError("dqlite does not support multiple result sets")

    def scroll(self, value: int, mode: str = "relative") -> None:
        """PEP 249 optional extension — not supported."""
        raise NotSupportedError("dqlite cursors are not scrollable")

    def __repr__(self) -> str:
        state = "closed" if self._closed else "open"
        return f"<AsyncCursor rowcount={self._rowcount} {state}>"

    def __aiter__(self) -> "AsyncCursor":
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
