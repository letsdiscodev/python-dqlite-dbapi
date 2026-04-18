"""Async cursor implementation for dqlite."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from dqlitedbapi.cursor import _convert_params, _convert_row, _strip_leading_comments
from dqlitedbapi.exceptions import InterfaceError

if TYPE_CHECKING:
    from dqlitedbapi.aio.connection import AsyncConnection


__all__ = ["AsyncCursor", "_strip_leading_comments"]


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

    @property
    def description(
        self,
    ) -> list[tuple[str, int | None, None, None, None, None, None]] | None:
        """Column descriptions for the last query."""
        return self._description

    @property
    def rowcount(self) -> int:
        """Number of rows affected by the last execute."""
        return self._rowcount

    @property
    def lastrowid(self) -> int | None:
        """Row ID of the last inserted row."""
        return self._lastrowid

    @property
    def arraysize(self) -> int:
        """Number of rows to fetch at a time with fetchmany()."""
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
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
        self._check_closed()

        conn = await self._connection._ensure_connection()
        params = _convert_params(parameters)

        # Determine if this is a query that returns rows.
        # Note: WITH ... INSERT/UPDATE/DELETE (without RETURNING) will be
        # misrouted to query_raw_typed. This is a known limitation of the heuristic.
        normalized = _strip_leading_comments(operation).upper()
        is_query = normalized.startswith(("SELECT", "PRAGMA", "EXPLAIN", "WITH")) or (
            " RETURNING " in normalized or normalized.endswith(" RETURNING")
        )

        async with self._connection._op_lock:
            if is_query:
                columns, column_types, rows = await conn.query_raw_typed(operation, params)
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
                last_id, affected = await conn.execute(operation, params)
                self._lastrowid = last_id
                self._rowcount = affected
                self._description = None
                self._rows = []

        return self

    async def executemany(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> "AsyncCursor":
        """Execute a database operation multiple times."""
        self._check_closed()

        total_affected = 0
        for params in seq_of_parameters:
            await self.execute(operation, params)
            if self._rowcount >= 0:
                total_affected += self._rowcount
        self._rowcount = total_affected
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
        """Close the cursor."""
        self._closed = True
        self._rows = []
        self._description = None

    def setinputsizes(self, sizes: Sequence[int | None]) -> None:
        """Set input sizes (no-op for dqlite)."""
        pass

    def setoutputsize(self, size: int, column: int | None = None) -> None:
        """Set output size (no-op for dqlite)."""
        pass

    def __aiter__(self) -> "AsyncCursor":
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        row = await self.fetchone()
        if row is None:
            raise StopAsyncIteration
        return row

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
