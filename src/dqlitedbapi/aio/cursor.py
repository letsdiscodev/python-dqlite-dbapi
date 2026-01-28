"""Async cursor implementation for dqlite."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from dqlitedbapi.exceptions import InterfaceError

if TYPE_CHECKING:
    from dqlitedbapi.aio.connection import AsyncConnection


class AsyncCursor:
    """Async database cursor."""

    def __init__(self, connection: "AsyncConnection") -> None:
        self._connection = connection
        self._description: list[tuple[str, None, None, None, None, None, None]] | None = None
        self._rowcount = -1
        self._arraysize = 1
        self._rows: list[tuple[Any, ...]] = []
        self._row_index = 0
        self._closed = False
        self._lastrowid: int | None = None

    @property
    def description(
        self,
    ) -> list[tuple[str, None, None, None, None, None, None]] | None:
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
        """Execute a database operation (query or command)."""
        self._check_closed()

        conn = self._connection._async_conn
        if conn is None:
            raise InterfaceError("Connection is not open")

        params = list(parameters) if parameters else None

        # Determine if this is a SELECT query
        is_query = operation.strip().upper().startswith(("SELECT", "PRAGMA", "EXPLAIN"))

        if is_query:
            assert conn._protocol is not None and conn._db_id is not None
            columns, rows = await conn._protocol.query_sql(conn._db_id, operation, params)
            self._description = [(name, None, None, None, None, None, None) for name in columns]
            self._rows = [tuple(row) for row in rows]
            self._row_index = 0
            self._rowcount = len(rows)
        else:
            assert conn._protocol is not None and conn._db_id is not None
            last_id, affected = await conn._protocol.exec_sql(conn._db_id, operation, params)
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

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set."""
        self._check_closed()

        if not self._rows or self._row_index >= len(self._rows):
            return None

        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows of a query result."""
        self._check_closed()

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
