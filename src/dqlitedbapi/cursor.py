"""PEP 249 Cursor implementation for dqlite."""

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from dqlitedbapi.exceptions import InterfaceError, InternalError, OperationalError

if TYPE_CHECKING:
    from dqlitedbapi.connection import Connection


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


class Cursor:
    """PEP 249 compliant database cursor."""

    def __init__(self, connection: "Connection") -> None:
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
        """Column descriptions for the last query.

        Returns a list of 7-tuples:
        (name, type_code, display_size, internal_size, precision, scale, null_ok)

        Only name is populated; others are None for compatibility.
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

    def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> "Cursor":
        """Execute a database operation (query or command)."""
        self._check_closed()

        self._connection._run_sync(self._execute_async(operation, parameters))
        return self

    async def _execute_async(self, operation: str, parameters: Sequence[Any] | None = None) -> None:
        """Async implementation of execute."""
        conn = await self._connection._get_async_connection()
        params = list(parameters) if parameters is not None else None

        # Determine if this is a query that returns rows.
        # Note: WITH ... INSERT/UPDATE/DELETE (without RETURNING) will be
        # misrouted to query_sql. This is a known limitation of the heuristic.
        normalized = _strip_leading_comments(operation).upper()
        is_query = normalized.startswith(("SELECT", "PRAGMA", "EXPLAIN", "WITH")) or (
            " RETURNING " in normalized or normalized.endswith(" RETURNING")
        )

        if conn._protocol is None or conn._db_id is None:
            raise InternalError("Connection protocol not initialized")

        try:
            if is_query:
                columns, rows = await conn._protocol.query_sql(conn._db_id, operation, params)
                self._description = [(name, None, None, None, None, None, None) for name in columns]
                self._rows = [tuple(row) for row in rows]
                self._row_index = 0
                self._rowcount = len(rows)
            else:
                last_id, affected = await conn._protocol.exec_sql(conn._db_id, operation, params)
                self._lastrowid = last_id
                self._rowcount = affected
                self._description = None
                self._rows = []
        except (OperationalError, InterfaceError, InternalError):
            raise
        except Exception as e:
            raise OperationalError(str(e)) from e

    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]) -> "Cursor":
        """Execute a database operation multiple times."""
        self._check_closed()

        self._connection._run_sync(self._executemany_async(operation, seq_of_parameters))
        return self

    async def _executemany_async(
        self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]
    ) -> None:
        """Async implementation of executemany."""
        total_affected = 0
        for params in seq_of_parameters:
            await self._execute_async(operation, params)
            if self._rowcount >= 0:
                total_affected += self._rowcount
        self._rowcount = total_affected

    def fetchone(self) -> tuple[Any, ...] | None:
        """Fetch the next row of a query result set."""
        self._check_closed()

        if not self._rows or self._row_index >= len(self._rows):
            return None

        row = self._rows[self._row_index]
        self._row_index += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Fetch the next set of rows of a query result."""
        self._check_closed()

        if size is None:
            size = self._arraysize

        result: list[tuple[Any, ...]] = []
        for _ in range(size):
            row = self.fetchone()
            if row is None:
                break
            result.append(row)

        return result

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Fetch all remaining rows of a query result."""
        self._check_closed()

        result = self._rows[self._row_index :]
        self._row_index = len(self._rows)
        return result

    def close(self) -> None:
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

    def __iter__(self) -> "Cursor":
        return self

    def __next__(self) -> tuple[Any, ...]:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
