"""Async connection implementation for dqlite."""

from typing import Any

from dqliteclient import DqliteConnection
from dqlitedbapi.aio.cursor import AsyncCursor
from dqlitedbapi.exceptions import InterfaceError, OperationalError


class AsyncConnection:
    """Async database connection."""

    def __init__(
        self,
        address: str,
        *,
        database: str = "default",
        timeout: float = 10.0,
    ) -> None:
        """Initialize connection (does not connect yet).

        Args:
            address: Node address in "host:port" format
            database: Database name to open
            timeout: Connection timeout in seconds
        """
        self._address = address
        self._database = database
        self._timeout = timeout
        self._async_conn: DqliteConnection | None = None
        self._closed = False

    async def connect(self) -> None:
        """Establish the connection."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            return

        self._async_conn = DqliteConnection(
            self._address,
            database=self._database,
            timeout=self._timeout,
        )
        try:
            await self._async_conn.connect()
        except Exception as e:
            self._async_conn = None
            raise OperationalError(f"Failed to connect: {e}") from e

    async def close(self) -> None:
        """Close the connection."""
        if self._async_conn is not None:
            await self._async_conn.close()
            self._async_conn = None
        self._closed = True

    async def commit(self) -> None:
        """Commit any pending transaction."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            await self._async_conn.execute("COMMIT")

    async def rollback(self) -> None:
        """Roll back any pending transaction."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            await self._async_conn.execute("ROLLBACK")

    async def cursor(self) -> AsyncCursor:
        """Return a new AsyncCursor object."""
        if self._closed:
            raise InterfaceError("Connection is closed")
        return AsyncCursor(self)

    async def __aenter__(self) -> "AsyncConnection":
        await self.connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
