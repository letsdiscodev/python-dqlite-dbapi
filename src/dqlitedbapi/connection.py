"""PEP 249 Connection implementation for dqlite."""

import asyncio
from typing import Any

from dqliteclient import DqliteConnection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError, OperationalError


class Connection:
    """PEP 249 compliant database connection."""

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
        self._loop: asyncio.AbstractEventLoop | None = None

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running loop, create a new one
                self._loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self._loop)
        return self._loop

    async def _get_async_connection(self) -> DqliteConnection:
        """Get or create the underlying async connection."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is None:
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

        return self._async_conn

    def close(self) -> None:
        """Close the connection."""
        if self._async_conn is not None:
            loop = self._get_loop()
            loop.run_until_complete(self._async_conn.close())
            self._async_conn = None
        self._closed = True

    def commit(self) -> None:
        """Commit any pending transaction."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            loop = self._get_loop()
            loop.run_until_complete(self._commit_async())

    async def _commit_async(self) -> None:
        """Async implementation of commit."""
        conn = await self._get_async_connection()
        await conn.execute("COMMIT")

    def rollback(self) -> None:
        """Roll back any pending transaction."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            loop = self._get_loop()
            loop.run_until_complete(self._rollback_async())

    async def _rollback_async(self) -> None:
        """Async implementation of rollback."""
        conn = await self._get_async_connection()
        await conn.execute("ROLLBACK")

    def cursor(self) -> Cursor:
        """Return a new Cursor object."""
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self)

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
