"""Async connection implementation for dqlite."""

import asyncio
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
        self._connect_lock = asyncio.Lock()
        self._op_lock = asyncio.Lock()

    async def _ensure_connection(self) -> DqliteConnection:
        """Ensure the underlying connection is established."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            return self._async_conn

        async with self._connect_lock:
            # Double-check after acquiring lock
            if self._async_conn is not None:
                return self._async_conn

            conn = DqliteConnection(
                self._address,
                database=self._database,
                timeout=self._timeout,
            )
            try:
                await conn.connect()
            except Exception as e:
                raise OperationalError(f"Failed to connect: {e}") from e

            self._async_conn = conn

        return self._async_conn

    async def connect(self) -> None:
        """Establish the connection."""
        await self._ensure_connection()

    async def close(self) -> None:
        """Close the connection."""
        if self._closed:
            return
        self._closed = True
        if self._async_conn is not None:
            await self._async_conn.close()
            self._async_conn = None

    async def commit(self) -> None:
        """Commit any pending transaction."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            async with self._op_lock:
                await self._async_conn.execute("COMMIT")

    async def rollback(self) -> None:
        """Roll back any pending transaction."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            async with self._op_lock:
                await self._async_conn.execute("ROLLBACK")

    def cursor(self) -> AsyncCursor:
        """Return a new AsyncCursor object.

        This is intentionally sync — SQLAlchemy calls cursor() from
        sync context within its greenlet-based async adapter.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")
        return AsyncCursor(self)

    async def __aenter__(self) -> "AsyncConnection":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, *args: Any) -> None:
        try:
            if exc_type is None:
                await self.commit()
            else:
                await self.rollback()
        except Exception:
            pass
        finally:
            await self.close()
