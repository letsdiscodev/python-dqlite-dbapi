"""PEP 249 Connection implementation for dqlite."""

import asyncio
import threading
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
        self._thread: threading.Thread | None = None

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Ensure a dedicated event loop is running in a background thread.

        This allows sync methods to work even when called from within
        an already-running async context (e.g. uvicorn).
        """
        if self._loop is None or self._loop.is_closed():
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
            self._thread.start()
        return self._loop

    def _run_sync(self, coro: Any) -> Any:
        """Run an async coroutine from sync code.

        Submits the coroutine to the dedicated background event loop
        and blocks until the result is available.
        """
        loop = self._ensure_loop()
        future = asyncio.run_coroutine_threadsafe(coro, loop)
        return future.result()

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
            self._run_sync(self._async_conn.close())
            self._async_conn = None
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._thread is not None:
                self._thread.join(timeout=5)
            self._loop.close()
            self._loop = None
            self._thread = None
        self._closed = True

    def commit(self) -> None:
        """Commit any pending transaction."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            self._run_sync(self._commit_async())

    async def _commit_async(self) -> None:
        """Async implementation of commit."""
        conn = await self._get_async_connection()
        await conn.execute("COMMIT")

    def rollback(self) -> None:
        """Roll back any pending transaction."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            self._run_sync(self._rollback_async())

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
