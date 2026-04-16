"""PEP 249 Connection implementation for dqlite."""

import asyncio
import contextlib
import threading
from typing import Any

from dqliteclient import DqliteConnection
from dqlitedbapi.cursor import Cursor
from dqlitedbapi.exceptions import InterfaceError, OperationalError, ProgrammingError


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
        self._loop_lock = threading.Lock()
        self._op_lock = threading.Lock()
        self._connect_lock: asyncio.Lock | None = None
        self._creator_thread = threading.get_ident()

    def _check_thread(self) -> None:
        """Raise ProgrammingError if called from a different thread than the creator."""
        current = threading.get_ident()
        if current != self._creator_thread:
            raise ProgrammingError(
                f"Connection objects created in a thread can only be used in that "
                f"same thread. The object was created in thread id "
                f"{self._creator_thread} and this is thread id {current}."
            )

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Ensure a dedicated event loop is running in a background thread.

        This allows sync methods to work even when called from within
        an already-running async context (e.g. uvicorn).
        """
        if self._loop is not None and not self._loop.is_closed():
            return self._loop
        with self._loop_lock:
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
                self._thread.start()
        return self._loop

    def _run_sync(self, coro: Any) -> Any:
        """Run an async coroutine from sync code.

        Submits the coroutine to the dedicated background event loop
        and blocks until the result is available. The operation lock
        ensures only one operation runs at a time, preventing wire
        protocol corruption from concurrent access.
        """
        with self._op_lock:
            loop = self._ensure_loop()
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            try:
                # Future.result() provides a happens-before memory barrier,
                # ensuring all writes by the event loop thread are visible here.
                return future.result(timeout=self._timeout)
            except TimeoutError as e:
                future.cancel()
                raise OperationalError(f"Operation timed out after {self._timeout} seconds") from e

    async def _get_async_connection(self) -> DqliteConnection:
        """Get or create the underlying async connection."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            return self._async_conn

        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()

        async with self._connect_lock:
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

    def close(self) -> None:
        """Close the connection."""
        self._check_thread()
        if self._closed:
            return
        self._closed = True
        try:
            if self._loop is not None and not self._loop.is_closed():
                with contextlib.suppress(Exception):
                    self._run_sync(self._close_async())
        finally:
            with self._loop_lock:
                if self._loop is not None and not self._loop.is_closed():
                    self._loop.call_soon_threadsafe(self._loop.stop)
                    if self._thread is not None:
                        self._thread.join(timeout=5)
                    self._loop.close()
                    self._loop = None
                    self._thread = None

    async def _close_async(self) -> None:
        """Async implementation of close -- runs on event loop thread."""
        if self._async_conn is not None:
            try:
                await self._async_conn.close()
            finally:
                self._async_conn = None

    def commit(self) -> None:
        """Commit any pending transaction."""
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        self._run_sync(self._commit_async())

    async def _commit_async(self) -> None:
        """Async implementation of commit."""
        if self._async_conn is not None:
            await self._async_conn.execute("COMMIT")

    def rollback(self) -> None:
        """Roll back any pending transaction."""
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        self._run_sync(self._rollback_async())

    async def _rollback_async(self) -> None:
        """Async implementation of rollback."""
        if self._async_conn is not None:
            await self._async_conn.execute("ROLLBACK")

    def cursor(self) -> Cursor:
        """Return a new Cursor object."""
        self._check_thread()
        if self._closed:
            raise InterfaceError("Connection is closed")
        return Cursor(self)

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type: type[BaseException] | None, *args: Any) -> None:
        try:
            if exc_type is None:
                self.commit()
            else:
                self.rollback()
        except Exception:
            pass
        finally:
            self.close()
