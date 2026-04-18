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
            timeout: Connection timeout in seconds (positive, finite)
        """
        import math

        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError(f"timeout must be a positive finite number, got {timeout}")
        self._address = address
        self._database = database
        self._timeout = timeout
        self._async_conn: DqliteConnection | None = None
        self._closed = False
        # asyncio primitives MUST be created inside the loop they will
        # run on. We instantiate lazily in _ensure_connection / the
        # op-serializing paths so constructors can safely run outside
        # a running loop (SQLAlchemy creates AsyncConnection in sync
        # glue code before any loop exists). See ISSUE-11.
        self._connect_lock: asyncio.Lock | None = None
        self._op_lock: asyncio.Lock | None = None

    def _ensure_locks(self) -> tuple[asyncio.Lock, asyncio.Lock]:
        """Lazy-create the asyncio locks on the currently-running loop."""
        if self._connect_lock is None:
            self._connect_lock = asyncio.Lock()
        if self._op_lock is None:
            self._op_lock = asyncio.Lock()
        return self._connect_lock, self._op_lock

    async def _ensure_connection(self) -> DqliteConnection:
        """Ensure the underlying connection is established."""
        if self._closed:
            raise InterfaceError("Connection is closed")

        if self._async_conn is not None:
            return self._async_conn

        connect_lock, _ = self._ensure_locks()
        async with connect_lock:
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
        """Close the connection.

        Serializes with any in-flight operation via ``_op_lock`` so we
        never tear down the underlying protocol while another task is
        mid-execute/mid-commit — that races would leave the caller
        with mysterious "connection closed" errors mid-query.
        """
        if self._closed:
            return
        # Set _closed first so any task waiting on the lock sees the
        # closed state as soon as it acquires. Then drain the current
        # in-flight op (if any) under the lock.
        self._closed = True
        if self._async_conn is None:
            return
        _, op_lock = self._ensure_locks()
        async with op_lock:
            if self._async_conn is not None:
                await self._async_conn.close()
                self._async_conn = None

    async def commit(self) -> None:
        """Commit any pending transaction.

        Silent no-op if the connection has never been used (preserves
        the existing "no spurious connect" contract) or if the server
        reports "no transaction is active" (matches stdlib sqlite3).
        """
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._async_conn is None:
            return
        import dqliteclient.exceptions as _client_exc

        _, op_lock = self._ensure_locks()
        async with op_lock:
            try:
                await self._async_conn.execute("COMMIT")
            except (OperationalError, _client_exc.OperationalError) as e:
                if "no transaction is active" not in str(e).lower():
                    raise

    async def rollback(self) -> None:
        """Roll back any pending transaction. Same no-op rules as commit."""
        if self._closed:
            raise InterfaceError("Connection is closed")
        if self._async_conn is None:
            return
        import dqliteclient.exceptions as _client_exc

        _, op_lock = self._ensure_locks()
        async with op_lock:
            try:
                await self._async_conn.execute("ROLLBACK")
            except (OperationalError, _client_exc.OperationalError) as e:
                if "no transaction is active" not in str(e).lower():
                    raise

    def cursor(self) -> AsyncCursor:
        """Return a new AsyncCursor object.

        This is intentionally sync — SQLAlchemy calls cursor() from
        sync context within its greenlet-based async adapter.
        """
        if self._closed:
            raise InterfaceError("Connection is closed")
        return AsyncCursor(self)

    def __repr__(self) -> str:
        state = "closed" if self._closed else ("connected" if self._async_conn else "unused")
        return (
            f"<AsyncConnection address={self._address!r} database={self._database!r} {state}>"
        )

    async def __aenter__(self) -> "AsyncConnection":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, *args: Any) -> None:
        if self._async_conn is None:
            await self.close()
            return
        try:
            if exc_type is None:
                await self.commit()
            else:
                try:
                    await self.rollback()
                except Exception:
                    pass  # Body's exception wins.
        finally:
            await self.close()
