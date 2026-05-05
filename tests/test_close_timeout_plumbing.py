"""close_timeout plumbing from Connection → DqliteConnection.

The client-layer ``DqliteConnection`` accepts a ``close_timeout``
governing the ``asyncio.wait_for`` deadline around
``protocol.wait_closed()`` during ``close()``. The dbapi layer must
forward the kwarg end-to-end so callers with non-LAN latencies (or
strict-shutdown SLAs) can tune the drain budget without reaching
into private attributes.

Validation raises ``ProgrammingError`` at the dbapi boundary — not
``ValueError`` like the client layer — so PEP 249 classification
is preserved (matches the sibling ``timeout`` validator).
"""

from unittest.mock import AsyncMock, patch

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import ProgrammingError


class TestCloseTimeoutPlumbing:
    def test_default_on_connection(self) -> None:
        conn = Connection("localhost:19001", timeout=2.0)
        try:
            assert conn._close_timeout == 0.5
        finally:
            conn.close()

    def test_custom_on_connection(self) -> None:
        conn = Connection("localhost:19001", timeout=2.0, close_timeout=3.25)
        try:
            assert conn._close_timeout == 3.25
        finally:
            conn.close()

    def test_async_connection_default(self) -> None:
        conn = AsyncConnection("localhost:19001", timeout=2.0)
        assert conn._close_timeout == 0.5

    def test_async_connection_custom(self) -> None:
        conn = AsyncConnection("localhost:19001", timeout=2.0, close_timeout=7.0)
        assert conn._close_timeout == 7.0

    @pytest.mark.parametrize("bad", [0, 0.0, -1, -0.5, float("nan"), float("inf"), float("-inf")])
    def test_invalid_sync_raises_programmingerror(self, bad: float) -> None:
        with pytest.raises(ProgrammingError, match="close_timeout"):
            Connection("localhost:19001", timeout=2.0, close_timeout=bad)

    @pytest.mark.parametrize("bad", [0, 0.0, -1, -0.5, float("nan"), float("inf"), float("-inf")])
    def test_invalid_async_raises_programmingerror(self, bad: float) -> None:
        with pytest.raises(ProgrammingError, match="close_timeout"):
            AsyncConnection("localhost:19001", timeout=2.0, close_timeout=bad)

    @pytest.mark.parametrize("bad", ["0.5", b"0.5", None, [], {}, complex(1)])
    def test_non_numeric_close_timeout_raises_programmingerror(self, bad: object) -> None:
        """PEP 249 §7: every input-validation failure must subclass Error.
        Previously the close_timeout validator called math.isfinite on
        non-numeric inputs and leaked a bare TypeError."""
        with pytest.raises(ProgrammingError):
            Connection("localhost:19001", timeout=2.0, close_timeout=bad)  # type: ignore[arg-type]
        with pytest.raises(ProgrammingError):
            AsyncConnection("localhost:19001", timeout=2.0, close_timeout=bad)  # type: ignore[arg-type]

    def test_module_connect_forwards(self) -> None:
        conn = dqlitedbapi.connect("localhost:19001", timeout=2.0, close_timeout=1.5)
        try:
            assert conn._close_timeout == 1.5
        finally:
            conn.close()

    def test_module_connect_invalid_raises(self) -> None:
        with pytest.raises(ProgrammingError, match="close_timeout"):
            dqlitedbapi.connect("localhost:19001", timeout=2.0, close_timeout=0)

    def test_aio_connect_forwards(self) -> None:
        conn = dqlitedbapi.aio.connect("localhost:19001", timeout=2.0, close_timeout=1.25)
        assert conn._close_timeout == 1.25

    def test_aio_connect_invalid_raises(self) -> None:
        with pytest.raises(ProgrammingError, match="close_timeout"):
            dqlitedbapi.aio.connect("localhost:19001", timeout=2.0, close_timeout=-1)

    def test_propagates_to_underlying_dqlite_connection(self) -> None:
        """After the first use, the inner DqliteConnection must receive
        the dbapi-level close_timeout so the actual wait_closed drain
        honours the caller's budget.
        """
        with (
            patch(
                "dqlitedbapi.connection._resolve_leader",
                new=AsyncMock(side_effect=lambda address, *, timeout, **_kw: address),
            ),
            patch("dqlitedbapi.connection.DqliteConnection") as MockConn,
        ):
            instance = AsyncMock()
            instance.connect = AsyncMock()
            MockConn.return_value = instance
            conn = Connection("localhost:19001", timeout=2.0, close_timeout=4.5)
            try:

                async def warm_up() -> None:
                    await conn._get_async_connection()

                conn._run_sync(warm_up())
                _args, kwargs = MockConn.call_args
                assert kwargs["close_timeout"] == 4.5
            finally:
                conn.close()
