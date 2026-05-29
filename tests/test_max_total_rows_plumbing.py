"""A custom max_total_rows on the dbapi Connection propagates down to the protocol."""

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


class TestMaxTotalRowsPropagation:
    def test_default_on_connection(self) -> None:
        conn = Connection("localhost:19001", timeout=2.0)
        try:
            assert conn._max_total_rows == 10_000_000
        finally:
            conn.close()

    def test_custom_cap_on_connection(self) -> None:
        conn = Connection("localhost:19001", timeout=2.0, max_total_rows=42)
        try:
            assert conn._max_total_rows == 42
        finally:
            conn.close()

    def test_none_disables_cap(self) -> None:
        conn = Connection("localhost:19001", timeout=2.0, max_total_rows=None)
        try:
            assert conn._max_total_rows is None
        finally:
            conn.close()

    def test_async_connection_default(self) -> None:
        conn = AsyncConnection("localhost:19001", timeout=2.0)
        assert conn._max_total_rows == 10_000_000

    def test_async_connection_custom(self) -> None:
        conn = AsyncConnection("localhost:19001", timeout=2.0, max_total_rows=7)
        assert conn._max_total_rows == 7

    def test_propagates_to_underlying_dqlite_connection(self) -> None:
        """The inner DqliteConnection is constructed with the dbapi-level cap."""
        from unittest.mock import AsyncMock, patch

        with (
            patch(
                "dqlitedbapi.connection._resolve_leader",
                new=AsyncMock(
                    side_effect=lambda address, *, timeout, **_kw: address,
                ),
            ),
            patch("dqlitedbapi.connection.DqliteConnection") as MockConn,
        ):
            instance = AsyncMock()
            instance.connect = AsyncMock()
            MockConn.return_value = instance
            conn = Connection("localhost:19001", timeout=2.0, max_total_rows=123)
            try:

                async def warm_up() -> None:
                    await conn._get_async_connection()

                conn._run_sync(warm_up())
                _args, kwargs = MockConn.call_args
                assert kwargs["max_total_rows"] == 123
            finally:
                conn.close()
