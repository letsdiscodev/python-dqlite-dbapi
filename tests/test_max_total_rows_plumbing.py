"""max_total_rows plumbing from Connection → DqliteConnection → Protocol.

max_total_rows is wired through every layer. This test verifies that
a custom cap set on the DBAPI Connection actually propagates down to
the protocol, so users can't silently end up with the default.
"""

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
        """After the first use, the inner DqliteConnection should see the
        same cap as the dbapi-level Connection."""
        from unittest.mock import AsyncMock, patch

        with (
            patch(
                "dqlitedbapi.connection._resolve_leader",
                new=AsyncMock(side_effect=lambda address, *, timeout: address),
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
                # DqliteConnection was constructed with max_total_rows=123
                _args, kwargs = MockConn.call_args
                assert kwargs["max_total_rows"] == 123
            finally:
                conn.close()
