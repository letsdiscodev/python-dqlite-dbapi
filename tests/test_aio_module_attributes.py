"""Tests for async module PEP 249 attributes and exports."""

import pytest

from dqlitedbapi import aio
from dqlitedbapi.exceptions import ProgrammingError


class TestAioModuleAttributes:
    def test_apilevel(self) -> None:
        assert aio.apilevel == "2.0"

    def test_threadsafety(self) -> None:
        assert aio.threadsafety == 1

    def test_paramstyle(self) -> None:
        assert aio.paramstyle == "qmark"

    def test_type_constructors_exported(self) -> None:
        """PEP 249 type constructors should be available from aio module."""
        assert callable(aio.Date)
        assert callable(aio.Time)
        assert callable(aio.Timestamp)
        assert callable(aio.DateFromTicks)
        assert callable(aio.TimeFromTicks)
        assert callable(aio.TimestampFromTicks)
        assert callable(aio.Binary)

    def test_type_objects_exported(self) -> None:
        """PEP 249 type objects should be available from aio module."""
        assert aio.STRING == "TEXT"
        assert aio.BINARY == "BLOB"
        assert aio.NUMBER == "INTEGER"
        assert aio.DATETIME == "DATE"
        assert aio.ROWID == "ROWID"


class TestAioConnectForwardsGovernors:
    def test_aio_connect_forwards_all_governors(self) -> None:
        conn = aio.connect(
            "localhost:19001",
            max_total_rows=500,
            max_continuation_frames=7,
            trust_server_heartbeat=True,
        )
        assert conn._max_total_rows == 500
        assert conn._max_continuation_frames == 7
        assert conn._trust_server_heartbeat is True

    def test_aio_connect_uses_default_governors(self) -> None:
        conn = aio.connect("localhost:19001")
        assert conn._max_total_rows == 10_000_000
        assert conn._max_continuation_frames == 100_000
        assert conn._trust_server_heartbeat is False

    def test_aio_connect_accepts_none_caps(self) -> None:
        conn = aio.connect(
            "localhost:19001",
            max_total_rows=None,
            max_continuation_frames=None,
        )
        assert conn._max_total_rows is None
        assert conn._max_continuation_frames is None


class TestAioConnectTimeoutValidation:
    @pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), float("-inf")])
    def test_connect_rejects_non_positive_or_non_finite(self, bad: float) -> None:
        with pytest.raises(ProgrammingError, match="timeout must be a positive finite number"):
            aio.connect("localhost:19001", timeout=bad)

    @pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), float("-inf")])
    def test_aconnect_rejects_non_positive_or_non_finite(self, bad: float) -> None:
        import asyncio

        async def run() -> None:
            with pytest.raises(ProgrammingError, match="timeout must be a positive finite number"):
                await aio.aconnect("localhost:19001", timeout=bad)

        asyncio.run(run())
