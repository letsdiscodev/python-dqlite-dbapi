"""Tests for _run_sync timeout behavior."""

import asyncio

import pytest

from dqlitedbapi.connection import Connection
from dqlitedbapi.exceptions import OperationalError


class TestRunSyncTimeout:
    def test_run_sync_times_out(self) -> None:
        """_run_sync should raise OperationalError after timeout."""
        conn = Connection("localhost:9001", timeout=0.1)

        async def hang_forever() -> None:
            await asyncio.sleep(999)

        with pytest.raises(OperationalError, match="timed out"):
            conn._run_sync(hang_forever())
