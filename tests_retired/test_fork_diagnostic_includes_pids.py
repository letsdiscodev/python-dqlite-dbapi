"""Pin: the fork-after-init diagnostic includes both creator and current
pid so an operator can correlate a worker-log trace to the master pid.
"""

from __future__ import annotations

import re
from unittest import mock

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


def test_sync_fork_diagnostic_includes_creator_and_current_pid() -> None:
    conn = dqlitedbapi.Connection("localhost:9001")
    fake_child_pid = conn._creator_pid + 1
    with (
        mock.patch("dqlitedbapi.connection.get_current_pid", return_value=fake_child_pid),
        pytest.raises(dqlitedbapi.InterfaceError) as exc,
    ):
        conn.cursor()
    msg = str(exc.value)
    assert "used after fork" in msg
    assert re.search(rf"pid {conn._creator_pid}\b", msg), msg
    assert re.search(rf"pid {fake_child_pid}\b", msg), msg


def test_async_fork_diagnostic_includes_creator_and_current_pid() -> None:
    conn = AsyncConnection("localhost:9001")
    fake_child_pid = conn._creator_pid + 1
    with (
        mock.patch("dqlitedbapi.aio.connection.get_current_pid", return_value=fake_child_pid),
        pytest.raises(dqlitedbapi.InterfaceError) as exc,
    ):
        conn.cursor()
    msg = str(exc.value)
    assert "used after fork" in msg
    assert re.search(rf"pid {conn._creator_pid}\b", msg), msg
    assert re.search(rf"pid {fake_child_pid}\b", msg), msg
