"""Pin: fork-after-init diagnostic includes both the creator pid
and the current observed pid so an operator chasing a stack trace
in worker logs can correlate the failure to the master / forkserver
pid.

Symmetric with the cross-thread sibling diagnostic which already
includes both ids. Without the pids, the diagnostic was a constant
string with no operator-correlation surface.
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
    # The cursor() entry uses the "AsyncConnection used after fork" lead-in.
    assert "used after fork" in msg
    assert re.search(rf"pid {conn._creator_pid}\b", msg), msg
    assert re.search(rf"pid {fake_child_pid}\b", msg), msg
