"""Pin: commit() rewraps a LEADER_ERROR_CODES OperationalError as AmbiguousCommitError.

The Raft entry may or may not have replicated before the flip; retrying
non-idempotent DML risks silent duplicate writes. AmbiguousCommitError inherits
from OperationalError so existing catches still fire.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import AmbiguousCommitError, OperationalError
from dqlitewire import LEADER_ERROR_CODES


@pytest.mark.parametrize("leader_code", sorted(LEADER_ERROR_CODES))
async def test_async_commit_leader_flip_rewraps_as_ambiguous_commit_error(
    leader_code: int,
) -> None:
    import asyncio
    import os
    import weakref

    aconn = AsyncConnection.__new__(AsyncConnection)
    aconn._closed = False
    aconn._timeout = 5.0
    aconn._close_timeout = 1.0
    aconn.messages = []
    aconn._transaction_owner = None
    aconn._creator_pid = os.getpid()
    aconn._loop_ref = weakref.ref(asyncio.get_running_loop())
    aconn._op_lock = asyncio.Lock()
    aconn._connect_lock = asyncio.Lock()

    inner = MagicMock()
    inner._protocol = MagicMock()
    inner.in_transaction = True

    async def _raise_leader_error(_sql: str) -> None:
        raise OperationalError(
            "ioerr / not leader",
            code=leader_code,
            raw_message="ioerr / not leader",
        )

    inner.execute = _raise_leader_error
    aconn._async_conn = inner

    with pytest.raises(AmbiguousCommitError) as ei:
        await aconn.commit()

    # Load-bearing: still catches as OperationalError.
    assert isinstance(ei.value, OperationalError)
    assert ei.value.code == leader_code
    assert isinstance(ei.value.__cause__, OperationalError)


def test_ambiguous_commit_error_is_exported_at_package_level() -> None:
    assert dqlitedbapi.AmbiguousCommitError is AmbiguousCommitError
    assert "AmbiguousCommitError" in dqlitedbapi.__all__
