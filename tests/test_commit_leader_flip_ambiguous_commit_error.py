"""Pin: ``Connection.commit()`` / ``AsyncConnection.commit()``
rewrap a LEADER_ERROR_CODES OperationalError as
``AmbiguousCommitError`` so retry middleware can branch on the
in-doubt commit shape.

The Raft log entry may or may not have been replicated to the new
leader's quorum before the flip; retrying non-idempotent DML risks
silent duplicate writes. ``AmbiguousCommitError`` inherits from
``OperationalError`` so existing ``except OperationalError`` arms
still catch it; cross-driver retry code can branch on
``isinstance(exc, AmbiguousCommitError)``.
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
    """A LEADER_ERROR_CODES OperationalError raised by the inner
    client during COMMIT must be rewrapped as
    AmbiguousCommitError. Existing OperationalError catches still
    fire (inheritance); new callers can isinstance-check the
    in-doubt shape."""
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
    # Code preserved.
    assert ei.value.code == leader_code
    # Original chained via __cause__.
    assert isinstance(ei.value.__cause__, OperationalError)


def test_ambiguous_commit_error_is_exported_at_package_level() -> None:
    """The AmbiguousCommitError class must be accessible from the
    top-level ``dqlitedbapi`` namespace so cross-driver retry code
    can ``isinstance(exc, dqlitedbapi.AmbiguousCommitError)``
    without reaching into private modules."""
    assert dqlitedbapi.AmbiguousCommitError is AmbiguousCommitError
    # And it's in __all__.
    assert "AmbiguousCommitError" in dqlitedbapi.__all__
