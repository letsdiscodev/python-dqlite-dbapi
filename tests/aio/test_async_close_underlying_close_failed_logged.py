"""Pin: ``AsyncConnection.close()``'s bare ``except Exception`` arm at
``aio/connection.py:732-737`` swallows a non-Cancel, non-InterfaceError
exception from the *secondary* cleanup-close in the ``finally`` and
DEBUG-logs it with ``exc_info``.

The contract: when the body close at line 621 raises and the
shielded cleanup close at line 652 ALSO raises (with something
that is not CancelledError / KeyboardInterrupt / SystemExit /
InterfaceError), the body's exception is what propagates — the
secondary cleanup failure is recorded at DEBUG and otherwise
swallowed. This preserves "the primary failure is what the caller
sees" semantics, the same shape the dqliteclient sibling at
``__init__.py:175-181`` follows.

The sibling arms (CancelledError at 720-731 and InterfaceError at
668-711) are pinned by ``test_async_close_finally_propagates_cancel.py``;
this test pins the catch-all so a refactor that narrows
``Exception`` or re-orders the chain is caught.
"""

from __future__ import annotations

import asyncio
import logging
import os as _os
import weakref
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _prime_connection() -> AsyncConnection:
    """Build an AsyncConnection with the minimum scaffolding needed to
    drive ``close()`` without a real cluster — mirrors the helper in
    ``test_async_close_finally_propagates_cancel.py``.
    """
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._async_conn = None
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn._cursors = weakref.WeakSet()
    conn.messages = []
    conn._timeout = 5.0
    conn._close_timeout = 0.5
    conn._creator_pid = _os.getpid()
    conn._closed_flag = [False]
    conn._connected_flag = [True]
    return conn


async def test_close_finally_exception_arm_swallows_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Body close raises ``RuntimeError("body close failed")`` so the
    body's ``self._async_conn = None`` is skipped; the finally's
    shielded close then raises ``RuntimeError("shielded close also
    failed")``. The bare ``except Exception`` arm at lines 732-737
    swallows the secondary failure and DEBUG-logs it; the primary
    body RuntimeError keeps propagating out of close().

    Pinned behaviour:
    1. The primary (body) RuntimeError propagates to the caller.
    2. The secondary (shielded) RuntimeError is logged at DEBUG with
       ``exc_info`` pointing at it, NOT propagated.
    3. The slot reaches the unconditional ``self._async_conn = None``
       at line 738 after the except so the close is idempotent.
    """
    conn = _prime_connection()
    conn._ensure_locks()

    inner = MagicMock()
    # First inner.close() call (body, line 621) raises so the body
    # skips ``self._async_conn = None``. Second call (finally's
    # shielded close, line 652) raises a non-Cancel non-InterfaceError
    # Exception so flow falls into the bare-Exception arm.
    inner.close = AsyncMock(
        side_effect=[
            RuntimeError("body close failed"),
            RuntimeError("shielded close also failed"),
        ]
    )
    conn._async_conn = inner

    caplog.set_level(logging.DEBUG, logger="dqlitedbapi.aio.connection")

    # Primary body failure propagates; secondary cleanup failure is
    # swallowed by the bare-Exception arm.
    with pytest.raises(RuntimeError, match="body close failed"):
        await conn.close()

    # Secondary (shielded) close was attempted, secondary RuntimeError
    # was swallowed — slot is still cleared by line 738.
    assert conn._async_conn is None
    assert inner.close.call_count == 2

    debug_rec = next(
        (r for r in caplog.records if "underlying close failed" in r.getMessage()),
        None,
    )
    assert debug_rec is not None, (
        "expected a DEBUG record from the bare-Exception suppression arm "
        "at aio/connection.py:732-737"
    )
    assert debug_rec.levelno == logging.DEBUG, (
        "underlying-close failure must log at DEBUG — a refactor that "
        "bumps to WARNING/ERROR surfaces the secondary failure too loudly "
        "for SA's pool-dispose path"
    )
    assert debug_rec.exc_info is not None, (
        "DEBUG record must carry exc_info pointing at the close-time exception "
        "so operators can triage transport-tear-down failures"
    )
    assert isinstance(debug_rec.exc_info[1], RuntimeError)
    assert "shielded close also failed" in str(debug_rec.exc_info[1]), (
        "DEBUG must capture the SECONDARY (cleanup) failure, not the primary — "
        "primary is what the caller sees in the propagating exception"
    )


async def test_close_finally_exception_arm_does_not_match_cancelled() -> None:
    """Order-of-arms regression pin: CancelledError must be handled by
    the dedicated arm at lines 720-731 (re-raise + clear state),
    NOT by the bare-Exception arm. A refactor that re-orders the
    except chain would silently swallow CancelledError here and
    break TaskGroup cancellation propagation.
    """
    conn = _prime_connection()
    conn._ensure_locks()

    inner = MagicMock()
    inner.close = AsyncMock(
        side_effect=[
            RuntimeError("body close failed"),
            asyncio.CancelledError(),
        ]
    )
    conn._async_conn = inner

    with pytest.raises(asyncio.CancelledError):
        await conn.close()
