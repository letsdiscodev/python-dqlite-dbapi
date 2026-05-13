"""Pin: ``AsyncConnection.__aexit__`` clean-exit commit emits a DEBUG
breadcrumb when interrupted by ``CancelledError`` /
``KeyboardInterrupt`` / ``SystemExit``.

Symmetric to ``test_connection_exit_commit_ki_breadcrumb`` for the
sync sibling. The breadcrumb is the only forensic trail an operator
triaging a "dangling server-side transaction after cancel" symptom
has — without the log line, the partial-commit-on-cancel hazard is
undetectable post-hoc.

Pinned so a future cleanup that drops the try/except wrap (arguing
"commit() already raises, no point catching here") lights up as a
test failure.
"""

from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.exceptions import OperationalError


def _connection_with_commit(commit_side_effect: BaseException) -> AsyncConnection:
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._address = "localhost:19001"
    conn._database = "default"
    conn._timeout = 1.0
    conn._max_total_rows = None
    conn._max_continuation_frames = None
    conn._trust_server_heartbeat = False
    conn._async_conn = MagicMock()  # truthy so __aexit__ does not early-return
    conn._closed = False
    conn._connect_lock = None
    conn._op_lock = None
    conn._loop_ref = None
    conn.messages = []
    conn.commit = AsyncMock(side_effect=commit_side_effect)
    conn.rollback = AsyncMock()
    conn.close = AsyncMock()
    return conn


@pytest.mark.parametrize(
    "exc_cls",
    [asyncio.CancelledError, KeyboardInterrupt, SystemExit],
)
def test_aexit_clean_commit_cancel_logs_breadcrumb_and_re_raises(
    exc_cls: type[BaseException], caplog: pytest.LogCaptureFixture
) -> None:
    """A cancel / signal landing inside the implicit commit at
    ``__aexit__`` must produce a DEBUG breadcrumb pointing at dqlite
    AND re-raise the original exception class faithfully.
    """
    conn = _connection_with_commit(exc_cls("simulated"))

    async def run() -> None:
        with (
            caplog.at_level(logging.DEBUG, logger="dqlitedbapi.aio.connection"),
            pytest.raises(exc_cls),
        ):
            await conn.__aexit__(None, None, None)

    asyncio.run(run())

    breadcrumbs = [
        r
        for r in caplog.records
        if r.levelname == "DEBUG" and "commit interrupted by cancel/signal" in r.getMessage()
    ]
    assert breadcrumbs, (
        f"expected DEBUG breadcrumb on clean-exit commit interrupt for "
        f"{exc_cls.__name__}; got: "
        f"{[r.getMessage() for r in caplog.records if r.levelname == 'DEBUG']}"
    )
    # exc_info carries the propagating cancel/signal so the audit trail
    # shows where the abort came from.
    assert breadcrumbs[0].exc_info is not None
    assert isinstance(breadcrumbs[0].exc_info[1], exc_cls)


def test_aexit_clean_commit_normal_exception_does_not_log_breadcrumb(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative pin: an ordinary exception during commit (e.g.
    ``OperationalError``) does NOT trigger the cancel/signal breadcrumb;
    the except arm is narrowly scoped to cancel + KI + SystemExit.
    """
    conn = _connection_with_commit(OperationalError("server-side"))

    async def run() -> None:
        with (
            caplog.at_level(logging.DEBUG, logger="dqlitedbapi.aio.connection"),
            pytest.raises(OperationalError),
        ):
            await conn.__aexit__(None, None, None)

    asyncio.run(run())

    breadcrumbs = [
        r
        for r in caplog.records
        if r.levelname == "DEBUG" and "commit interrupted by cancel/signal" in r.getMessage()
    ]
    assert not breadcrumbs, (
        f"non-cancel/signal exception must not trigger the breadcrumb; "
        f"got: {[r.getMessage() for r in breadcrumbs]}"
    )
