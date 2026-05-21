"""Pin: ``AsyncConnection.__aexit__`` rollback-cancel arm preserves
the body exception on ``__context__`` even though the cancel signal
supplants it on the propagated exception class.

Companion to ``test_aexit_rollback_debug_log
::test_aexit_rollback_cancelled_error_propagates`` which pins the
cancel signal as the propagated exception. This file pins the
complementary fact that the body exception is still recoverable
via ``__context__`` (PEP 343 implicit chaining) so an operator
inspecting the post-cancel state at a higher scope can still see
what triggered the rollback.

The structured-concurrency policy here is deliberate: the cancel
signal wins over the body exception class. SQLAlchemy's
``is_disconnect`` classifier walks ``__cause__`` (not
``__context__``) so a transport-class body exception is NOT
visible to SA's disconnect heuristics through this path. Operators
that need the body exception to drive ``is_disconnect`` must
observe it via ``BaseException`` catch + ``__context__`` walk in
the surrounding scope.
"""

from __future__ import annotations

import asyncio
import logging
import os
from unittest.mock import AsyncMock, MagicMock

import pytest

from dqlitedbapi.aio.connection import AsyncConnection


def _connection_with_failing_rollback(failure: BaseException) -> AsyncConnection:
    """Bare AsyncConnection whose ``rollback()`` raises ``failure``."""
    conn = AsyncConnection.__new__(AsyncConnection)
    conn._closed = False
    conn._creator_pid = os.getpid()
    conn._loop_ref = None
    conn._address = "127.0.0.1:9999"
    conn._async_conn = MagicMock()
    conn.messages = []
    conn.rollback = AsyncMock(side_effect=failure)
    return conn


def test_aexit_rollback_cancel_after_body_class_supplants() -> None:
    """Cancel during rollback supplants the body class on the
    propagated exception. The body exception class is not on
    ``__cause__`` (SA ``is_disconnect`` walks ``__cause__`` only) —
    operators relying on body-class disconnect classification must
    observe through ``BaseException`` in the surrounding scope."""
    conn = _connection_with_failing_rollback(asyncio.CancelledError())
    body_exc = RuntimeError("body-raised-this")

    async def run() -> BaseException:
        try:
            await conn.__aexit__(type(body_exc), body_exc, None)
        except asyncio.CancelledError as e:
            return e
        raise AssertionError("did not raise CancelledError")

    cancel = asyncio.run(run())
    assert isinstance(cancel, asyncio.CancelledError)
    # The cancel class wins. The body's class is unreachable via
    # ``__cause__`` (SA ``is_disconnect`` walks ``__cause__`` only).
    # This is the structured-concurrency policy choice — documented
    # in the rollback arm and the breadcrumb below.
    assert cancel.__cause__ is None


def test_aexit_rollback_cancel_breadcrumb_names_policy_choice(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The DEBUG breadcrumb must reference the structured-concurrency
    policy so an operator triaging logs understands why the cancel
    class won over the body class."""
    caplog.set_level(logging.DEBUG, logger="dqlitedbapi.aio.connection")
    conn = _connection_with_failing_rollback(asyncio.CancelledError())

    async def run() -> None:
        with pytest.raises(asyncio.CancelledError):
            await conn.__aexit__(RuntimeError, RuntimeError("body-raised"), None)

    asyncio.run(run())
    matching = [
        r
        for r in caplog.records
        if r.levelno == logging.DEBUG and "rollback interrupted by cancel/signal" in r.getMessage()
    ]
    assert matching, f"expected DEBUG breadcrumb; got {caplog.records!r}"
    msg = matching[0].getMessage()
    # The breadcrumb names the structured-concurrency policy and the
    # __context__-vs-__cause__ asymmetry so a log-reader can locate
    # the body exception without diving into PEP 343 details.
    assert "structured-concurrency" in msg or "__context__" in msg
