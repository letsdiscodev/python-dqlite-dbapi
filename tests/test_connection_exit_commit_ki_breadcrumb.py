"""Pin: sync ``Connection.__exit__`` clean-exit commit emits a DEBUG
breadcrumb when interrupted by ``KeyboardInterrupt`` / ``SystemExit``.

The async sibling ``AsyncConnection.__aexit__`` at
``aio/connection.py:1586-1604`` already wraps ``await self.commit()``
in ``try/except (CancelledError, KeyboardInterrupt, SystemExit)`` and
emits a DEBUG breadcrumb before re-raising — to make the partial-
commit-on-signal hazard observable in operator logs. The sync
sibling's rollback arm at ``connection.py:2235-2250`` ALREADY has the
KI/SystemExit branch; only the clean-exit commit path was the outlier.

Pin: clean-exit commit interrupted by KI / SystemExit logs a DEBUG
breadcrumb and re-raises.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


@pytest.mark.parametrize("signal_cls", [KeyboardInterrupt, SystemExit])
def test_clean_exit_commit_signal_interrupt_logs_breadcrumb_and_re_raises(
    signal_cls: type[BaseException], caplog: pytest.LogCaptureFixture
) -> None:
    """A signal landing inside the implicit commit at ``__exit__``
    must produce a DEBUG breadcrumb pointing at dqlite — NOT silently
    propagate without a trace.
    """
    conn = Connection.__new__(Connection)
    conn._address = "127.0.0.1:9999"
    conn._async_conn = MagicMock()  # not None — so __exit__ does not early-return
    conn.commit = MagicMock(side_effect=signal_cls("simulated"))

    with (
        caplog.at_level(logging.DEBUG, logger="dqlitedbapi.connection"),
        pytest.raises(signal_cls),
    ):
        conn.__exit__(None, None, None)

    breadcrumbs = [
        r
        for r in caplog.records
        if r.levelname == "DEBUG"
        and "clean-exit commit" in r.message
        and "interrupted by signal" in r.message
    ]
    assert breadcrumbs, (
        f"expected DEBUG breadcrumb on clean-exit commit interrupt; "
        f"got: {[r.message for r in caplog.records if r.levelname == 'DEBUG']}"
    )


def test_clean_exit_commit_normal_exception_does_not_log_breadcrumb(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Negative pin: a non-signal exception during commit (e.g.
    ``OperationalError``) does NOT trigger the breadcrumb (it
    propagates as-is, matching the documented "silent data loss
    is worse than a noisy failure" behavior).
    """
    from dqlitedbapi.exceptions import OperationalError

    conn = Connection.__new__(Connection)
    conn._address = "127.0.0.1:9999"
    conn._async_conn = MagicMock()
    conn.commit = MagicMock(side_effect=OperationalError("server-side"))

    with (
        caplog.at_level(logging.DEBUG, logger="dqlitedbapi.connection"),
        pytest.raises(OperationalError),
    ):
        conn.__exit__(None, None, None)

    # No clean-exit-commit breadcrumb for non-signal exceptions.
    breadcrumbs = [
        r for r in caplog.records if r.levelname == "DEBUG" and "clean-exit commit" in r.message
    ]
    assert not breadcrumbs, (
        f"non-signal exception must not trigger the signal breadcrumb; "
        f"got: {[r.message for r in breadcrumbs]}"
    )


def test_exit_no_async_conn_short_circuit_unaffected() -> None:
    """Negative pin: when ``_async_conn`` is None (never connected),
    ``__exit__`` returns without calling commit. The KI breadcrumb
    wrap must not change this short-circuit.
    """
    conn = Connection.__new__(Connection)
    conn._address = "127.0.0.1:9999"
    conn._async_conn = None
    conn.commit = MagicMock()

    conn.__exit__(None, None, None)
    conn.commit.assert_not_called()
