"""Sync Connection.__exit__ clean-exit commit interrupted by KeyboardInterrupt/SystemExit
logs a DEBUG breadcrumb and re-raises (the partial-commit-on-signal hazard)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from dqlitedbapi.connection import Connection


@pytest.mark.parametrize("signal_cls", [KeyboardInterrupt, SystemExit])
def test_clean_exit_commit_signal_interrupt_logs_breadcrumb_and_re_raises(
    signal_cls: type[BaseException], caplog: pytest.LogCaptureFixture
) -> None:
    conn = Connection.__new__(Connection)
    conn._closed = False
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
    """A non-signal exception during commit propagates as-is without a breadcrumb."""
    from dqlitedbapi.exceptions import OperationalError

    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._address = "127.0.0.1:9999"
    conn._async_conn = MagicMock()
    conn.commit = MagicMock(side_effect=OperationalError("server-side"))

    with (
        caplog.at_level(logging.DEBUG, logger="dqlitedbapi.connection"),
        pytest.raises(OperationalError),
    ):
        conn.__exit__(None, None, None)

    breadcrumbs = [
        r for r in caplog.records if r.levelname == "DEBUG" and "clean-exit commit" in r.message
    ]
    assert not breadcrumbs, (
        f"non-signal exception must not trigger the signal breadcrumb; "
        f"got: {[r.message for r in breadcrumbs]}"
    )


def test_exit_no_async_conn_short_circuit_unaffected() -> None:
    """When _async_conn is None, __exit__ returns without calling commit."""
    conn = Connection.__new__(Connection)
    conn._closed = False
    conn._address = "127.0.0.1:9999"
    conn._async_conn = None
    conn.commit = MagicMock()

    conn.__exit__(None, None, None)
    conn.commit.assert_not_called()
