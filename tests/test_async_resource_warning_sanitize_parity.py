"""Pin: ``AsyncConnection``'s GC-time ``ResourceWarning`` runs ``address``
through ``sanitize_for_log`` before interpolating (defence-in-depth parity
with sync ``_cleanup_loop_thread``). repr already mitigates CWE-117; the
sanitize strips control codepoints to ``?`` before repr would escape them."""

from __future__ import annotations

import os
import warnings

from dqlitedbapi.aio.connection import _async_unclosed_warning


def test_warning_routes_address_through_sanitize_for_log() -> None:
    """A control char in ``address`` reaches the warning text via
    ``sanitize_for_log`` as ``?`` rather than repr's ``\\xNN`` escape."""
    closed_flag = [False]
    connected_flag = [True]
    # \x01 (SOH) is a control codepoint that sanitize_for_log strips to ?.
    malformed_address = "host:9001\x01TAG"

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, malformed_address, os.getpid())

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert len(rw) == 1
    msg = str(rw[0].message)
    assert "?TAG" in msg
    # Negative: repr's SOH escape must not appear (would prove sanitizer bypassed).
    assert "\\x01" not in msg


def test_warning_preserves_clean_address_unchanged() -> None:
    """A clean ``address`` reaches the warning text unchanged (sanitise no-op)."""
    closed_flag = [False]
    connected_flag = [True]
    clean_address = "10.0.0.1:9001"

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, clean_address, os.getpid())

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert len(rw) == 1
    msg = str(rw[0].message)
    assert "10.0.0.1:9001" in msg
