"""Pin: ``AsyncConnection``'s GC-time ``ResourceWarning`` runs the
caller-supplied ``address`` through ``sanitize_for_log`` before
interpolating into the warning text, matching the sync sibling's
``_cleanup_loop_thread`` "defence-in-depth" posture.

The sync ``Connection._cleanup_loop_thread`` (connection.py:1140-
1157) sanitises via ``_sanitize_for_log(str(address))`` before
``!r``; the async sibling ``_async_unclosed_warning`` used to
interpolate the raw ``address`` directly. Python's ``repr()`` on
``str`` already escapes LF / CR / U+2028 / NUL / ESC into ``\\xNN``
sequences so the observable output is already log-injection-safe
(CWE-117 mitigated by repr). The sanitize call adds an extra
defence-in-depth layer (strips control codepoints to ``?`` before
repr would escape them), which is the package's documented posture.

The fix is the narrow parity-restoration; no behaviour change on
clean addresses, only a cosmetic difference for hostile / malformed
addresses (which are theoretically unreachable through normal
construction but documented as belt-and-suspenders).
"""

from __future__ import annotations

import os
import warnings

from dqlitedbapi.aio.connection import _async_unclosed_warning


def test_warning_routes_address_through_sanitize_for_log() -> None:
    """An ``address`` containing control characters reaches the
    warning text via ``sanitize_for_log`` (matching the sync sibling
    at connection.py:1140-1157). ``sanitize_for_log`` replaces
    control codepoints with literal ``?`` BEFORE repr would escape
    them as ``\\xNN`` — the observable difference is that the
    sanitized text contains ``?`` where the raw text would have the
    repr escape sequence.

    Python's ``repr()`` on ``str`` already escapes control chars
    into safe ``\\xNN`` / ``\\n`` / ``\\r`` sequences, so log
    injection (CWE-117) is already mitigated by repr alone. The
    sanitize call is the package's documented "defence-in-depth"
    layer; this test pins that the layer is present on the async
    surface (parity with the sync sibling)."""
    closed_flag = [False]
    connected_flag = [True]
    # Use a control character that ``sanitize_for_log`` strips to
    # ``?``. The literal ``\x01`` is a SOH control codepoint.
    malformed_address = "host:9001\x01TAG"

    with warnings.catch_warnings(record=True) as captured:
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, malformed_address, os.getpid())

    rw = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert len(rw) == 1
    msg = str(rw[0].message)
    # Without the sanitizer, the message would carry the repr escape
    # ``\x01TAG`` (5 chars after the colon segment); with the
    # sanitizer it carries ``?TAG``. Pin the post-fix shape.
    assert "?TAG" in msg
    # Negative: the repr escape MUST NOT appear (would prove the
    # sanitizer was bypassed). Look for the substring that repr
    # would emit for SOH: ``\x01``.
    assert "\\x01" not in msg


def test_warning_preserves_clean_address_unchanged() -> None:
    """Regression: a clean ``address`` reaches the warning text
    unchanged (modulo the standard repr escape). The sanitise call
    is a no-op for control-free strings."""
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
