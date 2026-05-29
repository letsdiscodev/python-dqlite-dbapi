"""Pin: ``_async_unclosed_warning`` finalizer is fork-safe — it skips
the ResourceWarning when run in a forked child (the captured flags are
a parent snapshot), short-circuiting on ``get_current_pid() != creator_pid``."""

from __future__ import annotations

import os
import warnings
from unittest.mock import patch

import pytest

from dqlitedbapi.aio.connection import _async_unclosed_warning


def test_async_finalizer_short_circuits_on_pid_mismatch_no_warning() -> None:
    """Simulated forked child: no ResourceWarning even though the flags
    (a parent snapshot) would otherwise trigger one."""
    closed_flag = [False]
    connected_flag = [True]
    parent_pid = os.getpid()
    child_pid = parent_pid + 1

    with (
        patch("dqlitedbapi.aio.connection.get_current_pid", return_value=child_pid),
        warnings.catch_warnings(record=True) as captured,
    ):
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "host:9001", parent_pid)

    leak_warnings = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert not leak_warnings, (
        f"forked-child async finalizer must not emit ResourceWarning; got "
        f"{[str(w.message) for w in leak_warnings]}"
    )


def test_async_finalizer_emits_warning_when_pid_matches() -> None:
    """Negative pin: same process + leak flags -> the warning still fires."""
    closed_flag = [False]
    connected_flag = [True]
    same_pid = os.getpid()

    with (
        patch("dqlitedbapi.aio.connection.get_current_pid", return_value=same_pid),
        warnings.catch_warnings(record=True) as captured,
    ):
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "host:9001", same_pid)

    leak_warnings = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert len(leak_warnings) == 1, (
        f"in-process finalizer must emit one ResourceWarning when "
        f"closed_flag=False and connected_flag=True; got "
        f"{[str(w.message) for w in leak_warnings]}"
    )


def test_async_finalizer_skips_when_closed_flag_set() -> None:
    """Pre-existing gate preserved: explicit close (closed_flag=True) skips."""
    closed_flag = [True]
    connected_flag = [True]
    same_pid = os.getpid()

    with (
        patch("dqlitedbapi.aio.connection.get_current_pid", return_value=same_pid),
        warnings.catch_warnings(record=True) as captured,
    ):
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "host:9001", same_pid)

    leak_warnings = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert not leak_warnings


def test_async_finalizer_skips_when_never_connected() -> None:
    """Pre-existing gate preserved: never-connected skips (avoids
    false-positive for ``AsyncConnection(...); del conn`` flows)."""
    closed_flag = [False]
    connected_flag = [False]
    same_pid = os.getpid()

    with (
        patch("dqlitedbapi.aio.connection.get_current_pid", return_value=same_pid),
        warnings.catch_warnings(record=True) as captured,
    ):
        warnings.simplefilter("always")
        _async_unclosed_warning(closed_flag, connected_flag, "host:9001", same_pid)

    leak_warnings = [w for w in captured if issubclass(w.category, ResourceWarning)]
    assert not leak_warnings


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires os.fork")
def test_async_finalizer_in_forked_child_does_not_emit_warning() -> None:
    """End-to-end: fork, GC the inherited AsyncConnection in the child,
    verify no false-positive ResourceWarning on its stderr."""
    import contextlib as _contextlib
    import gc
    import sys

    from dqlitedbapi.aio.connection import AsyncConnection

    conn = AsyncConnection("127.0.0.1:9999")
    # Flip connected_flag manually instead of running real
    # _ensure_connection (which would need a live cluster).
    conn._connected_flag[0] = True
    assert conn._finalizer is not None

    pipe_r, pipe_w = os.pipe()
    err_r, err_w = os.pipe()
    pid = os.fork()
    if pid == 0:
        # Child: redirect stderr to the pipe to capture finalizer warnings.
        os.close(pipe_r)
        os.close(err_r)
        os.dup2(err_w, sys.stderr.fileno())
        os.close(err_w)
        with _contextlib.suppress(BaseException):
            del conn
            gc.collect()
        os.write(pipe_w, b"DONE")
        os.close(pipe_w)
        os._exit(0)

    os.close(pipe_w)
    os.close(err_w)
    while os.read(pipe_r, 4096):
        pass
    os.close(pipe_r)
    child_stderr = b""
    while True:
        chunk = os.read(err_r, 4096)
        if not chunk:
            break
        child_stderr += chunk
    os.close(err_r)
    os.waitpid(pid, 0)
    decoded = child_stderr.decode(errors="replace")
    assert "was garbage-collected" not in decoded, (
        f"forked-child async finalizer emitted false-positive ResourceWarning: {decoded!r}"
    )
    del conn
