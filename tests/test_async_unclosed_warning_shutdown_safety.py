"""Pin: ``_async_unclosed_warning`` survives ``Py_FinalizeEx`` phase-3
module-globals-set-to-None teardown without raising into ``weakref.finalize``.
The fix captures each module global as a kwarg-default at definition time
(mirroring ``_cleanup_loop_thread``), plus a defensive early-bail on None."""

from __future__ import annotations

import warnings as _stdlib_warnings

import pytest

from dqliteclient import get_current_pid
from dqlitedbapi.aio import connection as _aio_conn_mod

_CREATOR_PID_SNAPSHOT = get_current_pid()


def _invoke_finalizer(
    *,
    closed: bool = False,
    connected: bool = True,
    **kwargs: object,
) -> None:
    """Exercise the finalize body; ``kwargs`` overrides capture params (None sim)."""
    _aio_conn_mod._async_unclosed_warning(
        closed_flag=[closed],
        connected_flag=[connected],
        address="127.0.0.1:9001",
        creator_pid=_CREATOR_PID_SNAPSHOT,
        **kwargs,
    )


class _RaisingSentinel:
    """Pretends to be a stdlib callable but raises if called."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __call__(self, *args: object, **kwargs: object) -> object:
        raise AssertionError(
            f"sentinel for {self.name} was called — body re-read module "
            "global instead of using its captured kwarg-default"
        )

    def __getattr__(self, item: str) -> object:
        raise AssertionError(
            f"sentinel for {self.name} had attribute {item!r} accessed — "
            "body re-read module global instead of using its captured "
            "kwarg-default"
        )


def test_body_survives_get_current_pid_set_to_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``get_current_pid`` is re-read at call time (for the fork-pid mock shim);
    the broad ``except`` around it absorbs the phase-3 ``None`` shape."""
    monkeypatch.setattr(_aio_conn_mod, "get_current_pid", None)
    with _stdlib_warnings.catch_warnings():
        _stdlib_warnings.simplefilter("error", ResourceWarning)
        _invoke_finalizer()


def test_body_survives_get_current_pid_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The broad ``except`` around ``get_current_pid()`` catches any class."""

    def _raising() -> int:
        raise RuntimeError("phase-3 teardown sentinel")

    monkeypatch.setattr(_aio_conn_mod, "get_current_pid", _raising)
    with _stdlib_warnings.catch_warnings():
        _stdlib_warnings.simplefilter("error", ResourceWarning)
        _invoke_finalizer()


def test_body_uses_captured_warnings_default_not_module_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_aio_conn_mod, "warnings", _RaisingSentinel("warnings"))
    with pytest.warns(ResourceWarning, match="AsyncConnection"):
        _invoke_finalizer()


def test_body_uses_captured_contextlib_default_not_module_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_aio_conn_mod, "contextlib", _RaisingSentinel("contextlib"))
    with pytest.warns(ResourceWarning, match="AsyncConnection"):
        _invoke_finalizer()


def test_body_uses_captured_sanitize_for_log_default_not_module_global(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_aio_conn_mod, "sanitize_for_log", _RaisingSentinel("sanitize_for_log"))
    with pytest.warns(ResourceWarning, match="AsyncConnection"):
        _invoke_finalizer()


def test_bail_silently_when_warnings_kwarg_is_none() -> None:
    with _stdlib_warnings.catch_warnings():
        _stdlib_warnings.simplefilter("error", ResourceWarning)
        _invoke_finalizer(_warnings=None)


def test_bail_silently_when_contextlib_kwarg_is_none() -> None:
    with _stdlib_warnings.catch_warnings():
        _stdlib_warnings.simplefilter("error", ResourceWarning)
        _invoke_finalizer(_contextlib=None)


def test_bail_silently_when_sanitize_for_log_kwarg_is_none() -> None:
    with _stdlib_warnings.catch_warnings():
        _stdlib_warnings.simplefilter("error", ResourceWarning)
        _invoke_finalizer(_sanitize_for_log=None)


def test_emit_when_globals_intact() -> None:
    with pytest.warns(ResourceWarning, match="AsyncConnection"):
        _invoke_finalizer(closed=False, connected=True)


def test_no_emit_when_already_closed() -> None:
    with _stdlib_warnings.catch_warnings():
        _stdlib_warnings.simplefilter("error", ResourceWarning)
        _invoke_finalizer(closed=True, connected=True)


def test_no_emit_when_never_connected() -> None:
    with _stdlib_warnings.catch_warnings():
        _stdlib_warnings.simplefilter("error", ResourceWarning)
        _invoke_finalizer(closed=False, connected=False)
