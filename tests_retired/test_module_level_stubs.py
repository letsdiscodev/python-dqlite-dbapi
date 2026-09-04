"""Module-level stdlib-parity stubs raise ``NotSupportedError`` rather than
escaping ``AttributeError`` outside the ``Error`` hierarchy."""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import NotSupportedError


def test_register_adapter_is_callable_stdlib_parity() -> None:
    """``register_adapter`` is a real hook; bad shape raises ``ProgrammingError``
    (a PEP 249 ``Error``), not bare ``TypeError``."""

    class _RegAdapterSentinel:  # test-only type, avoids polluting the shared _ADAPTERS dict
        pass

    from dqlitedbapi.exceptions import Error, ProgrammingError
    from dqlitedbapi.types import _ADAPTERS

    dqlitedbapi.register_adapter(_RegAdapterSentinel, str)
    try:
        assert _RegAdapterSentinel in _ADAPTERS
        with pytest.raises(ProgrammingError, match="callable") as ei:
            dqlitedbapi.register_adapter(_RegAdapterSentinel, "not callable")  # type: ignore[arg-type]
        assert isinstance(ei.value, Error)
        with pytest.raises(ProgrammingError, match="class") as ei:
            dqlitedbapi.register_adapter("not a type", str)  # type: ignore[arg-type]
        assert isinstance(ei.value, Error)
    finally:
        _ADAPTERS.pop(_RegAdapterSentinel, None)


def test_register_converter_raises_not_supported() -> None:
    with pytest.raises(NotSupportedError, match="register_converter"):
        dqlitedbapi.register_converter("decimal", lambda b: b)


def test_complete_statement_raises_not_supported() -> None:
    with pytest.raises(NotSupportedError, match="complete_statement"):
        dqlitedbapi.complete_statement("SELECT 1;")


def test_enable_callback_tracebacks_raises_not_supported() -> None:
    with pytest.raises(NotSupportedError, match="enable_callback_tracebacks"):
        dqlitedbapi.enable_callback_tracebacks(True)


@pytest.mark.parametrize(
    "kwarg",
    [
        "detect_types",
        "factory",
        "cached_statements",
        "uri",
    ],
)
def test_connect_rejects_stdlib_sqlite3_kwargs(kwarg: str) -> None:
    """Unhonoured stdlib ``sqlite3.connect`` kwargs raise ``NotSupportedError``,
    not bare ``TypeError``. (``check_same_thread``/``isolation_level``/``autocommit``
    are handled separately.)"""
    with pytest.raises(NotSupportedError, match="stdlib sqlite3 kwargs"):
        dqlitedbapi.connect("127.0.0.1:9999", **{kwarg: 0})  # type: ignore[arg-type]


def test_connect_accepts_check_same_thread_false() -> None:
    """``check_same_thread=False`` is accepted and stored on the Connection."""
    conn = dqlitedbapi.connect("127.0.0.1:9999", check_same_thread=False)
    assert conn._check_same_thread is False


def test_connect_accepts_check_same_thread_true() -> None:
    """Explicit ``True`` works the same as the default."""
    conn = dqlitedbapi.connect("127.0.0.1:9999", check_same_thread=True)
    assert conn._check_same_thread is True


def test_module_exports_register_adapter_in_all() -> None:
    """The four module-level stubs appear in ``__all__`` and on the module."""
    for name in (
        "register_adapter",
        "register_converter",
        "complete_statement",
        "enable_callback_tracebacks",
    ):
        assert name in dqlitedbapi.__all__, f"{name} missing from __all__"
        assert hasattr(dqlitedbapi, name), f"{name} not exposed on module"
