"""Pin: module-level stdlib-sqlite3-parity stubs raise
``NotSupportedError`` rather than escaping ``AttributeError``.

The four module-level helpers (``register_adapter``,
``register_converter``, ``complete_statement``,
``enable_callback_tracebacks``) plus the ``connect()``
``**unknown_kwargs`` rejection arm were added alongside the
sibling per-class stub family but had no direct unit pins.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import NotSupportedError


def test_register_adapter_raises_not_supported() -> None:
    with pytest.raises(NotSupportedError, match="register_adapter"):
        dqlitedbapi.register_adapter(int, str)


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
        "isolation_level",
        "check_same_thread",
        "factory",
        "cached_statements",
        "uri",
        "autocommit",
    ],
)
def test_connect_rejects_stdlib_sqlite3_kwargs(kwarg: str) -> None:
    """``connect()``'s ``**unknown_kwargs`` rejection arm: stdlib
    ``sqlite3.connect`` kwargs that this driver cannot honour
    must raise ``NotSupportedError`` (in the dbapi.Error
    hierarchy) rather than bare ``TypeError`` (escapes the
    hierarchy)."""
    with pytest.raises(NotSupportedError, match="stdlib sqlite3 kwargs"):
        dqlitedbapi.connect("127.0.0.1:9999", **{kwarg: 0})  # type: ignore[arg-type]


def test_module_exports_register_adapter_in_all() -> None:
    """The four module-level stubs must appear in ``__all__`` so
    ``hasattr(dqlitedbapi, "register_adapter") is True`` (parity
    with the per-class stubs)."""
    for name in (
        "register_adapter",
        "register_converter",
        "complete_statement",
        "enable_callback_tracebacks",
    ):
        assert name in dqlitedbapi.__all__, f"{name} missing from __all__"
        assert hasattr(dqlitedbapi, name), f"{name} not exposed on module"
