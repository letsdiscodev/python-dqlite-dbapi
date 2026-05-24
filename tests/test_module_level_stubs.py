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


def test_register_adapter_is_callable_stdlib_parity() -> None:
    """``register_adapter`` is now a real Python-side hook (matches
    stdlib ``sqlite3.register_adapter``). Common uses: ``Decimal``,
    ``UUID``, ``Path``, ``Enum`` binding. Verify the basic shape.

    Bad-shape rejection raises ``ProgrammingError`` (a PEP 249
    ``Error`` subclass) rather than a bare ``TypeError`` — cross-
    driver code that wraps registry mutations in
    ``except dbapi.Error:`` blocks classifies uniformly.

    Use a test-only sentinel class to avoid polluting the
    module-level ``_ADAPTERS`` dict for ``int`` / ``str`` /
    ``bytes`` etc. that other tests rely on.
    """

    class _RegAdapterSentinel:
        pass

    from dqlitedbapi.exceptions import Error, ProgrammingError
    from dqlitedbapi.types import _ADAPTERS

    # Should not raise:
    dqlitedbapi.register_adapter(_RegAdapterSentinel, str)
    try:
        assert _RegAdapterSentinel in _ADAPTERS
        # Bad shape still fails fast — and raises a PEP 249 Error
        # subclass (ProgrammingError), not bare TypeError.
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
    """``connect()``'s ``**unknown_kwargs`` rejection arm: stdlib
    ``sqlite3.connect`` kwargs that this driver cannot honour
    must raise ``NotSupportedError`` (in the dbapi.Error
    hierarchy) rather than bare ``TypeError`` (escapes the
    hierarchy).

    ``check_same_thread`` is pulled out of this parametrize set
    because it gets a specific, actionable rejection message — see
    ``test_connect_rejects_check_same_thread_with_specific_message``.

    ``isolation_level`` and ``autocommit`` accept their no-op
    sentinel values (``None`` / ``True`` / ``-1``) symmetric with
    the setter — see
    ``test_connect_isolation_level_autocommit_kwargs_symmetric.py``."""
    with pytest.raises(NotSupportedError, match="stdlib sqlite3 kwargs"):
        dqlitedbapi.connect("127.0.0.1:9999", **{kwarg: 0})  # type: ignore[arg-type]


def test_connect_rejects_check_same_thread_with_specific_message() -> None:
    """``check_same_thread`` is rejected with a specific, actionable
    message naming the kwarg, explaining the threading-model
    constraint, and pointing at the workaround.

    The generic "rejects stdlib sqlite3 kwargs not supported by this
    driver" message lumped check_same_thread with the other rejected
    stdlib kwargs (detect_types / factory / cached_statements / uri),
    leaving SA + FastAPI app authors guessing why their canonical
    ``connect_args={"check_same_thread": False}`` workaround caused
    engine construction to fail."""
    with pytest.raises(NotSupportedError) as exc_info:
        dqlitedbapi.connect("127.0.0.1:9999", check_same_thread=False)
    msg = str(exc_info.value)
    # Names the kwarg explicitly so a grep on the operator-facing
    # log surfaces the cause.
    assert "check_same_thread" in msg
    # Explains the threading-model constraint so the reader
    # understands the why, not just the what.
    assert "threadsafety=1" in msg
    # Points at the workaround so the reader's next action is
    # explicit.
    assert "pool" in msg.lower() or "Connection per thread" in msg


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
