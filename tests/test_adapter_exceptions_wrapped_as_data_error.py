"""Pin: a user-registered adapter that raises an arbitrary exception
must surface as a ``dbapi.DataError`` (within the PEP 249 ``Error``
hierarchy), not as a bare exception escaping that hierarchy.

PEP 249 §7 mandates that all database-related failures funnel through
``Error`` subclasses so cross-driver code can write
``except dbapi.Error:`` and catch every DB-side failure. The wire-
encode path is correctly wrapped at ``_call_client``; the adapter
path runs one frame earlier in ``_convert_params`` and was the
asymmetric hole.

A concrete trap: ``register_adapter(int, lambda i: i.no_such_attr)``
raises ``AttributeError``, which propagates past ``except dbapi.Error:``
blocks. The fix wraps adapter-call exceptions as ``DataError`` from
the original. Adapters that already raise ``dbapi.Error`` subclasses
(e.g. raise ``DataError`` directly) pass through unchanged — no
double-wrap.
"""

from __future__ import annotations

from typing import Any

import pytest

import dqlitedbapi
from dqlitedbapi.cursor import _convert_params
from dqlitedbapi.exceptions import DataError, Error, InterfaceError, ProgrammingError


class _Marker:
    """A plain class used as the adapter registry key. Avoids
    ``MagicMock`` whose auto-mocked ``__hash__`` breaks dict lookup
    (the registered class and the runtime ``type(instance)`` compare
    equal but hash differently)."""


def test_adapter_attribute_error_wrapped_as_data_error() -> None:
    """An adapter raising ``AttributeError`` must surface as
    ``DataError`` (subclass of ``dbapi.Error``).
    """
    dqlitedbapi.register_adapter(int, lambda i: i.no_such_attr)
    try:
        with pytest.raises(DataError) as exc_info:
            _convert_params([1])
        # The original AttributeError is preserved on __cause__.
        assert isinstance(exc_info.value.__cause__, AttributeError)
        # The wrapped exception is a member of the PEP 249 hierarchy.
        assert isinstance(exc_info.value, Error)
    finally:
        dqlitedbapi.unregister_adapter(int)


def test_adapter_runtime_error_wrapped_as_data_error() -> None:
    """An adapter raising ``RuntimeError`` (a non-``Error`` exception)
    must also be wrapped as ``DataError``.
    """
    dqlitedbapi.register_adapter(float, lambda f: (_ for _ in ()).throw(RuntimeError("boom")))
    try:
        with pytest.raises(DataError) as exc_info:
            _convert_params([1.5])
        assert isinstance(exc_info.value.__cause__, RuntimeError)
    finally:
        dqlitedbapi.unregister_adapter(float)


def test_adapter_dbapi_error_passes_through_without_double_wrap() -> None:
    """An adapter that raises a ``dbapi.Error`` subclass directly
    (e.g. ``DataError("invalid value")``) must propagate unchanged —
    no double-wrap.
    """
    sentinel = DataError("adapter rejected this value")

    def raising_adapter(_v: Any) -> Any:
        raise sentinel

    dqlitedbapi.register_adapter(bytes, raising_adapter)
    try:
        with pytest.raises(DataError) as exc_info:
            _convert_params([b"x"])
        # Same instance, not a wrapped copy
        assert exc_info.value is sentinel
        # __cause__ is unset because no wrap happened
        assert exc_info.value.__cause__ is None
    finally:
        dqlitedbapi.unregister_adapter(bytes)


def test_adapter_programming_error_passes_through() -> None:
    """Other ``dbapi.Error`` subclasses also pass through unchanged
    (regression-fence the no-double-wrap discipline)."""
    sentinel = ProgrammingError("adapter rejected the binding shape")

    def raising_adapter(_v: Any) -> Any:
        raise sentinel

    dqlitedbapi.register_adapter(complex, raising_adapter)
    try:
        with pytest.raises(ProgrammingError) as exc_info:
            _convert_params([complex(1, 2)])
        assert exc_info.value is sentinel
    finally:
        dqlitedbapi.unregister_adapter(complex)


def test_no_adapter_path_unaffected() -> None:
    """Negative pin: parameters without a registered adapter still
    pass through with the existing built-in conversion (datetime →
    iso8601, etc.). The adapter wrap must not regress the non-adapter
    path.
    """
    # int has no adapter, so passes through unchanged
    assert _convert_params([1, 2, 3]) == [1, 2, 3]
    # None passes through
    assert _convert_params(None) is None


def test_adapter_interface_error_passes_through() -> None:
    """``dbapi.InterfaceError`` is a sibling of ``DataError`` in the
    hierarchy; an adapter raising it directly should pass through
    unchanged (the wrap is "exception is not in the Error hierarchy",
    not "exception is not DataError")."""
    sentinel = InterfaceError("driver-misuse condition from adapter")

    def raising_adapter(_v: Any) -> Any:
        raise sentinel

    dqlitedbapi.register_adapter(_Marker, raising_adapter)
    try:
        with pytest.raises(InterfaceError) as exc_info:
            _convert_params([_Marker()])
        assert exc_info.value is sentinel
    finally:
        dqlitedbapi.unregister_adapter(_Marker)


def test_adapter_baseexception_passes_through_unwrapped() -> None:
    """``BaseException`` subclasses (``KeyboardInterrupt``,
    ``SystemExit``, ``GeneratorExit``, ``asyncio.CancelledError``)
    must NOT be wrapped — wrapping them as ``DataError`` would
    swallow Ctrl-C and break async cancellation. PEP 249 governs
    database errors, not control-flow signals. The wrap uses
    ``except Exception``, deliberately excluding ``BaseException``.

    Pin so a future refactor to ``except BaseException`` regresses
    here.
    """

    def raising_adapter(_v: Any) -> Any:
        raise KeyboardInterrupt("simulated SIGINT during adapter")

    dqlitedbapi.register_adapter(_Marker, raising_adapter)
    try:
        # KeyboardInterrupt must propagate unwrapped — NOT as DataError.
        with pytest.raises(KeyboardInterrupt):
            _convert_params([_Marker()])
    finally:
        dqlitedbapi.unregister_adapter(_Marker)


async def test_async_cursor_path_uses_same_wrap_via_shared_helper() -> None:
    """The sync ``_convert_params`` and the async
    ``_convert_params_async`` funnel every value through the shared
    ``_convert_one_bind_param`` helper, so the adapter-exception wrap is
    implemented once and covers both surfaces. The async cursor binds via
    ``_convert_params_async`` (which additionally yields cooperatively on
    large binds); verify the wrap still holds on that route.
    """
    from dqlitedbapi.aio import cursor as aio_cursor
    from dqlitedbapi.cursor import _convert_one_bind_param, _convert_params_async

    # Structural pin: the async cursor binds via the shared cooperative-
    # yield converter, not a private copy of the conversion logic.
    assert aio_cursor._convert_params_async is _convert_params_async, (  # type: ignore[attr-defined]
        "async cursor must bind via the shared _convert_params_async"
    )

    # Behavioural pin via the async route: an adapter raising a non-Error
    # exception surfaces as DataError with the original on __cause__,
    # exactly as the sync path does — because both call
    # ``_convert_one_bind_param``.
    assert callable(_convert_one_bind_param)
    dqlitedbapi.register_adapter(int, lambda i: i.no_such_attr)
    try:
        with pytest.raises(DataError) as exc_info:
            await _convert_params_async([1])
        assert isinstance(exc_info.value.__cause__, AttributeError)
        assert isinstance(exc_info.value, Error)
    finally:
        dqlitedbapi.unregister_adapter(int)
