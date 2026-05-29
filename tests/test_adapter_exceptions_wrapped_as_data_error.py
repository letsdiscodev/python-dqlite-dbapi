"""A user-registered adapter raising an arbitrary exception must surface as DataError;
adapters already raising a dbapi.Error subclass pass through unchanged (no double-wrap)."""

from __future__ import annotations

from typing import Any

import pytest

import dqlitedbapi
from dqlitedbapi.cursor import _convert_params
from dqlitedbapi.exceptions import DataError, Error, InterfaceError, ProgrammingError


class _Marker:
    # Plain class as registry key; MagicMock's auto-mocked __hash__ breaks dict lookup.
    pass


def test_adapter_attribute_error_wrapped_as_data_error() -> None:
    dqlitedbapi.register_adapter(int, lambda i: i.no_such_attr)
    try:
        with pytest.raises(DataError) as exc_info:
            _convert_params([1])
        assert isinstance(exc_info.value.__cause__, AttributeError)
        assert isinstance(exc_info.value, Error)
    finally:
        dqlitedbapi.unregister_adapter(int)


def test_adapter_runtime_error_wrapped_as_data_error() -> None:
    dqlitedbapi.register_adapter(float, lambda f: (_ for _ in ()).throw(RuntimeError("boom")))
    try:
        with pytest.raises(DataError) as exc_info:
            _convert_params([1.5])
        assert isinstance(exc_info.value.__cause__, RuntimeError)
    finally:
        dqlitedbapi.unregister_adapter(float)


def test_adapter_dbapi_error_passes_through_without_double_wrap() -> None:
    sentinel = DataError("adapter rejected this value")

    def raising_adapter(_v: Any) -> Any:
        raise sentinel

    dqlitedbapi.register_adapter(bytes, raising_adapter)
    try:
        with pytest.raises(DataError) as exc_info:
            _convert_params([b"x"])
        assert exc_info.value is sentinel
        assert exc_info.value.__cause__ is None
    finally:
        dqlitedbapi.unregister_adapter(bytes)


def test_adapter_programming_error_passes_through() -> None:
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
    assert _convert_params([1, 2, 3]) == [1, 2, 3]
    assert _convert_params(None) is None


def test_adapter_interface_error_passes_through() -> None:
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
    """BaseException must NOT be wrapped: doing so would swallow Ctrl-C / break cancellation."""

    def raising_adapter(_v: Any) -> Any:
        raise KeyboardInterrupt("simulated SIGINT during adapter")

    dqlitedbapi.register_adapter(_Marker, raising_adapter)
    try:
        with pytest.raises(KeyboardInterrupt):
            _convert_params([_Marker()])
    finally:
        dqlitedbapi.unregister_adapter(_Marker)


async def test_async_cursor_path_uses_same_wrap_via_shared_helper() -> None:
    """Sync and async conversion share _convert_one_bind_param, so the wrap covers both."""
    from dqlitedbapi.aio import cursor as aio_cursor
    from dqlitedbapi.cursor import _convert_one_bind_param, _convert_params_async

    assert aio_cursor._convert_params_async is _convert_params_async, (  # type: ignore[attr-defined]
        "async cursor must bind via the shared _convert_params_async"
    )

    assert callable(_convert_one_bind_param)
    dqlitedbapi.register_adapter(int, lambda i: i.no_such_attr)
    try:
        with pytest.raises(DataError) as exc_info:
            await _convert_params_async([1])
        assert isinstance(exc_info.value.__cause__, AttributeError)
        assert isinstance(exc_info.value, Error)
    finally:
        dqlitedbapi.unregister_adapter(int)
