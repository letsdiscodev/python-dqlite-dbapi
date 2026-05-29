"""Pin: a user ``__conform__`` raising a non-PEP-249 exception propagates
unwrapped through ``_convert_params``; documented adapter-misuse
(LookupError/TypeError/ValueError) still wraps as ``DataError``.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _convert_params
from dqlitedbapi.exceptions import DataError


class _BadConform:
    def __conform__(self, _protocol: object) -> object:
        raise RuntimeError("boom from conform")


def test_conform_runtime_error_propagates_unwrapped() -> None:
    """The RuntimeError from __conform__ must reach the caller unwrapped."""
    with pytest.raises(RuntimeError, match="boom from conform"):
        _convert_params([_BadConform()])


def test_conform_custom_exception_propagates_unwrapped() -> None:
    """An arbitrary user exception (outside the wrapped set) also propagates unwrapped."""

    class CustomCallerBug(Exception):
        pass

    class BadCustom:
        def __conform__(self, _protocol: object) -> object:
            raise CustomCallerBug("user-side caller bug")

    with pytest.raises(CustomCallerBug, match="user-side caller bug"):
        _convert_params([BadCustom()])


def test_adapter_lookup_error_still_wraps_as_data_error() -> None:
    """Regression: an adapter-misuse failure still wraps as DataError."""

    class UnknownType:
        pass

    # A None-returning ``__conform__`` declines adaptation, so the value
    # reaches the post-chain check as an unsupported type.
    class WithFailingConform:
        def __conform__(self, _protocol: object) -> object:
            return None

    with pytest.raises(DataError, match="is not supported"):
        _convert_params([WithFailingConform()])
