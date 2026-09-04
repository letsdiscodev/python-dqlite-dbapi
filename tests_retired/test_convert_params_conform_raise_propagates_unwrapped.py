"""Pin: a user ``__conform__`` raising a non-PEP-249 exception propagates
unwrapped through ``_convert_params``; a declined (None-returning) ``__conform__``
leaves an unsupported type, which raises ``ProgrammingError`` (stdlib parity).
"""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _convert_params
from dqlitedbapi.exceptions import ProgrammingError


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


def test_unsupported_type_after_declined_conform_raises_programming_error() -> None:
    """A None-returning ``__conform__`` declines adaptation, so the value reaches
    the post-chain check as an unsupported type → ProgrammingError (stdlib parity)."""

    class UnknownType:
        pass

    class WithFailingConform:
        def __conform__(self, _protocol: object) -> object:
            return None

    with pytest.raises(ProgrammingError, match="is not supported"):
        _convert_params([WithFailingConform()])
