"""Pin: a user-defined ``__conform__`` raising a non-PEP-249
exception (RuntimeError, KeyError, custom class) propagates
UNWRAPPED through ``_convert_params``, matching stdlib
``sqlite3.Cursor.execute``'s discipline and the docstring
promise in ``_convert_bind_param``.

The narrow ``except (LookupError, TypeError, ValueError)`` arm in
``_convert_params`` wraps only documented adapter-misuse classes;
arbitrary exceptions from user code propagate unchanged so cross-
driver ``except dbapi.Error:`` does NOT silently swallow
programmer bugs in user-defined ``__conform__`` implementations.

Regression: legitimate adapter-failure paths still wrap as
``DataError``.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _convert_params
from dqlitedbapi.exceptions import DataError


class _BadConform:
    """User class whose ``__conform__`` raises RuntimeError."""

    def __conform__(self, _protocol: object) -> object:
        raise RuntimeError("boom from conform")


def test_conform_runtime_error_propagates_unwrapped() -> None:
    """The RuntimeError from __conform__ must reach the caller as
    a RuntimeError, NOT wrapped as DataError. Stdlib parity.
    """
    with pytest.raises(RuntimeError, match="boom from conform"):
        _convert_params([_BadConform()])


def test_conform_custom_exception_propagates_unwrapped() -> None:
    """An arbitrary user exception (not in the LookupError /
    TypeError / ValueError set) also propagates unwrapped.
    """

    class CustomCallerBug(Exception):
        pass

    class BadCustom:
        def __conform__(self, _protocol: object) -> object:
            raise CustomCallerBug("user-side caller bug")

    with pytest.raises(CustomCallerBug, match="user-side caller bug"):
        _convert_params([BadCustom()])


def test_adapter_lookup_error_still_wraps_as_data_error() -> None:
    """Regression: an adapter-class failure (LookupError /
    TypeError / ValueError) STILL wraps as DataError so
    documented adapter-misuse error shape is preserved.
    """

    class UnknownType:
        # No __conform__, no registered adapter. _convert_bind_param
        # routes through the fallback path which raises a
        # documented error class.
        pass

    # Use a type that triggers a TypeError in the adapter chain.
    # The simplest way: a type that's not str / bytes / int / float /
    # None / bool / datetime — _convert_bind_param will raise.
    class WithFailingConform:
        def __conform__(self, _protocol: object) -> object:
            # Returning None signals "no adaptation"; _convert_bind_param
            # then falls back to its own checks.
            return None

    # A None-returning ``__conform__`` declines adaptation, so no
    # transform runs and the value reaches the post-chain check as an
    # unsupported type — wrapped as ``DataError`` with the "type X is
    # not supported" diagnostic (NOT the "adapter for X produced ..."
    # wording, which is reserved for a real adapter returning a
    # non-primitive).
    with pytest.raises(DataError, match="is not supported"):
        _convert_params([WithFailingConform()])
