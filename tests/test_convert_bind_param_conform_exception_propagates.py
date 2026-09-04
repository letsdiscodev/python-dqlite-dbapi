"""A user-defined __conform__ raising any exception propagates to the caller (stdlib parity)."""

from __future__ import annotations

import pytest

from dqlitedbapi.types import adapt_bind_param


def test_conform_runtimeerror_propagates_unwrapped() -> None:
    class Bad:
        def __conform__(self, protocol: object) -> object:
            raise RuntimeError("boom from conform")

    with pytest.raises(RuntimeError, match="boom from conform"):
        adapt_bind_param(Bad())


def test_conform_typeerror_propagates_unwrapped() -> None:
    """A __conform__ raising TypeError surfaces with the original exception, not swallowed."""

    class Bad:
        def __conform__(self, protocol: object) -> object:
            raise TypeError("wrong signature")

    with pytest.raises(TypeError, match="wrong signature"):
        adapt_bind_param(Bad())


def test_conform_returning_none_rejects_non_primitive_at_dbapi_layer() -> None:
    """__conform__ returning None leaves the value unchanged; the wire-primitive guard then
    rejects it at the dbapi layer with the original type named in the diagnostic."""
    from dqlitedbapi.exceptions import ProgrammingError

    class Declines:
        def __conform__(self, protocol: object) -> object:
            return None

    with pytest.raises(ProgrammingError, match="Declines"):
        adapt_bind_param(Declines())


def test_conform_returning_adapted_value_used() -> None:
    """__conform__ returning a wire-encodable value: the adapted value is used."""

    class HasConform:
        def __conform__(self, protocol: object) -> object:
            return "adapted!"

    assert adapt_bind_param(HasConform()) == "adapted!"
