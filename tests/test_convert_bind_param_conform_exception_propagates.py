"""Pin: a user-defined ``__conform__`` raising any exception
propagates to the caller, matching stdlib ``sqlite3``.

Stdlib parity probe (CPython 3.13):

    >>> class Bad:
    ...     def __conform__(self, protocol):
    ...         raise RuntimeError("boom from conform")
    >>> import sqlite3
    >>> sqlite3.connect(":memory:").execute("SELECT ?", (Bad(),))
    Traceback (most recent call last):
      ...
    RuntimeError: boom from conform

The dqlite driver previously swallowed the exception and let the
caller's misuse silently surface as a wire ``EncodeError`` →
``DataError`` deep inside ``_call_client``. The hidden disposition
diverged from stdlib AND erased the user's traceback.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.types import _convert_bind_param


def test_conform_runtimeerror_propagates_unwrapped() -> None:
    """A ``__conform__`` raising ``RuntimeError`` propagates as-is."""

    class Bad:
        def __conform__(self, protocol: object) -> object:
            raise RuntimeError("boom from conform")

    with pytest.raises(RuntimeError, match="boom from conform"):
        _convert_bind_param(Bad())


def test_conform_typeerror_propagates_unwrapped() -> None:
    """A ``__conform__`` raising ``TypeError`` (e.g., wrong signature
    written by a user) surfaces with the original class so the
    developer can debug. The previous swallow erased this."""

    class Bad:
        def __conform__(self, protocol: object) -> object:
            raise TypeError("wrong signature")

    with pytest.raises(TypeError, match="wrong signature"):
        _convert_bind_param(Bad())


def test_conform_returning_none_rejects_non_primitive_at_dbapi_layer() -> None:
    """When ``__conform__`` returns ``None`` (protocol decline), the
    value is left unchanged; the post-chain wire-primitive guard
    then rejects the non-primitive at the dbapi layer with
    ``ProgrammingError`` (not at the wire encoder with
    ``EncodeError``). The diagnostic names the original input type
    so operators can locate the misregistration site."""
    from dqlitedbapi.exceptions import DataError

    class Declines:
        def __conform__(self, protocol: object) -> object:
            return None

    with pytest.raises(DataError, match="Declines"):
        _convert_bind_param(Declines())


def test_conform_returning_adapted_value_used() -> None:
    """The happy path: ``__conform__`` returns a wire-encodable value;
    the adapted value is used in place of the original."""

    class HasConform:
        def __conform__(self, protocol: object) -> object:
            return "adapted!"

    assert _convert_bind_param(HasConform()) == "adapted!"
