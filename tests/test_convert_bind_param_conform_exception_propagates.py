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


def test_conform_returning_none_falls_through() -> None:
    """The protocol-decline path (``__conform__`` returns ``None``)
    leaves the value unchanged so the wire encoder's normal type
    rejection runs — unaffected by the propagate-exception change."""

    class Declines:
        def __conform__(self, protocol: object) -> object:
            return None

    inst = Declines()
    assert _convert_bind_param(inst) is inst


def test_conform_returning_adapted_value_used() -> None:
    """The happy path: ``__conform__`` returns a wire-encodable value;
    the adapted value is used in place of the original."""

    class HasConform:
        def __conform__(self, protocol: object) -> object:
            return "adapted!"

    assert _convert_bind_param(HasConform()) == "adapted!"
