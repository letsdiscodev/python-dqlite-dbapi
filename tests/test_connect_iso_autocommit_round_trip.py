"""Pin: ``dqlitedbapi.connect()`` round-trips ``isolation_level`` /
``autocommit`` kwargs onto the returned ``Connection``.

The factory previously popped + validated both kwargs and then
silently dropped them — ``connect(addr, autocommit=-1).autocommit``
returned ``True``, and ``connect(addr, isolation_level="DEFERRED")
.isolation_level`` returned ``None``. The setter docstrings
explicitly promise the cross-driver porting idiom
(``conn.autocommit = LEGACY_TRANSACTION_CONTROL; assert
conn.autocommit == -1``); the constructor surface mirrors that now.

Also pins the tight autocommit gate at the connect-site: the setter
rejects ``Decimal('-1')`` / ``-1.0`` / custom-``__eq__`` objects with
``NotSupportedError``; the connect factory's previous loose ``!= -1``
predicate accepted them. Connect-site and setter-side validation must
agree.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import NotSupportedError


def _make_connection_returning(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the live ``Connection`` ctor with a no-op MagicMock so
    the tests exercise only the connect()-level kwarg routing."""

    class FakeConnection:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self._isolation_level_value: object = None
            self._autocommit_value: object = True

        @property
        def isolation_level(self) -> object:
            return self._isolation_level_value

        @isolation_level.setter
        def isolation_level(self, value: object) -> None:
            # Mirror the real setter's accept-set used at __init__ time:
            # _STDLIB_IMPLICIT_TX_VALUES + None.
            from dqlitedbapi.connection import _STDLIB_IMPLICIT_TX_VALUES

            if value is None or (
                isinstance(value, str) and value.upper() in _STDLIB_IMPLICIT_TX_VALUES
            ):
                self._isolation_level_value = value
            else:
                raise NotSupportedError(f"unsupported isolation_level={value!r}")

        @property
        def autocommit(self) -> object:
            return self._autocommit_value

        @autocommit.setter
        def autocommit(self, value: object) -> None:
            if value is True:
                self._autocommit_value = True
                return
            if isinstance(value, int) and not isinstance(value, bool) and value == -1:
                self._autocommit_value = -1
                return
            raise NotSupportedError(f"unsupported autocommit={value!r}")

    monkeypatch.setattr(dqlitedbapi, "Connection", FakeConnection)


def test_connect_routes_autocommit_through_setter(monkeypatch: pytest.MonkeyPatch) -> None:
    _make_connection_returning(monkeypatch)
    conn = dqlitedbapi.connect("host:9001", autocommit=-1)
    assert conn.autocommit == -1


def test_connect_routes_isolation_level_through_setter(monkeypatch: pytest.MonkeyPatch) -> None:
    _make_connection_returning(monkeypatch)
    conn = dqlitedbapi.connect("host:9001", isolation_level="DEFERRED")
    assert conn.isolation_level == "DEFERRED"


def test_connect_isolation_level_none_does_not_overwrite_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``isolation_level=None`` is the stdlib autocommit shape — pass
    it through the setter so the value lands on the connection."""
    _make_connection_returning(monkeypatch)
    conn = dqlitedbapi.connect("host:9001", isolation_level=None)
    assert conn.isolation_level is None


def test_connect_autocommit_loose_equality_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """The connect-site gate must mirror the setter's exact-int
    discipline. ``Decimal('-1')`` / ``-1.0`` are loosely equal to ``-1``
    via Python's ``==`` but break the ``isinstance(conn.autocommit,
    int)`` introspection idiom; the setter rejects them and so must
    ``connect()``."""
    _make_connection_returning(monkeypatch)
    with pytest.raises(NotSupportedError):
        dqlitedbapi.connect("host:9001", autocommit=Decimal("-1"))
    with pytest.raises(NotSupportedError):
        dqlitedbapi.connect("host:9001", autocommit=-1.0)


def test_connect_autocommit_true_preserved(monkeypatch: pytest.MonkeyPatch) -> None:
    _make_connection_returning(monkeypatch)
    conn = dqlitedbapi.connect("host:9001", autocommit=True)
    assert conn.autocommit is True


# Quiet "unused" lint for the MagicMock import.
_ = MagicMock
