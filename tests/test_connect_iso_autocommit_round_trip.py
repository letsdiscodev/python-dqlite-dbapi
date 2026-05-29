"""Pin: ``connect()`` round-trips ``isolation_level``/``autocommit`` kwargs onto the
returned Connection (the factory previously validated then dropped them), and the
connect-site gate matches the setter's exact-int discipline (rejecting ``Decimal('-1')``
/ ``-1.0`` rather than the old loose ``!= -1``).
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import NotSupportedError


def _make_connection_returning(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the live Connection ctor so tests exercise only connect()-level routing."""

    class FakeConnection:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self._isolation_level_value: object = None
            self._autocommit_value: object = True

        @property
        def isolation_level(self) -> object:
            return self._isolation_level_value

        @isolation_level.setter
        def isolation_level(self, value: object) -> None:
            # Mirror the real setter's accept-set: _STDLIB_IMPLICIT_TX_VALUES + None.
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
    """``isolation_level=None`` (stdlib autocommit shape) must land on the connection."""
    _make_connection_returning(monkeypatch)
    conn = dqlitedbapi.connect("host:9001", isolation_level=None)
    assert conn.isolation_level is None


def test_connect_autocommit_loose_equality_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """``Decimal('-1')``/``-1.0`` are == -1 but break isinstance(int); connect() must
    reject them like the setter does."""
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
