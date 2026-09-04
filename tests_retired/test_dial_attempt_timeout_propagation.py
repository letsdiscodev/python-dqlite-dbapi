"""``dial_timeout`` / ``attempt_timeout`` are surfaced on every sync/async connect
entry point and forwarded through ``_build_and_connect`` into ``DqliteConnection``.
"""

from __future__ import annotations

import inspect

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect
from dqlitedbapi.aio import connect as aio_connect
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection, _build_and_connect
from dqlitedbapi.exceptions import ProgrammingError


def test_sync_connect_signature_carries_dial_and_attempt_timeout() -> None:
    """The knobs are keyword args with ``None`` defaults that collapse onto ``timeout``."""
    sig = inspect.signature(dqlitedbapi.connect)
    assert "dial_timeout" in sig.parameters
    assert "attempt_timeout" in sig.parameters
    assert sig.parameters["dial_timeout"].default is None
    assert sig.parameters["attempt_timeout"].default is None


def test_sync_connection_init_signature_carries_dial_and_attempt_timeout() -> None:
    sig = inspect.signature(Connection.__init__)
    assert "dial_timeout" in sig.parameters
    assert "attempt_timeout" in sig.parameters


def test_aio_connect_signature_carries_dial_and_attempt_timeout() -> None:
    sig = inspect.signature(aio_connect)
    assert "dial_timeout" in sig.parameters
    assert "attempt_timeout" in sig.parameters


def test_aconnect_signature_carries_dial_and_attempt_timeout() -> None:
    sig = inspect.signature(aconnect)
    assert "dial_timeout" in sig.parameters
    assert "attempt_timeout" in sig.parameters


def test_async_connection_init_signature_carries_dial_and_attempt_timeout() -> None:
    sig = inspect.signature(AsyncConnection.__init__)
    assert "dial_timeout" in sig.parameters
    assert "attempt_timeout" in sig.parameters


def test_build_and_connect_signature_forwards_dial_and_attempt_timeout() -> None:
    """``_build_and_connect`` must accept the kwargs; dropping them silently strips
    operator configuration."""
    sig = inspect.signature(_build_and_connect)
    assert "dial_timeout" in sig.parameters
    assert "attempt_timeout" in sig.parameters


def test_sync_connection_stores_dial_and_attempt_timeout() -> None:
    conn = Connection("127.0.0.1:9001", timeout=10.0, dial_timeout=0.5, attempt_timeout=1.0)
    assert conn._dial_timeout == 0.5
    assert conn._attempt_timeout == 1.0


def test_async_connection_stores_dial_and_attempt_timeout() -> None:
    aconn = AsyncConnection("127.0.0.1:9001", timeout=10.0, dial_timeout=0.25, attempt_timeout=2.0)
    assert aconn._dial_timeout == 0.25
    assert aconn._attempt_timeout == 2.0


def test_sync_connection_dial_timeout_validated_at_construction() -> None:
    """Zero / negative / non-finite values are rejected at construction."""
    with pytest.raises(ProgrammingError):
        Connection("127.0.0.1:9001", dial_timeout=0.0)
    with pytest.raises(ProgrammingError):
        Connection("127.0.0.1:9001", dial_timeout=-1.0)


def test_sync_connection_attempt_timeout_validated_at_construction() -> None:
    with pytest.raises(ProgrammingError):
        Connection("127.0.0.1:9001", attempt_timeout=0.0)
    with pytest.raises(ProgrammingError):
        Connection("127.0.0.1:9001", attempt_timeout=-0.001)


def test_async_connection_dial_timeout_validated_at_construction() -> None:
    with pytest.raises(ProgrammingError):
        AsyncConnection("127.0.0.1:9001", dial_timeout=-2.0)


def test_async_connection_defaults_collapse_to_none() -> None:
    """``None`` defaults preserve the single-knob shape for callers not passing them."""
    aconn = AsyncConnection("127.0.0.1:9001", timeout=10.0)
    assert aconn._dial_timeout is None
    assert aconn._attempt_timeout is None
