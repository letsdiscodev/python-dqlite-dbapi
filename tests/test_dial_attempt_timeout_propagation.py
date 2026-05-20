"""Pin: ``dial_timeout`` and ``attempt_timeout`` are surfaced on
the dbapi-sync ``connect`` / ``Connection`` and the dbapi-async
``connect`` / ``aconnect`` / ``AsyncConnection`` entry points,
and forwarded through ``_build_and_connect`` into the underlying
:class:`DqliteConnection`.

The two knobs mirror go-dqlite's ``Config.DialTimeout`` /
``Config.AttemptTimeout``. The client layer (dqliteclient) exposes
them; without this propagation, dbapi / SA users could only
configure ``timeout``, collapsing all three knobs onto the per-RPC
budget. The wire-byte path is unchanged; the propagation is purely
about reaching the operator-tunable knobs from the higher layers.
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
    """``dqlitedbapi.connect`` exposes the two knobs as keyword
    arguments with ``None`` defaults (collapse onto ``timeout``)."""
    sig = inspect.signature(dqlitedbapi.connect)
    assert "dial_timeout" in sig.parameters
    assert "attempt_timeout" in sig.parameters
    assert sig.parameters["dial_timeout"].default is None
    assert sig.parameters["attempt_timeout"].default is None


def test_sync_connection_init_signature_carries_dial_and_attempt_timeout() -> None:
    """``dqlitedbapi.Connection.__init__`` exposes the same knobs."""
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
    """``_build_and_connect`` must accept the kwargs so the layer
    above can forward them through. A regression that drops the
    kwargs from the signature would silently strip operator
    configuration."""
    sig = inspect.signature(_build_and_connect)
    assert "dial_timeout" in sig.parameters
    assert "attempt_timeout" in sig.parameters


def test_sync_connection_stores_dial_and_attempt_timeout() -> None:
    """Construction stores the values on private attributes that
    are read at the ``_build_and_connect`` call site."""
    conn = Connection("127.0.0.1:9001", timeout=10.0, dial_timeout=0.5, attempt_timeout=1.0)
    assert conn._dial_timeout == 0.5
    assert conn._attempt_timeout == 1.0


def test_async_connection_stores_dial_and_attempt_timeout() -> None:
    """Same for the async surface."""
    aconn = AsyncConnection("127.0.0.1:9001", timeout=10.0, dial_timeout=0.25, attempt_timeout=2.0)
    assert aconn._dial_timeout == 0.25
    assert aconn._attempt_timeout == 2.0


def test_sync_connection_dial_timeout_validated_at_construction() -> None:
    """The same ``_validate_timeout`` discipline as ``timeout`` /
    ``close_timeout``: zero / negative / non-finite values are
    rejected at construction, not silently forwarded."""
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
    """``None`` defaults preserve the existing single-knob shape
    so no behavior changes for callers that don't pass the new
    kwargs."""
    aconn = AsyncConnection("127.0.0.1:9001", timeout=10.0)
    assert aconn._dial_timeout is None
    assert aconn._attempt_timeout is None
