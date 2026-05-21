"""Pin: every dbapi entry point uses the client-layer's promoted
``DEFAULT_TIMEOUT_SECONDS`` / ``DEFAULT_CLOSE_TIMEOUT_SECONDS``
constants instead of inline literals.

The client at ``dqliteclient/connection.py`` promotes both defaults
to a single source of truth and its block-comment claims "lockstep
across the whole stack." The dbapi layer had not adopted the
promotion: every dbapi entry point shipped a literal ``10.0`` (and
companion ``0.5``), so a future tuning of the client constants
would silently leave the dbapi defaults stale.

This pin locks the five dbapi entry points (``connect``,
``aio.connect``, ``aio.aconnect``, ``Connection.__init__``,
``AsyncConnection.__init__``) to the client-layer constants so a
single ``DEFAULT_TIMEOUT_SECONDS`` bump propagates everywhere.
"""

from __future__ import annotations

import inspect

from dqliteclient import DEFAULT_CLOSE_TIMEOUT_SECONDS, DEFAULT_TIMEOUT_SECONDS
from dqlitedbapi import connect as sync_connect
from dqlitedbapi.aio import aconnect
from dqlitedbapi.aio import connect as aio_connect
from dqlitedbapi.aio.connection import AsyncConnection
from dqlitedbapi.connection import Connection


def test_sync_connect_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(sync_connect).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS


def test_aio_connect_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(aio_connect).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS


def test_aio_aconnect_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(aconnect).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS


def test_sync_connection_class_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(Connection.__init__).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS


def test_async_connection_class_default_timeout_uses_promoted_constant() -> None:
    params = inspect.signature(AsyncConnection.__init__).parameters
    assert params["timeout"].default == DEFAULT_TIMEOUT_SECONDS
    assert params["close_timeout"].default == DEFAULT_CLOSE_TIMEOUT_SECONDS
