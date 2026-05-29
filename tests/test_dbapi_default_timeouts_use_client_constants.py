"""Pin: every dbapi entry point uses the client-layer's promoted
timeout constants, not inline literals, so a bump propagates everywhere."""

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
