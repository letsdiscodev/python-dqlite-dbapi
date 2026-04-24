"""``dqlitedbapi.Connection.__init__`` and ``AsyncConnection.__init__``
eagerly validate the ``address`` argument so a typoed DSN surfaces at
construction time as ``InterfaceError`` rather than at first use with
a less helpful downstream error.
"""

from __future__ import annotations

import pytest

from dqlitedbapi import Connection, InterfaceError
from dqlitedbapi.aio.connection import AsyncConnection


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "no-port",
        "host:",
        "host:abc",
        "host:0",
        "host:70000",
        "::1:9000",  # unbracketed IPv6
    ],
)
def test_sync_init_rejects_invalid_address(bad: str) -> None:
    with pytest.raises(InterfaceError, match="Invalid address"):
        Connection(bad)


def test_sync_init_rejects_non_string_address() -> None:
    with pytest.raises(InterfaceError, match="host:port"):
        Connection(None)  # type: ignore[arg-type]


def test_sync_init_accepts_valid_address() -> None:
    Connection("127.0.0.1:9001")


def test_async_init_rejects_invalid_address() -> None:
    with pytest.raises(InterfaceError, match="Invalid address"):
        AsyncConnection("host:abc")


def test_async_init_rejects_non_string_address() -> None:
    with pytest.raises(InterfaceError, match="host:port"):
        AsyncConnection(None)  # type: ignore[arg-type]


def test_async_init_accepts_valid_address() -> None:
    AsyncConnection("[::1]:9001")
