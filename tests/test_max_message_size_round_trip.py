"""``max_message_size`` flows from connect()/aconnect() to the connection's slot.

Out-of-range validation lives at the wire layer; the dbapi forwards the value verbatim."""

from __future__ import annotations

from typing import Any

import pytest

import dqlitedbapi
import dqlitedbapi.aio


def test_sync_connect_propagates_max_message_size() -> None:
    conn = dqlitedbapi.connect("localhost:9001", max_message_size=12345)
    assert conn._max_message_size == 12345


def test_sync_connect_default_max_message_size_is_none() -> None:
    """``None`` is the default sentinel; the wire layer supplies its 64 MiB default."""
    conn = dqlitedbapi.connect("localhost:9001")
    assert conn._max_message_size is None


def test_async_connect_propagates_max_message_size() -> None:
    """``aio.connect`` is a sync-shape factory returning an ``AsyncConnection``."""
    conn = dqlitedbapi.aio.connect("localhost:9001", max_message_size=54321)
    assert conn._max_message_size == 54321


def test_async_connect_default_max_message_size_is_none() -> None:
    conn = dqlitedbapi.aio.connect("localhost:9001")
    assert conn._max_message_size is None


@pytest.mark.parametrize(
    "bad_value",
    [
        0,
        -1,
        True,  # bool is a subclass of int, still rejected
        "100",
    ],
)
def test_sync_connect_invalid_max_message_size_rejected_at_protocol_layer(
    bad_value: Any,
) -> None:
    """The dbapi stores bad values verbatim; the wire layer is the sole validator."""
    conn = dqlitedbapi.connect("localhost:9001", max_message_size=bad_value)
    assert conn._max_message_size == bad_value
