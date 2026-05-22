"""Pin: ``max_message_size`` kwarg flows from
``dqlitedbapi.connect()`` / ``aconnect()`` through
``Connection`` / ``AsyncConnection`` to the underlying client
:class:`DqliteConnection`'s ``_max_message_size`` slot.

The client layer already plumbs the knob through (see
done/client-wire-max-message-size-64mib-cap-not-propagated-as-knob.md).
This test pins the dbapi-side propagation that was missing.

Out-of-range validation happens at the wire layer
(``DqliteProtocol`` rejects non-int / bool / non-positive); the
dbapi forward passes the value through verbatim.
"""

from __future__ import annotations

from typing import Any

import pytest

import dqlitedbapi
import dqlitedbapi.aio


def test_sync_connect_propagates_max_message_size() -> None:
    """``connect(address, max_message_size=N)`` constructs a
    ``Connection`` whose stored slot equals ``N``. The actual
    underlying ``DqliteConnection`` is built lazily on first use;
    the dbapi-side slot is what gets forwarded.
    """
    conn = dqlitedbapi.connect("localhost:9001", max_message_size=12345)
    assert conn._max_message_size == 12345
    # Test does not connect to a real cluster; no close needed.


def test_sync_connect_default_max_message_size_is_none() -> None:
    """``None`` is the default sentinel — the wire layer fills in its
    own 64 MiB default when ``max_message_size`` is absent.
    """
    conn = dqlitedbapi.connect("localhost:9001")
    assert conn._max_message_size is None


def test_async_connect_propagates_max_message_size() -> None:
    """``aio.connect(address, max_message_size=N)`` is the sync-shape
    factory that returns an ``AsyncConnection``. Same propagation
    expectation as the sync sibling.
    """
    conn = dqlitedbapi.aio.connect("localhost:9001", max_message_size=54321)
    assert conn._max_message_size == 54321


def test_async_connect_default_max_message_size_is_none() -> None:
    conn = dqlitedbapi.aio.connect("localhost:9001")
    assert conn._max_message_size is None


@pytest.mark.parametrize(
    "bad_value",
    [
        0,  # protocol layer rejects: must be >= 1
        -1,
        True,  # bool rejected (subclass of int)
        "100",  # type error from protocol layer
    ],
)
def test_sync_connect_invalid_max_message_size_rejected_at_protocol_layer(
    bad_value: Any,
) -> None:
    """The dbapi forward does not re-validate ``max_message_size``;
    pathological values reach the wire layer which rejects with
    ``TypeError`` (bool, str) or ``ValueError`` (non-positive int).

    The dbapi just constructs and stores the value; the protocol
    layer's ``ValueError`` / ``TypeError`` surfaces at the eventual
    ``connect()`` call (out-of-scope here — the slot read above
    confirms the value reached the dbapi storage). This test pins
    that the dbapi does NOT pre-emptively reject (single source of
    truth at the protocol layer).
    """
    conn = dqlitedbapi.connect("localhost:9001", max_message_size=bad_value)
    assert conn._max_message_size == bad_value
