"""Pin: dbapi exceptions preserve the un-truncated server text via raw_message.

The client-layer ``OperationalError`` truncates ``message`` to 1 KiB
for safe display but keeps the full text on ``raw_message``. The dbapi
wrapper now plumbs that through so callers don't have to walk
``__cause__`` for the un-truncated diagnostic.
"""

from __future__ import annotations

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import (
    DataError,
    IntegrityError,
    InterfaceError,
    OperationalError,
)


@pytest.mark.asyncio
async def test_operational_error_preserves_raw_message_through_call_client() -> None:
    long_msg = "x" * 5000

    async def _raise() -> None:
        raise _client_exc.OperationalError(1, long_msg)

    with pytest.raises(OperationalError) as ei:
        await _call_client(_raise())
    # ``message`` is the truncated client-layer string (1 KiB cap +
    # the explanatory ``... [truncated, N bytes]`` suffix). Stay
    # well below the original 5 KiB so a future widening of the cap
    # is caught.
    assert len(ei.value.args[0]) < len(long_msg)
    assert "truncated" in ei.value.args[0]
    assert ei.value.code == 1
    # Full text reachable via raw_message.
    assert ei.value.raw_message == long_msg


def test_operational_error_raw_message_defaults_to_message_when_short() -> None:
    """When raw_message is not passed, it falls back to ``message``."""
    exc = OperationalError("short message", code=1)
    assert exc.raw_message == "short message"


def test_data_error_raw_message_defaults() -> None:
    exc = DataError("bad value")
    assert exc.raw_message == "bad value"


def test_integrity_error_raw_message_explicit() -> None:
    exc = IntegrityError("truncated", code=19, raw_message="full server text with details")
    assert exc.raw_message == "full server text with details"


def test_interface_error_does_not_carry_raw_message_field() -> None:
    """Sanity: InterfaceError is plain (not _DatabaseErrorWithCode), so
    no raw_message attribute is added there."""
    exc = InterfaceError("something")
    assert not hasattr(exc, "raw_message")
