"""Pin: ``_call_client``'s broad ``(TypeError, ValueError)`` arm
surfaces a ``DataError`` whose message names the original exception
class — not the previous ``"cannot bind parameter: ..."`` prefix that
falsely attributed every in-coro fault to the user's bind value.

The arm catches all ``TypeError`` / ``ValueError`` from the awaited
coroutine, including non-bind sources (a future
``asyncio.timeout(invalid)`` / ``asyncio.wait_for`` misuse, a
refactor-bug raising one of those classes elsewhere). Operators
triaging logs need the message to identify the actual root-cause
class rather than blame the user's bind values.
"""

from __future__ import annotations

from typing import Any

import pytest

from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import DataError


@pytest.mark.asyncio
async def test_call_client_typeerror_from_non_bind_path_does_not_blame_bind() -> None:
    """A non-bind ``TypeError`` (e.g. a contributor bug deep in a
    future refactor's ``asyncio.timeout(NaN)``) must surface as
    ``DataError`` with a class-named prefix, not as
    ``"cannot bind parameter: ..."``."""

    async def race_op() -> Any:
        raise TypeError("asyncio.wait_for got float NaN as timeout")

    with pytest.raises(DataError) as info:
        await _call_client(race_op())

    msg = str(info.value)
    assert "cannot bind parameter" not in msg, (
        "The wrap must not attribute the fault to the bind value "
        "when the actual root cause was a non-bind TypeError. "
        f"Got: {msg!r}"
    )
    assert "TypeError" in msg, (
        f"Message must name the original class so operators can triage logs. Got: {msg!r}"
    )


@pytest.mark.asyncio
async def test_call_client_valueerror_message_surfaces_class_name() -> None:
    """Sibling ``ValueError`` path — same shape."""

    async def race_op() -> Any:
        raise ValueError("unparseable address from a third-party node store")

    with pytest.raises(DataError) as info:
        await _call_client(race_op())

    msg = str(info.value)
    assert "cannot bind parameter" not in msg
    assert "ValueError" in msg


@pytest.mark.asyncio
async def test_call_client_typeerror_raw_message_unchanged() -> None:
    """The ``raw_message`` attribute (used by SA's ``is_disconnect``
    substring scan) preserves the original message verbatim — only
    the user-facing ``args[0]`` carries the class-named prefix."""

    async def race_op() -> Any:
        raise TypeError("original error text")

    with pytest.raises(DataError) as info:
        await _call_client(race_op())

    assert info.value.raw_message == "original error text"
