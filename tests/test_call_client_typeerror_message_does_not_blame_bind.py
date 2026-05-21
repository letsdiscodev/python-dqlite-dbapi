"""Pin: ``_call_client`` no longer over-catches ``(TypeError,
ValueError)`` — the bare classes propagate raw so coro-internal
faults surface with their honest stack and not as
``DataError("caller-input fault ...")``.

The wire-encode rejection path (``EncodeError`` -> ``DataError``)
remains the narrow wrap; that arm continues to surface bind-shape
failures as ``DataError`` per PEP 249 §7. See
``test_bind_typeerror_as_dataerror.py`` for that arm's pin.
"""

from __future__ import annotations

from typing import Any

import pytest

from dqlitedbapi.cursor import _call_client


@pytest.mark.asyncio
async def test_call_client_typeerror_from_non_bind_path_propagates_raw() -> None:
    """A non-bind ``TypeError`` (driver refactor bug, third-party
    retry middleware typo, ``asyncio.timeout(NaN)`` misuse) must
    propagate as ``TypeError`` with its original frame intact, NOT
    be remapped to ``DataError("caller-input fault ...")``."""

    async def race_op() -> Any:
        raise TypeError("asyncio.wait_for got float NaN as timeout")

    with pytest.raises(TypeError, match="asyncio.wait_for"):
        await _call_client(race_op())


@pytest.mark.asyncio
async def test_call_client_valueerror_propagates_raw() -> None:
    """Sibling ``ValueError`` path — same narrow-catch discipline."""

    async def race_op() -> Any:
        raise ValueError("unparseable address from a third-party node store")

    with pytest.raises(ValueError, match="unparseable address"):
        await _call_client(race_op())
