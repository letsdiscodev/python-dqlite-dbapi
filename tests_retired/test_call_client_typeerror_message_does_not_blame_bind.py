"""Pin: ``_call_client`` doesn't over-catch (TypeError, ValueError) — they propagate raw,
not as DataError. Only the narrow EncodeError -> DataError wrap remains for bind-shape faults."""

from __future__ import annotations

from typing import Any

import pytest

from dqlitedbapi.cursor import _call_client


@pytest.mark.asyncio
async def test_call_client_typeerror_from_non_bind_path_propagates_raw() -> None:
    """A non-bind TypeError propagates with its original frame, not remapped to DataError."""

    async def race_op() -> Any:
        raise TypeError("asyncio.wait_for got float NaN as timeout")

    with pytest.raises(TypeError, match="asyncio.wait_for"):
        await _call_client(race_op())


@pytest.mark.asyncio
async def test_call_client_valueerror_propagates_raw() -> None:
    """Sibling ValueError path: same narrow-catch discipline."""

    async def race_op() -> Any:
        raise ValueError("unparseable address from a third-party node store")

    with pytest.raises(ValueError, match="unparseable address"):
        await _call_client(race_op())
