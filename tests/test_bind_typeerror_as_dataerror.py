"""Pin: ``_call_client`` narrowly wraps EncodeError as DataError (PEP 249 §7);
bare TypeError/ValueError propagates raw so the originating frame surfaces."""

import pytest

from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import DataError
from dqlitewire import EncodeError


async def _raise(exc: BaseException) -> None:
    raise exc


class TestCallClientWrapsEncodeError:
    async def test_encode_error_becomes_dataerror(self) -> None:
        with pytest.raises(DataError, match="wire encode failed"):
            await _call_client(_raise(EncodeError("not serializable")))

    async def test_encode_error_chained(self) -> None:
        original = EncodeError("unsupported type 'Decimal'")
        with pytest.raises(DataError) as ei:
            await _call_client(_raise(original))
        assert ei.value.__cause__ is original


class TestCallClientPropagatesCoroInternalErrors:
    """Bare TypeError/ValueError from the coro propagates raw, not wrapped as DataError."""

    async def test_bare_typeerror_propagates_raw(self) -> None:
        with pytest.raises(TypeError):
            await _call_client(_raise(TypeError("driver-internal refactor bug")))

    async def test_bare_valueerror_propagates_raw(self) -> None:
        with pytest.raises(ValueError):
            await _call_client(_raise(ValueError("third-party middleware typo")))
