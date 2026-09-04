"""A wire ``EncodeError`` reaching ``_call_client`` must wrap as
``DataError``, not leak past ``except dbapi.Error:``."""

import pytest

from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import DataError
from dqlitewire.exceptions import EncodeError as WireEncodeError


async def test_wire_encode_error_wrapped_as_dataerror() -> None:
    async def raises_wire_encode() -> None:
        raise WireEncodeError("simulated bind-time encode failure")

    with pytest.raises(DataError, match="wire encode failed"):
        await _call_client(raises_wire_encode())


async def test_wire_encode_error_preserves_cause() -> None:
    original = WireEncodeError("simulated")

    async def raises_wire_encode() -> None:
        raise original

    try:
        await _call_client(raises_wire_encode())
    except DataError as e:
        assert e.__cause__ is original
    else:
        pytest.fail("expected DataError")
