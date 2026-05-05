"""Pin: a wire-layer ``EncodeError`` raised at a point that bypasses
the client's ``_run_protocol`` arm reaches ``_call_client`` directly
and must be wrapped as ``dbapi.DataError`` — NOT propagated as a
bare wire exception (which leaks past ``except dbapi.Error:``).

PEP 249 §7 classifies encode-side / caller-input issues as
``DataError``.
"""

import pytest

from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import DataError
from dqlitewire.exceptions import EncodeError as WireEncodeError


@pytest.mark.asyncio
async def test_wire_encode_error_wrapped_as_dataerror() -> None:
    async def raises_wire_encode() -> None:
        raise WireEncodeError("simulated bind-time encode failure")

    with pytest.raises(DataError, match="wire encode failed"):
        await _call_client(raises_wire_encode())


@pytest.mark.asyncio
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
