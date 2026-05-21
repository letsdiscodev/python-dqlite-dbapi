"""Pin: ``_call_client``'s narrow wrap discipline.

PEP 249 §7 requires bind-time encoder rejections to surface as
``DataError`` (per the "problems with the processed data" arm). The
wire encoder (``dqlitewire.types``) raises ``EncodeError`` for
unsupported bind values (``Decimal``, ``UUID``, ``Path``, ``Enum``,
arbitrary user classes); ``_call_client`` catches ``EncodeError``
and wraps as ``DataError``.

The previous catch was wider — ``except (TypeError, ValueError)``
— and over-attributed coro-internal faults (driver refactor bugs,
third-party retry middleware typos) as ``DataError("caller-input
fault ...")``. The wrap is now narrow: ``EncodeError`` ->
``DataError``; bare ``TypeError`` / ``ValueError`` propagates raw so
the originating frame surfaces honestly.
"""

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
    """Narrow-catch discipline: bare ``TypeError`` / ``ValueError`` from
    the awaited coro propagates raw so operators triaging the failure
    see the originating frame instead of a misleading ``DataError(
    "caller-input fault ...")`` wrap."""

    async def test_bare_typeerror_propagates_raw(self) -> None:
        with pytest.raises(TypeError):
            await _call_client(_raise(TypeError("driver-internal refactor bug")))

    async def test_bare_valueerror_propagates_raw(self) -> None:
        with pytest.raises(ValueError):
            await _call_client(_raise(ValueError("third-party middleware typo")))
