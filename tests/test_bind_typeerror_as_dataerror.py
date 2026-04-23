"""PEP 249 §7: bind-time ``TypeError`` from the wire encoder surfaces
as ``DataError``.

The wire codec accepts only ``bool``/``int``/``float``/``str``/
``bytes``/``None``. Values outside that set (``Decimal``, ``UUID``,
``Path``, ``Enum``, arbitrary user classes, …) raise ``TypeError``
deep inside the encoder. Without a PEP 249 wrap, that ``TypeError``
leaks past the dbapi boundary as a non-``Error`` exception —
callers that use ``except dqlitedbapi.Error`` would not see it.

``_call_client`` is the single choke point for every wire round-trip
from the dbapi layer; wrapping ``TypeError`` (and ``ValueError``,
which the wire layer also raises) there into ``DataError`` covers
both sync and async cursors in one place.
"""

import pytest

from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import DataError


async def _raise(exc: BaseException) -> None:
    raise exc


class TestCallClientWrapsTypeError:
    async def test_typeerror_becomes_dataerror(self) -> None:
        with pytest.raises(DataError, match="cannot bind"):
            await _call_client(_raise(TypeError("not serializable")))

    async def test_valueerror_becomes_dataerror(self) -> None:
        with pytest.raises(DataError, match="cannot bind"):
            await _call_client(_raise(ValueError("bad value")))

    async def test_typeerror_chained(self) -> None:
        original = TypeError("unsupported type 'Decimal'")
        with pytest.raises(DataError) as ei:
            await _call_client(_raise(original))
        assert ei.value.__cause__ is original
