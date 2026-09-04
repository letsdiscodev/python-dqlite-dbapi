"""``adapt_bind_param`` rejects ``str`` binds with embedded NUL bytes via a ``DataError``
naming the BLOB workaround, before the wire encoder runs. dqlite encodes TEXT as
NUL-terminated UTF-8 so embedded ``\\x00`` cannot round-trip (unlike stdlib sqlite3)."""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import adapt_bind_param


def test_text_with_embedded_nul_rejected_with_blob_hint() -> None:
    """The pre-encode guard fires, naming both 'embedded NUL' and the 'BLOB' workaround."""
    with pytest.raises(DataError) as excinfo:
        adapt_bind_param("hello\x00world")
    message = str(excinfo.value)
    assert "embedded NUL" in message
    assert "BLOB" in message
    # Offset is named so operators can identify the byte in a mixed payload.
    assert "offset 5" in message


def test_text_nul_rejection_is_in_dbapi_error_hierarchy() -> None:
    """The rejection inherits from ``dbapi.Error`` for uniform ``except dbapi.Error:`` catch."""
    with pytest.raises(dqlitedbapi.Error):
        adapt_bind_param("\x00")


def test_bytes_with_embedded_nul_unaffected() -> None:
    """The bytes/BLOB path round-trips NUL-containing data — the workaround the diagnostic
    points at; guard must not regress to a blanket reject."""
    payload = b"hello\x00world"
    assert adapt_bind_param(payload) is payload
    payload_ba = bytearray(b"hello\x00world")
    assert adapt_bind_param(payload_ba) is payload_ba
    payload_mv = memoryview(b"hello\x00world")
    assert adapt_bind_param(payload_mv) is payload_mv


def test_text_without_nul_unaffected() -> None:
    """Plain TEXT binds (no embedded NUL) pass through unchanged."""
    assert adapt_bind_param("hello world") == "hello world"
    assert adapt_bind_param("") == ""
    assert adapt_bind_param("unicode: 日本語") == "unicode: 日本語"


def test_adapter_producing_str_with_nul_also_rejected() -> None:
    """The guard runs AFTER the adapter chain, so an adapter producing a NUL-bearing ``str``
    is also caught rather than escaping into the wire encoder."""

    class _NeedsAdapter:
        pass

    dqlitedbapi.register_adapter(_NeedsAdapter, lambda _v: "leading\x00trailing")
    try:
        with pytest.raises(DataError, match="embedded NUL"):
            adapt_bind_param(_NeedsAdapter())
    finally:
        dqlitedbapi.unregister_adapter(_NeedsAdapter)
