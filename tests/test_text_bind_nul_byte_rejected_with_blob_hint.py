"""Pin: ``_convert_bind_param`` rejects ``str`` bind values containing
embedded NUL bytes with a ``DataError`` naming the BLOB workaround,
BEFORE the wire encoder runs.

dqlite's wire protocol encodes TEXT as NUL-terminated UTF-8, so an
embedded ``"\\x00"`` cannot round-trip. The wire encoder
(``dqlitewire.encode_text``) already rejects with ``EncodeError``,
which the cursor's ``_call_client`` wraps as ``DataError`` — but the
wire-internal diagnostic doesn't point at the actionable workaround
(use a BLOB column / bind ``bytes``).

Stdlib ``sqlite3.sqlite3_bind_text`` accepts embedded NULs faithfully
(``sqlite3_column_text`` truncates at the first NUL on read-back, but
``sqlite3_column_blob`` round-trips), so cross-driver code that stores
NUL-containing TEXT silently works against stdlib but raises against
dqlite. Surface a discoverable dbapi-layer diagnostic so operators
porting from stdlib see the BLOB hint immediately.

The bytes path is unaffected — bind ``bytes`` / ``memoryview`` against
a BLOB column to round-trip NUL-containing data.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import DataError
from dqlitedbapi.types import _convert_bind_param


def test_text_with_embedded_nul_rejected_with_blob_hint() -> None:
    """The dbapi-layer pre-encode guard fires and names the BLOB
    workaround. Catches both the canonical 'embedded NUL' phrasing
    and the actionable 'BLOB' workaround hint so operators porting
    from stdlib see the path forward without walking ``__cause__``.
    """
    with pytest.raises(DataError) as excinfo:
        _convert_bind_param("hello\x00world")
    message = str(excinfo.value)
    assert "embedded NUL" in message
    assert "BLOB" in message
    # The offset is named so operators can identify the byte in a
    # mixed-binary payload.
    assert "offset 5" in message


def test_text_nul_rejection_is_in_dbapi_error_hierarchy() -> None:
    """The rejection inherits from ``dbapi.Error`` so cross-driver
    code's ``except dbapi.Error:`` arm catches uniformly."""
    with pytest.raises(dqlitedbapi.Error):
        _convert_bind_param("\x00")


def test_bytes_with_embedded_nul_unaffected() -> None:
    """The BLOB / bytes path round-trips NUL-containing data — this
    is the canonical workaround the diagnostic points at. Pin the
    bytes pass-through so the guard cannot regress to a blanket
    reject.
    """
    payload = b"hello\x00world"
    assert _convert_bind_param(payload) is payload
    payload_ba = bytearray(b"hello\x00world")
    assert _convert_bind_param(payload_ba) is payload_ba
    payload_mv = memoryview(b"hello\x00world")
    assert _convert_bind_param(payload_mv) is payload_mv


def test_text_without_nul_unaffected() -> None:
    """Plain TEXT binds (no embedded NUL) pass through unchanged."""
    assert _convert_bind_param("hello world") == "hello world"
    assert _convert_bind_param("") == ""
    assert _convert_bind_param("unicode: 日本語") == "unicode: 日本語"


def test_adapter_producing_str_with_nul_also_rejected() -> None:
    """The guard runs AFTER the adapter / ``__conform__`` chain so an
    adapter that legitimately produces a NUL-bearing ``str`` (e.g. a
    caller's misregistered serialiser) is also caught with the same
    actionable diagnostic, rather than escaping past the adapter site
    into the wire encoder.
    """

    class _NeedsAdapter:
        pass

    dqlitedbapi.register_adapter(_NeedsAdapter, lambda _v: "leading\x00trailing")
    try:
        with pytest.raises(DataError, match="embedded NUL"):
            _convert_bind_param(_NeedsAdapter())
    finally:
        dqlitedbapi.unregister_adapter(_NeedsAdapter)
