"""Pin: README claims about ``isolation_level`` attribute presence
and ``register_adapter`` implementation status remain accurate.

The README has historically drifted from implementation:
- A claim that "there is no isolation_level attribute" persisted
  after the property was added.
- ``register_adapter`` was grouped with stub methods despite being
  fully implemented.

These tests fail loudly if a future revert hides the property OR
silently demotes ``register_adapter`` / ``unregister_adapter`` to a
stub.
"""

from __future__ import annotations

import pytest

import dqlitedbapi


def test_isolation_level_attribute_exists_on_sync_connection() -> None:
    conn = dqlitedbapi.connect("localhost:9001")
    assert hasattr(conn, "isolation_level"), (
        "README documents Connection.isolation_level as an attribute "
        "(getter returns None). A revert that hides the property "
        "contradicts the README."
    )
    assert conn.isolation_level is None


def test_isolation_level_setter_accepts_none_only() -> None:
    """README: 'the setter accepts only None (no-op) and rejects every
    other value with NotSupportedError'."""
    conn = dqlitedbapi.connect("localhost:9001")
    conn.isolation_level = None  # accepted, no-op
    for bad in ("", "DEFERRED", "IMMEDIATE", "EXCLUSIVE", "SERIALIZABLE", "AUTOCOMMIT"):
        with pytest.raises(dqlitedbapi.NotSupportedError):
            conn.isolation_level = bad


def test_register_adapter_is_implemented() -> None:
    """README: 'working register_adapter / unregister_adapter
    (process-global)'. Calling must succeed; not raise NotSupportedError."""
    sentinel = object()
    dqlitedbapi.register_adapter(type(sentinel), str)
    dqlitedbapi.unregister_adapter(type(sentinel))


def test_register_converter_remains_a_stub() -> None:
    """The README's stub list still includes register_converter — pin
    that it raises NotSupportedError so a future change that
    silently implements it can be evaluated against the README."""
    with pytest.raises(dqlitedbapi.NotSupportedError):
        dqlitedbapi.register_converter("SOMETYPE", lambda x: x)


def test_complete_statement_remains_a_stub() -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        dqlitedbapi.complete_statement("SELECT 1")


def test_enable_callback_tracebacks_remains_a_stub() -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        dqlitedbapi.enable_callback_tracebacks(True)
