"""Pin: README claims about ``isolation_level`` presence and ``register_adapter``
implementation status stay accurate (both have drifted from the README before)."""

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


def test_isolation_level_setter_accepts_stdlib_pre_3_12_set() -> None:
    """Setter accepts the stdlib pre-3.12 set as no-ops; rejects others with
    ProgrammingError. Enables ``dst.isolation_level = src.isolation_level`` from stdlib."""
    conn = dqlitedbapi.connect("localhost:9001")
    for ok in (None, "", "DEFERRED", "IMMEDIATE", "EXCLUSIVE"):
        conn.isolation_level = ok  # accepted no-op
    for bad in ("SERIALIZABLE", "AUTOCOMMIT", "foo"):
        with pytest.raises(dqlitedbapi.ProgrammingError):
            conn.isolation_level = bad


def test_register_adapter_is_implemented() -> None:
    """register_adapter / unregister_adapter work, not raise NotSupportedError."""
    sentinel = object()
    dqlitedbapi.register_adapter(type(sentinel), str)
    dqlitedbapi.unregister_adapter(type(sentinel))


def test_register_converter_remains_a_stub() -> None:
    """register_converter is still a stub (README stub list); raises NotSupportedError."""
    with pytest.raises(dqlitedbapi.NotSupportedError):
        dqlitedbapi.register_converter("SOMETYPE", lambda x: x)


def test_complete_statement_remains_a_stub() -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        dqlitedbapi.complete_statement("SELECT 1")


def test_enable_callback_tracebacks_remains_a_stub() -> None:
    with pytest.raises(dqlitedbapi.NotSupportedError):
        dqlitedbapi.enable_callback_tracebacks(True)
