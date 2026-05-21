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


def test_isolation_level_setter_accepts_stdlib_pre_3_12_set() -> None:
    """The setter accepts the stdlib pre-3.12 accept-set
    (``{None, "", "DEFERRED", "IMMEDIATE", "EXCLUSIVE"}``) as no-ops
    and rejects everything else with ``ProgrammingError`` (PEP 249 §7
    caller-shape misuse). The previous behaviour rejected ``""`` and
    the three named values with ``NotSupportedError``, breaking the
    canonical cross-driver ``dst.isolation_level = src.isolation_level``
    idiom against a stdlib source connection (whose default is
    ``""``).
    """
    conn = dqlitedbapi.connect("localhost:9001")
    for ok in (None, "", "DEFERRED", "IMMEDIATE", "EXCLUSIVE"):
        conn.isolation_level = ok  # accepted no-op
    for bad in ("SERIALIZABLE", "AUTOCOMMIT", "foo"):
        with pytest.raises(dqlitedbapi.ProgrammingError):
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
