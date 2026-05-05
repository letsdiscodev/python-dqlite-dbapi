"""``register_adapter`` is module-global at the dbapi types layer
(see ``types.py``) — the sync surface re-exports it; the async
surface must too. Without it, users learning the API via
``from dqlitedbapi.aio import ...`` cannot find the hook even
though calling it from either namespace mutates the same registry.
"""

def test_aio_exports_register_adapter() -> None:
    from dqlitedbapi.aio import register_adapter

    assert callable(register_adapter)


def test_aio_register_adapter_is_in_all() -> None:
    import dqlitedbapi.aio

    assert "register_adapter" in dqlitedbapi.aio.__all__


def test_aio_register_adapter_shares_registry_with_sync() -> None:
    """Mutating via the async namespace must affect the sync namespace
    too — the underlying registry is module-global at
    ``dqlitedbapi.types``."""
    import dqlitedbapi
    import dqlitedbapi.aio

    class _Probe:
        pass

    def _adapter(_: _Probe) -> str:
        return "probe"

    dqlitedbapi.aio.register_adapter(_Probe, _adapter)

    # Both surfaces reach the same callable.
    assert dqlitedbapi.aio.register_adapter is dqlitedbapi.register_adapter

    # The shared registry honours the registration regardless of
    # which surface set it. We exercise this through the type adapter
    # dispatch indirectly — the public contract is "shared dict".
    from dqlitedbapi.types import _ADAPTERS

    assert _Probe in _ADAPTERS

    # Cleanup so the test does not leak global state to siblings.
    del _ADAPTERS[_Probe]
