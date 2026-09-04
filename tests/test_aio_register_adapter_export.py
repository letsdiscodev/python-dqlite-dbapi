"""The async surface re-exports register_adapter; both namespaces share one registry."""


def test_aio_exports_register_adapter() -> None:
    from dqlitedbapi.aio import register_adapter

    assert callable(register_adapter)


def test_aio_register_adapter_is_in_all() -> None:
    import dqlitedbapi.aio

    assert "register_adapter" in dqlitedbapi.aio.__all__


def test_aio_register_adapter_shares_registry_with_sync() -> None:
    """Mutating via the async namespace affects the sync namespace (shared registry)."""
    import dqlitedbapi
    import dqlitedbapi.aio

    class _Probe:
        pass

    def _adapter(_: _Probe) -> str:
        return "probe"

    dqlitedbapi.aio.register_adapter(_Probe, _adapter)

    assert dqlitedbapi.aio.register_adapter is dqlitedbapi.register_adapter

    from dqlitedbapi.types import _ADAPTERS

    assert _Probe in _ADAPTERS

    del _ADAPTERS[_Probe]  # avoid leaking global state to sibling tests
