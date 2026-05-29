"""Pin: ``register_adapter``/``unregister_adapter`` are positional-only, matching
stdlib's C signature (keyword args would be a silent cross-driver portability trap)."""

from __future__ import annotations

import pytest

import dqlitedbapi


def test_register_adapter_rejects_keyword_args() -> None:
    with pytest.raises(TypeError):
        dqlitedbapi.register_adapter(type_=int, adapter=str)  # type: ignore[call-arg]


def test_unregister_adapter_rejects_keyword_args() -> None:
    with pytest.raises(TypeError):
        dqlitedbapi.unregister_adapter(type_=int)  # type: ignore[call-arg]


def test_register_adapter_positional_still_works() -> None:
    """Regression: the canonical positional form remains supported."""

    class _Marker:
        pass

    dqlitedbapi.register_adapter(_Marker, lambda v: "x")
    try:
        from dqlitedbapi.types import _ADAPTERS

        assert _Marker in _ADAPTERS
    finally:
        dqlitedbapi.unregister_adapter(_Marker)
