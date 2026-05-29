"""_convert_bind_param's DataError distinguishes "no adapter for this type" from "a registered
adapter returned a non-primitive"; only the message text differs, the class stays DataError."""

from __future__ import annotations

from decimal import Decimal

import pytest

from dqlitedbapi import DataError, register_adapter, unregister_adapter
from dqlitedbapi.types import _convert_bind_param


def test_unsupported_type_with_no_adapter_says_not_supported() -> None:
    with pytest.raises(DataError) as exc:
        _convert_bind_param(Decimal("1.5"))
    msg = str(exc.value)
    assert "is not supported" in msg
    # Must NOT falsely blame an adapter that never ran.
    assert "adapter for" not in msg
    assert "produced" not in msg


def test_registered_adapter_returning_non_primitive_blames_the_adapter() -> None:
    class _Custom:
        pass

    register_adapter(_Custom, lambda _v: ["not", "primitive"])
    try:
        with pytest.raises(DataError) as exc:
            _convert_bind_param(_Custom())
    finally:
        unregister_adapter(_Custom)
    msg = str(exc.value)
    assert "adapter for _Custom produced" in msg
    assert "non-primitive list" in msg
