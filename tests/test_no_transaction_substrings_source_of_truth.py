"""Pin: dbapi reuses ``dqlitewire.NO_TRANSACTION_MESSAGE_SUBSTRINGS`` (single source)."""

from __future__ import annotations

from dqlitedbapi.connection import _NO_TX_SUBSTRINGS, _is_no_transaction_error
from dqlitewire import NO_TRANSACTION_MESSAGE_SUBSTRINGS


def test_dbapi_substring_tuple_is_wire_layer_object() -> None:
    """Identity (not equality) proves the tuple is shared, not a copied literal."""
    assert _NO_TX_SUBSTRINGS is NO_TRANSACTION_MESSAGE_SUBSTRINGS


def test_recogniser_uses_wire_layer_substrings() -> None:
    from dqlitedbapi import OperationalError

    for substr in NO_TRANSACTION_MESSAGE_SUBSTRINGS:
        exc = OperationalError(f"prefix {substr} suffix", code=1)
        assert _is_no_transaction_error(exc), (
            f"dbapi recogniser must accept the substring {substr!r} "
            "from dqlitewire.NO_TRANSACTION_MESSAGE_SUBSTRINGS"
        )
