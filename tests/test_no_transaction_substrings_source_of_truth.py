"""Pin: the dbapi's ``_is_no_transaction_error`` recogniser shares
its substring list with ``dqlitewire.NO_TRANSACTION_MESSAGE_SUBSTRINGS``.

Mirrors the client-layer pin in
``test_no_transaction_substrings_source_of_truth.py`` so both
layers cannot drift apart on the substring list. The wire-level
constant is the single source of truth.
"""

from __future__ import annotations

from dqlitedbapi.connection import _NO_TX_SUBSTRINGS, _is_no_transaction_error
from dqlitewire import NO_TRANSACTION_MESSAGE_SUBSTRINGS


def test_dbapi_substring_tuple_is_wire_layer_object() -> None:
    """Identity (`is`), not equality, proves the tuple objects are
    the same — a future maintainer who copies the literal would
    break this pin."""
    assert _NO_TX_SUBSTRINGS is NO_TRANSACTION_MESSAGE_SUBSTRINGS


def test_recogniser_uses_wire_layer_substrings() -> None:
    from dqlitedbapi import OperationalError

    for substr in NO_TRANSACTION_MESSAGE_SUBSTRINGS:
        exc = OperationalError(f"prefix {substr} suffix", code=1)
        assert _is_no_transaction_error(exc), (
            f"dbapi recogniser must accept the substring {substr!r} "
            "from dqlitewire.NO_TRANSACTION_MESSAGE_SUBSTRINGS"
        )
