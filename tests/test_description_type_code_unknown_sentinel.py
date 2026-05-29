"""An unresolvable column type yields the ``UNKNOWN`` Type Object sentinel, not
``None`` (which would violate PEP 249 §6.1.2's "must compare equal to a Type Object").
"""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import BINARY, DATETIME, NUMBER, ROWID, STRING, UNKNOWN


def test_unknown_exported_at_top_level() -> None:
    assert hasattr(dqlitedbapi, "UNKNOWN")
    assert "UNKNOWN" in dqlitedbapi.__all__


def test_unknown_compares_unequal_to_real_type_objects() -> None:
    assert (UNKNOWN == STRING) is False
    assert (UNKNOWN == NUMBER) is False
    assert (UNKNOWN == BINARY) is False
    assert (UNKNOWN == DATETIME) is False
    assert (UNKNOWN == ROWID) is False


def test_unknown_equality_against_arbitrary_int_returns_false_not_typeerror() -> None:
    """The idiom ``type_code == STRING`` must not raise TypeError when UNKNOWN."""
    type_code = UNKNOWN
    result = type_code == STRING
    assert result is False
    result2 = type_code == 999
    assert result2 is False


def test_unknown_self_equality() -> None:
    assert UNKNOWN == UNKNOWN
    from dqlitedbapi.types import _DBAPIType

    fresh = _DBAPIType(_name="UNKNOWN")
    assert fresh == UNKNOWN


def test_unknown_repr_is_named() -> None:
    assert repr(UNKNOWN) == "UNKNOWN"
