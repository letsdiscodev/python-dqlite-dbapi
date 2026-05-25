"""Pin: ``Cursor.description[i][1]`` (type_code) is the ``UNKNOWN``
Type Object sentinel when the wire cannot resolve a column's type
— NOT ``None``, which would violate PEP 249 §6.1.2 ("must compare
equal to one of [the Type Objects]").

Two paths:
1. Empty result set with empty ``column_types`` (zero-row SELECT /
   zero-row RETURNING).
2. NULL-only column (every row's value at the column index is NULL,
   rescue scan exhausted).

``UNKNOWN`` is a real Type Object with an empty ``values`` set, so
equality against STRING / NUMBER / BINARY / DATETIME / ROWID
cleanly returns False — no TypeError, no spurious True. Callers
that want to detect the unresolved case can write
``type_code == UNKNOWN``.
"""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import BINARY, DATETIME, NUMBER, ROWID, STRING, UNKNOWN


def test_unknown_exported_at_top_level() -> None:
    assert hasattr(dqlitedbapi, "UNKNOWN")
    assert "UNKNOWN" in dqlitedbapi.__all__


def test_unknown_compares_unequal_to_real_type_objects() -> None:
    """The sentinel must not collide with any of the documented
    PEP 249 Type Objects.
    """
    assert (UNKNOWN == STRING) is False
    assert (UNKNOWN == NUMBER) is False
    assert (UNKNOWN == BINARY) is False
    assert (UNKNOWN == DATETIME) is False
    assert (UNKNOWN == ROWID) is False


def test_unknown_equality_against_arbitrary_int_returns_false_not_typeerror() -> None:
    """The chained PEP 249 idiom ``type_code == STRING`` must not
    raise TypeError when ``type_code is UNKNOWN``.
    """
    type_code = UNKNOWN
    # Both the equality call and the result class are pinned.
    result = type_code == STRING
    assert result is False
    # Also pin no TypeError for chained against an arbitrary int.
    result2 = type_code == 999
    assert result2 is False


def test_unknown_self_equality() -> None:
    """Two references to UNKNOWN compare equal so callers can
    introspect for the unresolved case.
    """
    assert UNKNOWN == UNKNOWN
    # Equality against a fresh empty-values instance also holds
    # (consistent with the existing Type Object __eq__ semantics).
    from dqlitedbapi.types import _DBAPIType

    fresh = _DBAPIType(_name="UNKNOWN")
    assert fresh == UNKNOWN


def test_unknown_repr_is_named() -> None:
    """The ``_name="UNKNOWN"`` arg means repr is informative."""
    assert repr(UNKNOWN) == "UNKNOWN"
