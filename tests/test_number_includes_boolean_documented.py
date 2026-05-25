"""Pin: ``NUMBER`` Type Object includes ``ValueType.BOOLEAN`` (a
deliberate SQLite-storage-affinity choice that diverges from
psycopg2's disjoint BOOL/NUMBER design). The divergence is
documented inline in ``types.py``; cross-driver porters need the
NOTE block to understand the dispatch semantic.
"""

from __future__ import annotations

import inspect

from dqlitedbapi import NUMBER
from dqlitewire import ValueType


def test_number_compares_equal_to_boolean_wire_value() -> None:
    """Sanity-pin the existing behaviour the comment defends."""
    assert NUMBER == ValueType.BOOLEAN


def test_number_definition_carries_boolean_overlap_note() -> None:
    """The types.py source must carry the NOTE block explaining
    the BOOLEAN-in-NUMBER overlap as a documented cross-driver
    divergence from psycopg2.
    """
    import dqlitedbapi.types as types_mod

    source = inspect.getsource(types_mod)
    # The NOTE block is anchored on these phrases.
    assert "NUMBER.values" in source
    assert "BOOLEAN" in source
    assert "psycopg2" in source.lower() or "psycopg" in source.lower()
    # The rationale must mention SQLite's storage discipline.
    assert "INTEGER affinity" in source or "loose typing" in source
