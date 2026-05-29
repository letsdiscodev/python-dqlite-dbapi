"""``ROWID`` overlaps ``NUMBER`` on ``ValueType.INTEGER`` by design: the wire
protocol carries no rowid-alias hint, so both sentinels match INTEGER."""

from dqlitedbapi.types import NUMBER, ROWID
from dqlitewire.constants import ValueType


def test_rowid_and_number_both_match_integer_column() -> None:
    type_code = int(ValueType.INTEGER)
    assert type_code == NUMBER
    assert type_code == ROWID
    assert NUMBER != ROWID  # sentinels remain distinct objects despite overlap


def test_rowid_does_not_match_float_or_text() -> None:
    assert int(ValueType.FLOAT) != ROWID
    assert int(ValueType.TEXT) != ROWID


def test_number_matches_float_and_boolean_rowid_does_not() -> None:
    # NUMBER spans INTEGER + FLOAT + BOOLEAN; ROWID is INTEGER-only.
    assert int(ValueType.FLOAT) == NUMBER
    assert int(ValueType.BOOLEAN) == NUMBER
    assert int(ValueType.FLOAT) != ROWID
    assert int(ValueType.BOOLEAN) != ROWID
