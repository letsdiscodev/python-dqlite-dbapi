"""Pin: ``ROWID.values`` overlaps ``NUMBER.values`` on
``ValueType.INTEGER`` by design.

PEP 249 §3 does not outlaw overlap between Type Objects, but the
dqlite dbapi advertises both sentinels for INTEGER columns because
the wire protocol carries no "this column is a rowid alias" hint.
Cross-driver callers iterating type sentinels with the documented
PEP 249 chained-``==`` idiom get BOTH predicates true. This test
pins the deliberate behaviour so a future "disjoin ROWID from
NUMBER" refactor cannot land silently.

Cross-driver context (informational):

- psycopg2 / psycopg3: ROWID is disjoint from NUMBER on Postgres.
- stdlib sqlite3: does not export ROWID at all.

Both treat the dqlite overlap as a cross-driver portability caveat;
the docstring on ``ROWID`` in ``types.py`` documents the rationale.
"""

from dqlitedbapi.types import NUMBER, ROWID
from dqlitewire.constants import ValueType


def test_rowid_and_number_both_match_integer_column() -> None:
    type_code = int(ValueType.INTEGER)
    assert type_code == NUMBER
    assert type_code == ROWID
    assert NUMBER != ROWID  # the sentinels themselves remain distinct objects


def test_rowid_does_not_match_float_or_text() -> None:
    # ROWID is INTEGER-specific; the overlap is on INTEGER only.
    assert int(ValueType.FLOAT) != ROWID
    assert int(ValueType.TEXT) != ROWID


def test_number_matches_float_and_boolean_rowid_does_not() -> None:
    # NUMBER spans INTEGER + FLOAT + BOOLEAN. ROWID is INTEGER-only.
    assert int(ValueType.FLOAT) == NUMBER
    assert int(ValueType.BOOLEAN) == NUMBER
    assert int(ValueType.FLOAT) != ROWID
    assert int(ValueType.BOOLEAN) != ROWID
