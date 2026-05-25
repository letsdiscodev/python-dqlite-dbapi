"""PEP 249 type-object comparison tests.

Per PEP 249 the module's type objects (STRING, BINARY, NUMBER, DATETIME,
ROWID) must compare equal to whatever `cursor.description[i][1]` carries.
We carry the wire `ValueType` integer there, so the type objects have to
compare equal to the integer code too — not just to uppercase SQL type
name strings.
"""

import pytest

from dqlitedbapi import BINARY, DATETIME, NUMBER, ROWID, STRING
from dqlitewire.constants import ValueType


class TestStringType:
    def test_equals_sql_type_names(self) -> None:
        assert STRING == "TEXT"
        assert STRING == "varchar"
        assert STRING == "CLOB"

    def test_equals_wire_text_value_type(self) -> None:
        assert STRING == ValueType.TEXT
        assert int(ValueType.TEXT) == STRING

    def test_does_not_equal_non_text_wire_types(self) -> None:
        assert STRING != ValueType.INTEGER
        assert STRING != ValueType.BLOB
        assert STRING != ValueType.ISO8601


class TestBinaryType:
    def test_equals_blob_wire_type(self) -> None:
        assert BINARY == ValueType.BLOB
        assert int(ValueType.BLOB) == BINARY
        assert BINARY == "BLOB"


class TestNumberType:
    @pytest.mark.parametrize("vt", [ValueType.INTEGER, ValueType.FLOAT, ValueType.BOOLEAN])
    def test_equals_numeric_wire_types(self, vt: ValueType) -> None:
        assert vt == NUMBER
        assert int(vt) == NUMBER

    def test_does_not_equal_text(self) -> None:
        assert NUMBER != ValueType.TEXT


class TestDatetimeType:
    @pytest.mark.parametrize("vt", [ValueType.ISO8601, ValueType.UNIXTIME])
    def test_equals_datetime_wire_types(self, vt: ValueType) -> None:
        assert vt == DATETIME
        assert int(vt) == DATETIME

    def test_equals_declared_type_names(self) -> None:
        assert DATETIME == "DATETIME"
        assert DATETIME == "DATE"
        assert DATETIME == "timestamp"


class TestRowidType:
    def test_equals_integer_wire_type(self) -> None:
        assert ROWID == ValueType.INTEGER
        assert int(ValueType.INTEGER) == ROWID


class TestHashability:
    def test_types_are_hashable_by_name(self) -> None:
        # The PEP 249 type objects hash by their class-level ``_name``
        # so they can be used as dict keys / set members. SQLAlchemy
        # memoises ``cursor.description`` type_codes — which can be
        # the ``UNKNOWN`` _DBAPIType sentinel — in a dialect-level
        # dict, so the type objects MUST be hashable. The hash-eq
        # invariant against multi-value equality with bare wire ints
        # is intentionally relaxed (see TestHashEqInvariantRelaxation
        # in test_dbapi_type_hash_consistency).
        for obj in (STRING, BINARY, NUMBER, DATETIME, ROWID):
            assert isinstance(hash(obj), int)


# Maintenance note: if a future ValueType is intentionally exempt
# (e.g. ``ValueType.RESERVED`` for protocol negotiation), add it to
# ``EXEMPT_VALUE_TYPES`` here — do NOT broaden a DBAPI type-object
# just to silence this test, or you will silently mis-classify a
# column for user code.
EXEMPT_VALUE_TYPES: frozenset[ValueType] = frozenset({ValueType.NULL})

DBAPI_TYPE_OBJECTS = (STRING, BINARY, NUMBER, DATETIME, ROWID)


class TestValueTypeMappingExhaustiveness:
    """Pin the cross-package contract: every wire ``ValueType`` (except
    ``NULL``, which PEP 249 represents as ``None``) is covered by at
    least one module-level DBAPI type object so
    ``cursor.description[i][1] == STRING / BINARY / NUMBER / DATETIME /
    ROWID`` comparisons in user code do not silently fall through to
    "none of the above" for any column the wire can produce.

    A regression would surface if either:
    - a new ValueType is added in wire/constants.py without
      corresponding updates to the dbapi type objects, or
    - an existing ValueType is removed from a type object by an
      over-zealous "cleanup".
    """

    @pytest.mark.parametrize(
        "value_type",
        [v for v in ValueType if v not in EXEMPT_VALUE_TYPES],
        ids=lambda v: f"{v.name}_{int(v)}",
    )
    def test_every_non_exempt_value_type_has_dbapi_type_coverage(
        self, value_type: ValueType
    ) -> None:
        """The PEP 249 comparison surface must cover every wire
        ValueType. ``cursor.description[i][1]`` is a ValueType int;
        user code compares it against STRING / BINARY / NUMBER /
        DATETIME / ROWID. At least one of those must compare equal.
        """
        matched = [t for t in DBAPI_TYPE_OBJECTS if t == value_type]
        assert matched, (
            f"ValueType.{value_type.name} ({int(value_type)}) is not covered "
            f"by any DBAPI type object. Add it to the appropriate "
            f"_DBAPIType(...) call in dqlitedbapi/types.py per the PEP 249 "
            f"§6.1.2 type_code contract."
        )

    def test_null_value_type_is_intentionally_exempt(self) -> None:
        """PEP 249 specifies SQL NULL is the Python ``None`` singleton;
        the DBAPI type-object surface intentionally has no entry for
        NULL. A regression that adds NULL to one of the type objects
        would imply the contract changed; this test makes that change
        explicit so it cannot land silently.
        """
        for t in DBAPI_TYPE_OBJECTS:
            assert t != ValueType.NULL, (
                f"DBAPI type object {t!r} unexpectedly compares equal to "
                f"ValueType.NULL — NULL has no DBAPI type-object per PEP 249."
            )


class TestDBAPITypeEqFallthrough:
    """Pin the ``NotImplemented`` fallthrough and ``bool`` guard on
    ``_DBAPIType.__eq__``.

    ``__eq__`` returns ``NotImplemented`` (not ``False``) for any
    ``other`` that is not a ``str``, ``ValueType``, or non-bool ``int``.
    Per the Python data model, this lets the reflected comparison be
    consulted before Python falls back to identity (``False`` for
    distinct objects). A refactor to ``return False`` would silently
    break reflected ``__eq__`` against any future sibling class and
    drop the intended fallthrough semantics.

    The ``not isinstance(other, bool)`` guard is the stronger half:
    ``bool`` is a subclass of ``int``, so without the guard
    ``NUMBER == True`` would return True (``True in {1, 2, …}`` is
    True), silently violating the PEP 249 type-object-identity
    contract.
    """

    @pytest.mark.parametrize("other", [None, [], {}, (), object(), 1.5, {1, 2}])
    @pytest.mark.parametrize("type_obj", DBAPI_TYPE_OBJECTS)
    def test_not_equal_to_unrelated_types(self, type_obj: object, other: object) -> None:
        # ``__eq__`` must return ``NotImplemented`` for unrelated types
        # so Python consults the reflected comparison, then falls back
        # to identity. For every ``other`` in the parametrize list,
        # ``type(other).__eq__`` also returns ``NotImplemented`` against
        # ``_DBAPIType``, so the reflected comparison works and identity
        # resolves to False.
        assert type_obj != other
        assert other != type_obj

    @pytest.mark.parametrize("type_obj", DBAPI_TYPE_OBJECTS)
    @pytest.mark.parametrize("value", [True, False])
    def test_not_equal_to_bool_even_if_integer_match(self, type_obj: object, value: bool) -> None:
        # bool is a subclass of int; a refactor that dropped the
        # ``not isinstance(other, bool)`` guard would silently let
        # ``NUMBER == True`` return True. Pin both directions so a
        # regression cannot land without a test failure.
        assert type_obj != value
        assert value != type_obj

    def test_types_are_hashable_by_singleton_name(self) -> None:
        # ``_DBAPIType`` hashes on the singleton's class-level
        # ``_name`` so it can be used as a dict key / set member.
        # The hash-eq invariant against multi-value equality with
        # bare wire ints is intentionally relaxed: ``NUMBER ==
        # FLOAT_CODE`` is True, but ``{NUMBER: x}[FLOAT_CODE]``
        # returns ``KeyError`` because the hash codes differ. The
        # cross-driver idiom for testing ``description[i][1]`` is
        # linear ``== STRING`` etc., not hash lookup.
        assert isinstance(hash(STRING), int)
        assert isinstance(hash(NUMBER), int)
        assert hash(STRING) != hash(NUMBER)


class TestDBAPITypeAcceptRejectMatrix:
    """Pin the explicit accept / reject matrix for each PEP 249 type
    singleton.

    The case-insensitive name match is intentional and supports
    operators that mirror SQLite's permissive declared-type strings.
    What we do NOT want is for typos (``"DATETIEM"``, ``"BLOBS"``) to
    be silently equal to anything — both halves should be pinned so a
    future cleanup that tightens the comparator doesn't accidentally
    break user code that relied on case-insensitive matching."""

    @pytest.mark.parametrize(
        ("type_obj", "accepts", "rejects"),
        [
            (
                DATETIME,
                [
                    "DATETIME",
                    "datetime",
                    "DateTime",
                    "DATE",
                    "TIME",
                    "TIMESTAMP",
                    ValueType.ISO8601,
                    ValueType.UNIXTIME,
                    int(ValueType.ISO8601),
                ],
                ["", "DATETIEM", "datetimes", "TIMESTAMPS", 99, "INTEGER"],
            ),
            (
                STRING,
                ["TEXT", "varchar", "CHAR", "CLOB", ValueType.TEXT],
                ["", "TXT", "STRINGS", 99, "INTEGER"],
            ),
            (
                BINARY,
                ["BLOB", "blob", "BINARY", "VARBINARY", ValueType.BLOB],
                ["", "BIN", "BLOBS", 99],
            ),
            (
                NUMBER,
                [
                    "INTEGER",
                    "INT",
                    "REAL",
                    "FLOAT",
                    ValueType.INTEGER,
                    ValueType.FLOAT,
                    ValueType.BOOLEAN,
                ],
                ["", "NUMERICS", "INTGER", 99],
            ),
            (
                ROWID,
                ["ROWID", "rowid", "INTEGER PRIMARY KEY", ValueType.INTEGER],
                ["", "ROWIDS", 99],
            ),
        ],
    )
    def test_dbapi_type_singleton_accept_reject_matrix(
        self,
        type_obj: object,
        accepts: list[object],
        rejects: list[object],
    ) -> None:
        for v in accepts:
            assert type_obj == v, f"{type_obj!r} should accept {v!r} but does not"
        for v in rejects:
            assert type_obj != v, f"{type_obj!r} should reject {v!r} but accepted it"
