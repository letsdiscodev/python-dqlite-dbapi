"""``_DBAPIType`` objects are hashable by their identifier name.

PEP 249 type objects (``STRING``, ``BINARY``, ``NUMBER``, ``DATETIME``,
``ROWID``, ``UNKNOWN``) hash on their canonical class-level ``_name``
so they can be used as dict keys / set members. SQLAlchemy's
dialect-level type-memo dict keys ``cursor.description``'s
``type_code``, which can be ``UNKNOWN`` (a ``_DBAPIType`` instance)
when the wire layer cannot resolve a column's type, so the type
objects MUST be hashable.

The hash-eq invariant with multi-value ``__eq__`` against ``int`` /
``ValueType`` / ``str`` is intentionally relaxed: those comparands
hash to different values than the type object, so cross-type
``{NUMBER: x}[FLOAT_CODE]`` returns ``KeyError`` despite the
equality holding. Callers should use linear equality
(``desc[i][1] == NUMBER``) against the module-level type objects,
not use them as conflated hash-eq keys against bare wire ints.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.types import BINARY, DATETIME, NUMBER, ROWID, STRING


class TestDbapiTypesHashable:
    """PEP 249 type objects must hash so SA can memo by type_code."""

    @pytest.mark.parametrize("obj", [STRING, BINARY, NUMBER, DATETIME, ROWID])
    def test_hashable(self, obj: object) -> None:
        # Must not raise; the actual value is unspecified beyond
        # being a stable int.
        h1 = hash(obj)
        h2 = hash(obj)
        assert h1 == h2

    def test_can_be_set_members(self) -> None:
        s = {NUMBER, STRING}
        assert NUMBER in s
        assert STRING in s

    def test_can_be_dict_keys(self) -> None:
        d = {NUMBER: "n", STRING: "s"}
        assert d[NUMBER] == "n"
        assert d[STRING] == "s"

    def test_distinct_singletons_hash_to_different_values(self) -> None:
        # _name-based hashing keeps STRING / NUMBER / ROWID / etc.
        # distinct, so they live in separate hash buckets and SA's
        # memo dict can store entries for each without collision.
        assert hash(STRING) != hash(NUMBER)
        assert hash(NUMBER) != hash(ROWID)
        assert hash(BINARY) != hash(DATETIME)


class TestHashEqInvariantRelaxation:
    """The hash-eq invariant against multi-value ``__eq__`` with
    bare wire ints is intentionally relaxed.

    PEP 249 callers should use linear equality
    (``desc[i][1] == STRING``) against the module-level type
    objects, NOT cross-type set membership against bare wire ints.
    """

    def test_chained_equality_is_the_documented_idiom(self) -> None:
        from dqlitewire.constants import ValueType

        # Wire type code as seen in description[i][1].
        type_code = int(ValueType.TEXT)
        assert type_code == STRING or type_code == NUMBER  # noqa: PLR1714

        type_code = int(ValueType.INTEGER)
        assert type_code == STRING or type_code == NUMBER  # noqa: PLR1714


class TestDbapiTypesDistinct:
    """Distinct _DBAPIType instances must not collide under eq."""

    def test_string_not_equal_to_number(self) -> None:
        assert STRING != NUMBER

    def test_binary_not_equal_to_string(self) -> None:
        assert BINARY != STRING

    def test_identity_self_equal(self) -> None:
        assert STRING == STRING  # noqa: PLR0124 -- intentional self-eq
