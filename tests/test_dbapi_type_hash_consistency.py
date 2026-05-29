"""``_DBAPIType`` objects hash on their ``_name`` so SQLAlchemy can memo
description ``type_code`` (incl. ``UNKNOWN``) by key. The hash-eq invariant
against bare wire ints is intentionally relaxed: use ``==``, not membership."""

from __future__ import annotations

import pytest

from dqlitedbapi.types import BINARY, DATETIME, NUMBER, ROWID, STRING


class TestDbapiTypesHashable:
    """PEP 249 type objects must hash so SA can memo by type_code."""

    @pytest.mark.parametrize("obj", [STRING, BINARY, NUMBER, DATETIME, ROWID])
    def test_hashable(self, obj: object) -> None:
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
        assert hash(STRING) != hash(NUMBER)
        assert hash(NUMBER) != hash(ROWID)
        assert hash(BINARY) != hash(DATETIME)


class TestHashEqInvariantRelaxation:
    """Use ``==`` against module-level type objects, not set membership
    against bare wire ints (the hash-eq invariant is relaxed there)."""

    def test_chained_equality_is_the_documented_idiom(self) -> None:
        from dqlitewire.constants import ValueType

        type_code = int(ValueType.TEXT)
        assert type_code == STRING or type_code == NUMBER  # noqa: PLR1714

        type_code = int(ValueType.INTEGER)
        assert type_code == STRING or type_code == NUMBER  # noqa: PLR1714

    def test_bare_int_set_membership_silently_misses_does_not_raise(self) -> None:
        # Bare-int membership silently misses (hash lookup never reaches
        # __eq__) even though ``==`` against a set member holds.
        from dqlitewire.constants import ValueType

        integer_code = int(ValueType.INTEGER)
        assert integer_code == NUMBER
        assert (integer_code in {NUMBER}) is False
        assert (integer_code in {STRING, NUMBER}) is False
        assert NUMBER in {STRING, NUMBER}


class TestDbapiTypesDistinct:
    """Distinct _DBAPIType instances must not collide under eq."""

    def test_string_not_equal_to_number(self) -> None:
        assert STRING != NUMBER

    def test_binary_not_equal_to_string(self) -> None:
        assert BINARY != STRING

    def test_identity_self_equal(self) -> None:
        assert STRING == STRING  # noqa: PLR0124 -- intentional self-eq
