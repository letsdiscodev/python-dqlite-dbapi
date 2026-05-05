"""``_DBAPIType`` objects are deliberately unhashable.

PEP 249 type objects (``STRING``, ``BINARY``, ``NUMBER``, ``DATETIME``,
``ROWID``) wrap a set of accepted values that can include multiple
wire-level ``ValueType`` codes. Any hash function that hashed to a
single canonical int would silently violate the Python hash-eq
invariant: ``NUMBER == FLOAT_CODE`` would be True while
``hash(NUMBER) != hash(FLOAT_CODE)``, so ``{NUMBER: x}[FLOAT_CODE]``
would raise ``KeyError`` despite equality holding.

The objects raise ``TypeError: unhashable type`` on any attempt to
hash them, which is noisier and therefore safer than a silent
dispatch miss. Callers should use linear equality (``desc[i][1] ==
NUMBER``) against the module-level type objects, not use them as dict
keys or ``set`` members.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.types import BINARY, DATETIME, NUMBER, ROWID, STRING


class TestDbapiTypesUnhashable:
    """PEP 249 type objects must refuse hashing."""

    @pytest.mark.parametrize("obj", [STRING, BINARY, NUMBER, DATETIME, ROWID])
    def test_not_hashable(self, obj: object) -> None:
        with pytest.raises(TypeError, match="unhashable"):
            hash(obj)

    def test_cannot_be_set_members(self) -> None:
        with pytest.raises(TypeError, match="unhashable"):
            set([NUMBER, STRING])  # noqa: C405 -- literal triggers B018

    def test_cannot_be_dict_keys(self) -> None:
        with pytest.raises(TypeError, match="unhashable"):
            dict([(NUMBER, "x")])  # noqa: C406 -- literal triggers B018


class TestDocumentedIdiom:
    """Pin the chained-equality idiom documented in the class
    docstring and README. Without this pin a future refactor that
    implements ``__hash__ = lambda self: id(self)`` (which would
    re-enable ``in {...}`` syntactically) would silently break
    multi-value sentinel dispatch — set membership would fall back
    to identity, missing every legitimate ``ValueType`` int.
    """

    def test_type_in_set_raises_typeerror(self) -> None:
        # The shape that PEP 249 callers might naively try first.
        with pytest.raises(TypeError, match="unhashable"):
            _ = STRING in {STRING, NUMBER}

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
