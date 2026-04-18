"""PEP 249 qmark parameter validation tests (ISSUE-07).

PEP 249: for ``qmark`` paramstyle, "the sequence is mandatory and the
driver will not accept mappings." We additionally reject ``set``/
``frozenset`` because they are unordered and would silently scramble
positional bindings.
"""

import pytest

from dqlitedbapi.cursor import _reject_non_sequence_params
from dqlitedbapi.exceptions import ProgrammingError


class TestRejectMappings:
    def test_dict_rejected(self) -> None:
        with pytest.raises(ProgrammingError, match="mapping"):
            _reject_non_sequence_params({"x": 1})

    def test_ordered_dict_rejected(self) -> None:
        from collections import OrderedDict

        with pytest.raises(ProgrammingError, match="mapping"):
            _reject_non_sequence_params(OrderedDict(a=1))


class TestRejectUnorderedSequences:
    def test_set_rejected(self) -> None:
        with pytest.raises(ProgrammingError, match="set"):
            _reject_non_sequence_params({1, 2, 3})

    def test_frozenset_rejected(self) -> None:
        with pytest.raises(ProgrammingError, match="set"):
            _reject_non_sequence_params(frozenset({1, 2, 3}))


class TestAccept:
    def test_list_accepted(self) -> None:
        _reject_non_sequence_params([1, 2, 3])  # no raise

    def test_tuple_accepted(self) -> None:
        _reject_non_sequence_params((1, 2, 3))  # no raise

    def test_none_accepted(self) -> None:
        _reject_non_sequence_params(None)  # no raise

    def test_empty_list_accepted(self) -> None:
        _reject_non_sequence_params([])  # no raise
