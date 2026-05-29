"""Non-sized iterable params raise ``ProgrammingError`` instead of being materialised:
the leader-flip retry path may re-issue a statement, but a generator drains only once."""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _reject_non_sequence_params
from dqlitedbapi.exceptions import Error, ProgrammingError


def _gen() -> object:
    yield 1


@pytest.mark.parametrize(
    "params_factory",
    [
        _gen,
        lambda: iter([1, 2]),
        lambda: filter(None, [1, 2]),
        lambda: map(int, ["1", "2"]),
        lambda: (x for x in [1]),
    ],
)
def test_non_sized_iterables_are_rejected(params_factory: object) -> None:
    params = params_factory()  # type: ignore[operator]
    with pytest.raises(ProgrammingError, match="sized") as ei:
        _reject_non_sequence_params(params)
    assert isinstance(ei.value, Error)


def test_tuple_and_list_still_pass() -> None:
    _reject_non_sequence_params((1, 2))
    _reject_non_sequence_params([1, 2])
    _reject_non_sequence_params(())
    _reject_non_sequence_params([])
    _reject_non_sequence_params(None)  # "no params"
