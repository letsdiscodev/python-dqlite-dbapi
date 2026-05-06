"""Pin: ``Cursor.execute(sql, gen)`` (where ``gen`` is a generator
or non-sized iterable) raises ``ProgrammingError`` rather than
silently materialising the generator.

Stdlib ``sqlite3`` rejects generator parameters with
``ProgrammingError("parameters are of unsupported type")``; the
substantive correctness reason for matching that posture is the
leader-flip retry path. ``ClusterClient.connect()``'s retry-with-
backoff may re-issue the same statement after a leader flip, but
a single-pass iterable can be drained only once. The previous
behaviour (silent materialise via ``_convert_params``) masked the
constraint and made the failure mode (placeholder-count mismatch
buried in the wire layer) much further from the cause.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _reject_non_sequence_params
from dqlitedbapi.exceptions import Error, ProgrammingError


def _gen() -> object:
    """Return a fresh generator that yields one int."""
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
    """Sanity: legitimate sequence shapes must NOT raise."""
    _reject_non_sequence_params((1, 2))
    _reject_non_sequence_params([1, 2])
    _reject_non_sequence_params(())
    _reject_non_sequence_params([])
    # ``None`` is the legitimate "no params" form.
    _reject_non_sequence_params(None)
