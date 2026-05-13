"""Pin: ``_classify_caller_sql`` rejects structural-type outer params
(str / bytes / bytearray / memoryview / Mapping / set / frozenset)
BEFORE the placeholder-count check, so the diagnostic blames the
caller for the right reason — "you passed a string where a sequence
was expected" — rather than reporting a misleading character count.

Without the precedence:

- ``cur.execute("SELECT ?", "abc")``: ``len("abc") == 3``,
  ``placeholder_count == 1``, classifier raised
  "Incorrect number of bindings supplied. ... uses 1, and there are
  3 supplied." The "3" is the string length, not "you passed 3
  params" — wrong diagnostic.
- ``cur.execute("SELECT ?, ?, ?", "abc")``: ``len("abc") == 3 ==
  placeholder_count``, classifier passed, bind-layer rejected ``str``
  later with a different message. Cross-driver porting saw two
  different error classes for what is structurally the same bug.

The structural reject (``_reject_non_sequence_params``) used to run
in ``_convert_bind_params`` AFTER the count check fired. Hoisting it
into ``_classify_caller_sql`` covers both ``execute`` and the
``executemany`` per-iteration twin via one code path.
"""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _classify_caller_sql
from dqlitedbapi.exceptions import ProgrammingError


@pytest.mark.parametrize(
    "params",
    [
        "abc",
        b"abc",
        bytearray(b"abc"),
        memoryview(b"abc"),
    ],
)
def test_classify_caller_sql_rejects_string_like_outer_param(params: object) -> None:
    """The diagnostic must be the structural ``parameters must be a
    sequence of values`` shape, NOT the count-mismatch shape — the
    string is rejected BEFORE the placeholder-count check fires."""
    with pytest.raises(ProgrammingError, match="parameters must be a sequence of values"):
        _classify_caller_sql("SELECT ?, ?, ?", params)  # type: ignore[arg-type]


def test_classify_caller_sql_rejects_string_when_length_matches_placeholders() -> None:
    """The smoking-gun shape: ``len("abc") == 3 == placeholder_count``
    used to silently pass the count check and route to the bind-layer
    rejection. Pin that the structural reject now fires first."""
    with pytest.raises(ProgrammingError, match="parameters must be a sequence of values"):
        _classify_caller_sql("SELECT ?, ?, ?", "abc")


def test_classify_caller_sql_rejects_string_when_length_mismatches() -> None:
    """The other smoking-gun shape: ``cur.execute("SELECT ?",
    "abc")`` used to report ``"uses 1, and there are 3 supplied"`` —
    a misleading count message that blames the caller for the wrong
    reason. Pin that the structural-reject diagnostic fires here too,
    NOT the count-mismatch diagnostic."""
    with pytest.raises(ProgrammingError) as exc_info:
        _classify_caller_sql("SELECT ?", "abc")
    assert "parameters must be a sequence of values" in str(exc_info.value)
    assert "Incorrect number of bindings supplied" not in str(exc_info.value)


def test_classify_caller_sql_rejects_mapping_outer_param() -> None:
    with pytest.raises(ProgrammingError, match="qmark paramstyle requires a sequence"):
        _classify_caller_sql("SELECT ?", {"key": "value"})  # type: ignore[arg-type]


def test_classify_caller_sql_rejects_set_outer_param() -> None:
    with pytest.raises(ProgrammingError, match="qmark paramstyle requires an ordered sequence"):
        _classify_caller_sql("SELECT ?", {1})  # type: ignore[arg-type]


def test_classify_caller_sql_accepts_tuple_outer_param() -> None:
    """Negative pin: a legitimate tuple passes through with no
    rejection (length-1 tuple against 1 placeholder)."""
    _classify_caller_sql("SELECT ?", ("abc",))


def test_classify_caller_sql_count_mismatch_diagnostic_still_fires_for_real_sequence() -> None:
    """The classifier's pre-existing count-mismatch diagnostic still
    fires for real sequences with the wrong arity — structural reject
    does NOT swallow this case."""
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings supplied"):
        _classify_caller_sql("SELECT ?", (1, 2, 3))


def test_classify_caller_sql_executemany_skip_param_count_does_not_reject_structural() -> None:
    """The executemany pre-flight uses ``skip_param_count_check=True``
    with ``parameters=None``; structural reject must NOT fire when no
    params are passed (the per-iteration check handles each row)."""
    # No exception — this is the hoist call shape used at the top of
    # ``_executemany_async``.
    _classify_caller_sql("SELECT ?", None, skip_param_count_check=True)
