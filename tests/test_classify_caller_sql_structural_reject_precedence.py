"""``_classify_caller_sql`` rejects structural-type outer params (str,
bytes, Mapping, set, ...) BEFORE the placeholder-count check, so the
diagnostic blames a string-where-sequence-expected rather than
reporting a misleading character count (e.g. len("abc") == 3)."""

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
    """String is rejected as structural BEFORE the count-mismatch check."""
    with pytest.raises(ProgrammingError, match="parameters must be a sequence of values"):
        _classify_caller_sql("SELECT ?, ?, ?", params)  # type: ignore[arg-type]


def test_classify_caller_sql_rejects_string_when_length_matches_placeholders() -> None:
    """``len("abc") == 3 == placeholder_count`` used to pass the count
    check; pin that the structural reject now fires first."""
    with pytest.raises(ProgrammingError, match="parameters must be a sequence of values"):
        _classify_caller_sql("SELECT ?, ?, ?", "abc")


def test_classify_caller_sql_rejects_string_when_length_mismatches() -> None:
    """``execute("SELECT ?", "abc")`` used to report a misleading
    ``"uses 1, and there are 3 supplied"``; pin the structural reject."""
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
    """A legitimate tuple passes through with no rejection."""
    _classify_caller_sql("SELECT ?", ("abc",))


def test_classify_caller_sql_count_mismatch_diagnostic_still_fires_for_real_sequence() -> None:
    """Count-mismatch diagnostic still fires for real sequences with
    the wrong arity (structural reject does not swallow this)."""
    with pytest.raises(ProgrammingError, match="Incorrect number of bindings supplied"):
        _classify_caller_sql("SELECT ?", (1, 2, 3))


def test_classify_caller_sql_executemany_skip_param_count_does_not_reject_structural() -> None:
    """With ``skip_param_count_check=True`` and ``parameters=None`` the
    structural reject must not fire (per-iteration check handles rows)."""
    _classify_caller_sql("SELECT ?", None, skip_param_count_check=True)
