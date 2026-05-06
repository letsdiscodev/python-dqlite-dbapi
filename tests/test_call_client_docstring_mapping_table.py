"""Pin: ``_call_client``'s docstring mapping table reflects what the
code actually does.

The docstring is operator-facing — runbooks and structured-error
tooling use it as the source of truth for "which dbapi exception
class will I see for which client-layer cause". Drift between the
table and the code's actual ``except`` arms silently misroutes
operators when a future maintainer adds a new arm or changes a
mapping.

Two specific drifts these pins close:

1. Pre-fix the docstring claimed "any other DqliteError → dbapi
   .InterfaceError" but the catch-all arm raises ``DatabaseError``
   (the conservative fallback so future server-sourced errors are
   classified as server-side, not driver-misuse).

2. Pre-fix the docstring omitted ``ClusterPolicyError → dbapi
   .InterfaceError`` and ``wire.EncodeError → dbapi.DataError`` —
   both implemented as dedicated ``except`` arms but missing from
   the operator-facing mapping table.

These pins assert the rows are present and the fallback class is
``DatabaseError`` (not ``InterfaceError``).
"""

import pytest

from dqlitedbapi.cursor import _call_client


def _row_line_for(needle: str) -> str:
    """Return the docstring TABLE-ROW line containing ``needle``;
    fail the test if no such line exists. Mapping-table rows contain
    the ``→`` arrow so the heuristic skips the preamble paragraph
    where class names are mentioned in prose.
    """
    docstring = _call_client.__doc__ or ""
    for line in docstring.splitlines():
        if needle in line and "→" in line:
            return line
    pytest.fail(f"_call_client docstring missing table-row for {needle!r}; got: {docstring!r}")


def test_docstring_includes_cluster_policy_error_row_routing_to_interface_error() -> None:
    """``ClusterPolicyError → dbapi.InterfaceError`` is a code-arm at
    cursor.py:260+ (`Cluster policy rejection; ...`); the operator-
    facing mapping table must list it AND show the right target so
    runbooks know what to catch.
    """
    line = _row_line_for("ClusterPolicyError")
    assert "InterfaceError" in line, (
        f"ClusterPolicyError row must route to InterfaceError; got: {line!r}"
    )


def test_docstring_includes_wire_encode_error_row_routing_to_data_error() -> None:
    """``wire.EncodeError → dbapi.DataError`` is a code-arm at
    cursor.py:308+ (`wire encode failed: ...`); operator-facing
    mapping must list it AND show the right target.
    """
    line = _row_line_for("EncodeError")
    assert "DataError" in line, f"wire.EncodeError row must route to DataError; got: {line!r}"


def test_docstring_fallback_class_is_database_error_not_interface_error() -> None:
    """Pre-fix the docstring said "any other DqliteError → dbapi.
    InterfaceError" but the actual catch-all raises ``DatabaseError``.
    Cross-driver code following the docstring writes the wrong
    ``except`` clause and silently misses future server-sourced
    failures. Pin the corrected mapping.
    """
    line = _row_line_for("any other DqliteError")
    assert "DatabaseError" in line, f"fallback row must route to DatabaseError; got: {line!r}"
    assert "InterfaceError" not in line, (
        f"fallback row must NOT route to InterfaceError; got: {line!r}"
    )
