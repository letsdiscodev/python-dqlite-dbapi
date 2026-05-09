"""Pin: ``Connection`` and ``AsyncConnection`` reject empty,
whitespace-only, AND leading/trailing-whitespace ``database=`` values
with ``InterfaceError`` at the construction site, mirroring the
address discipline (``_client_parse_address`` rejects empty and
whitespace-bearing addresses).

dqlite-server's ``OPEN(name=whitespace)`` has implementation-defined
semantics: it may create a database literally named ``" "``, fail
with a SQL-level filename error, or silently mismatch a future open
of the same logical name written without surrounding whitespace. The
dbapi layer is the strict canonicalisation boundary; surfacing this
at construction beats a downstream wire-time failure.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi.aio import AsyncConnection


@pytest.mark.parametrize(
    "bad",
    ["", " ", "  ", "\t", "\n", "\r\n", " \t\n ", "  default", "default ", " default "],
)
def test_sync_connection_rejects_whitespace_database(bad: str) -> None:
    with pytest.raises(dqlitedbapi.InterfaceError, match=r"non-empty string|leading or trailing"):
        dqlitedbapi.Connection("localhost:9001", database=bad)


@pytest.mark.parametrize(
    "bad",
    ["", " ", "  ", "\t", "\n", "\r\n", " \t\n ", "  default", "default ", " default "],
)
def test_async_connection_rejects_whitespace_database(bad: str) -> None:
    with pytest.raises(dqlitedbapi.InterfaceError, match=r"non-empty string|leading or trailing"):
        AsyncConnection("localhost:9001", database=bad)


def test_sync_connection_accepts_non_empty_database() -> None:
    conn = dqlitedbapi.Connection("localhost:9001", database="default")
    assert conn._database == "default"


def test_async_connection_accepts_non_empty_database() -> None:
    conn = AsyncConnection("localhost:9001", database="default")
    assert conn._database == "default"


def test_sync_diagnostic_includes_offending_value() -> None:
    """Diagnostic carries the offending value via repr so operators
    can correlate the typo with their config."""
    with pytest.raises(dqlitedbapi.InterfaceError, match=r"' default'") as exc:
        dqlitedbapi.Connection("localhost:9001", database=" default")
    assert "leading or trailing" in str(exc.value)
