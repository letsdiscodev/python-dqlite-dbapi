"""``Connection``/``AsyncConnection`` reject empty, whitespace-only, and
whitespace-bearing ``database=`` values at construction with ``InterfaceError``:
the server's ``OPEN(name=whitespace)`` semantics are implementation-defined."""

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
    """Diagnostic carries the offending value via repr to ease config correlation."""
    with pytest.raises(dqlitedbapi.InterfaceError, match=r"' default'") as exc:
        dqlitedbapi.Connection("localhost:9001", database=" default")
    assert "leading or trailing" in str(exc.value)
