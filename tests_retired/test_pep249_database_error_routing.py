"""Primary SQLite codes route to bare DatabaseError, matching CPython util.c."""

from __future__ import annotations

import pytest

from dqlitedbapi.cursor import _classify_operational
from dqlitedbapi.exceptions import DatabaseError


@pytest.mark.parametrize(
    ("code", "label"),
    [
        (22, "SQLITE_NOLFS"),
        (23, "SQLITE_AUTH"),
        (27, "SQLITE_NOTICE"),
        (28, "SQLITE_WARNING"),
    ],
)
def test_primary_code_routes_to_bare_database_error(code: int, label: str) -> None:
    cls = _classify_operational(code)
    assert cls is DatabaseError, (
        f"primary code {code} ({label}) must route to DatabaseError "
        f"per CPython util.c default arm; got {cls.__name__}"
    )


@pytest.mark.parametrize(
    ("ext_code", "primary"),
    [
        (279, 23),  # SQLITE_AUTH_USER (23 | 1<<8)
        (283, 27),  # SQLITE_NOTICE_RECOVER_WAL
        (284, 28),  # SQLITE_WARNING_AUTOINDEX
    ],
)
def test_extended_code_inherits_primary_routing(ext_code: int, primary: int) -> None:
    cls = _classify_operational(ext_code)
    assert cls is DatabaseError, (
        f"extended code {ext_code} (primary {primary}) must inherit "
        f"DatabaseError routing; got {cls.__name__}"
    )
