"""Pin: PEP 249 classification for newly-mapped SQLite primary codes.

CPython stdlib ``sqlite3`` (``Modules/_sqlite/util.c::_pysqlite_seterror``)
maps several primary codes that the dqlite dbapi previously bucketed
into ``OperationalError``. Callers porting between stdlib and dqlite
who use ``except DatabaseError:`` for corruption-handling get
incomplete coverage when those codes route to ``OperationalError``.

This file pins the new mappings and the rationale.
"""

from __future__ import annotations

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.cursor import _call_client
from dqlitedbapi.exceptions import (
    DatabaseError,
    InternalError,
    OperationalError,
)


@pytest.mark.parametrize(
    ("code", "exc_class"),
    [
        (7, InternalError),  # SQLITE_NOMEM
        (11, DatabaseError),  # SQLITE_CORRUPT
        (15, OperationalError),  # SQLITE_PROTOCOL
        (24, DatabaseError),  # SQLITE_FORMAT
        (26, DatabaseError),  # SQLITE_NOTADB
    ],
)
@pytest.mark.asyncio
async def test_primary_code_classifies_to_pep249_class(
    code: int, exc_class: type[Exception]
) -> None:
    async def _raise() -> None:
        raise _client_exc.OperationalError(code, f"primary {code}")

    with pytest.raises(exc_class) as ei:
        await _call_client(_raise())
    # Code preserved on every class (DatabaseError now also carries code).
    assert ei.value.code == code  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("extended", "exc_class"),
    [
        (11 | (1 << 8), DatabaseError),  # SQLITE_CORRUPT_VTAB
        (11 | (2 << 8), DatabaseError),  # SQLITE_CORRUPT_SEQUENCE
    ],
)
@pytest.mark.asyncio
async def test_extended_corrupt_codes_classify_to_database_error(
    extended: int, exc_class: type[Exception]
) -> None:
    """Extended CORRUPT codes mask down to primary 11 via
    primary_sqlite_code; verify they reach DatabaseError too."""

    async def _raise() -> None:
        raise _client_exc.OperationalError(extended, f"extended {extended}")

    with pytest.raises(exc_class) as ei:
        await _call_client(_raise())
    assert ei.value.code == extended  # type: ignore[attr-defined]


def test_database_error_carries_code_and_raw_message() -> None:
    """Sanity: the public DatabaseError now accepts code/raw_message
    so the new mappings preserve those attributes."""
    exc = DatabaseError("disk image is malformed", code=11)
    assert exc.code == 11
    assert exc.raw_message == "disk image is malformed"

    exc2 = DatabaseError("short", code=11, raw_message="full server text")
    assert exc2.raw_message == "full server text"
