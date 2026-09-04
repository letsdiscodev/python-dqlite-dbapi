"""Pin: PEP 249 classification for SQLite primary codes (matching stdlib sqlite3)."""

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
async def test_primary_code_classifies_to_pep249_class(
    code: int, exc_class: type[Exception]
) -> None:
    async def _raise() -> None:
        raise _client_exc.OperationalError(f"primary {code}", code)

    with pytest.raises(exc_class) as ei:
        await _call_client(_raise())
    assert ei.value.code == code  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("extended", "exc_class"),
    [
        (11 | (1 << 8), DatabaseError),  # SQLITE_CORRUPT_VTAB
        (11 | (2 << 8), DatabaseError),  # SQLITE_CORRUPT_SEQUENCE
    ],
)
async def test_extended_corrupt_codes_classify_to_database_error(
    extended: int, exc_class: type[Exception]
) -> None:
    """Extended CORRUPT codes mask down to primary 11 and still reach DatabaseError."""

    async def _raise() -> None:
        raise _client_exc.OperationalError(f"extended {extended}", extended)

    with pytest.raises(exc_class) as ei:
        await _call_client(_raise())
    assert ei.value.code == extended  # type: ignore[attr-defined]


def test_database_error_carries_code_and_raw_message() -> None:
    """DatabaseError accepts code/raw_message so the mappings preserve them."""
    exc = DatabaseError("disk image is malformed", code=11)
    assert exc.code == 11
    assert exc.raw_message == "disk image is malformed"

    exc2 = DatabaseError("short", code=11, raw_message="full server text")
    assert exc2.raw_message == "full server text"
