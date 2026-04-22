"""Parametrised matrix: SQLite error codes → PEP 249 exception classes.

Pins the classification rules of :func:`dqlitedbapi.cursor._call_client`.
A regression that changes how a code is classified (e.g., a new SQLite
error code added upstream that falls into the constraint family, or a
refactor that forgets the mask) trips this test rather than silently
surfacing via a bug report.
"""

import pytest

import dqliteclient.exceptions as _client_exc
from dqlitedbapi.cursor import _call_client, _classify_operational
from dqlitedbapi.exceptions import (
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    OperationalError,
    ProgrammingError,
)

# (code, expected PEP 249 class) pairs. Primary codes and the
# constraint extended family cover the non-default branches; a couple
# of unrelated codes confirm the default stays OperationalError.
_MATRIX = [
    # SQLITE_CONSTRAINT family — all map to IntegrityError.
    (19, IntegrityError),  # SQLITE_CONSTRAINT
    (275, IntegrityError),  # SQLITE_CONSTRAINT_CHECK
    (531, IntegrityError),  # SQLITE_CONSTRAINT_COMMITHOOK
    (787, IntegrityError),  # SQLITE_CONSTRAINT_FOREIGNKEY
    (1043, IntegrityError),  # SQLITE_CONSTRAINT_FUNCTION
    (1299, IntegrityError),  # SQLITE_CONSTRAINT_NOTNULL
    (1555, IntegrityError),  # SQLITE_CONSTRAINT_PRIMARYKEY
    (1811, IntegrityError),  # SQLITE_CONSTRAINT_TRIGGER
    (2067, IntegrityError),  # SQLITE_CONSTRAINT_UNIQUE
    (2323, IntegrityError),  # SQLITE_CONSTRAINT_VTAB
    (2579, IntegrityError),  # SQLITE_CONSTRAINT_ROWID
    (2835, IntegrityError),  # SQLITE_CONSTRAINT_PINNED
    (3091, IntegrityError),  # SQLITE_CONSTRAINT_DATATYPE
    # Other primary codes — default to OperationalError.
    (1, OperationalError),  # SQLITE_ERROR
    (5, OperationalError),  # SQLITE_BUSY
    (8, OperationalError),  # SQLITE_READONLY
    # Data-category codes map to PEP 249 DataError.
    (18, DataError),  # SQLITE_TOOBIG
    (20, DataError),  # SQLITE_MISMATCH
    # SQLITE_RANGE — bind-index out of range — is caller-side bad
    # parameter, closest to PEP 249 ProgrammingError.
    (25, ProgrammingError),  # SQLITE_RANGE
    # SQLITE_INTERNAL (primary code 2) — PEP 249 and stdlib sqlite3 map
    # this to InternalError. Extended-code siblings (``code & 0xFF == 2``)
    # should follow the same mapping by the masking convention.
    (2, InternalError),  # SQLITE_INTERNAL
    (258, InternalError),  # hypothetical extended SQLITE_INTERNAL sibling
    # dqlite-specific leader-change codes. These share primary 10
    # (SQLITE_IOERR) per SQLite's ``primary | (ext << 8)`` convention,
    # so the mask correctly routes them to OperationalError.
    (10250, OperationalError),  # SQLITE_IOERR_NOT_LEADER
    (10506, OperationalError),  # SQLITE_IOERR_LEADERSHIP_LOST
    # No code at all — default OperationalError.
    (None, OperationalError),
]


@pytest.mark.parametrize("code,expected_cls", _MATRIX)
def test_classify_operational(code: int | None, expected_cls: type) -> None:
    assert _classify_operational(code) is expected_cls


@pytest.mark.parametrize("code,expected_cls", _MATRIX)
async def test_call_client_maps_code(code: int | None, expected_cls: type) -> None:
    """_call_client must dispatch a client.OperationalError to the
    expected PEP 249 class with .code preserved.
    """

    async def raise_op() -> None:
        raise _client_exc.OperationalError(code or 0, "boom")

    with pytest.raises(expected_cls) as exc_info:
        await _call_client(raise_op())
    # Code must be forwarded regardless of classification.
    assert getattr(exc_info.value, "code", None) == (code or 0)
    # The mapped exception remains a DatabaseError (PEP 249 root for
    # database-sourced failures).
    assert isinstance(
        exc_info.value,
        OperationalError | IntegrityError | InternalError | DataError | ProgrammingError,
    )


async def test_call_client_other_client_errors_still_map() -> None:
    """The non-OperationalError branches are not affected by the new
    classification logic; confirm the mapping still works.
    """

    async def raise_data() -> None:
        raise _client_exc.DataError("bad value")

    with pytest.raises(DataError, match="bad value"):
        await _call_client(raise_data())

    async def raise_iface() -> None:
        raise _client_exc.InterfaceError("wrong state")

    with pytest.raises(InterfaceError, match="wrong state"):
        await _call_client(raise_iface())
