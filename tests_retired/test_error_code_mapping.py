"""Parametrised matrix pinning SQLite error code -> PEP 249 exception classification."""

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
    (18, DataError),  # SQLITE_TOOBIG
    # SQLITE_MISMATCH — stdlib sqlite3 groups with SQLITE_CONSTRAINT under IntegrityError.
    (20, IntegrityError),  # SQLITE_MISMATCH
    # SQLITE_RANGE / SQLITE_MISUSE — driver misuse; stdlib sqlite3 maps both to InterfaceError.
    (25, InterfaceError),  # SQLITE_RANGE
    (21, InterfaceError),  # SQLITE_MISUSE
    (12, InternalError),  # SQLITE_NOTFOUND
    # SQLITE_INTERNAL: primary 2 and ``code & 0xFF == 2`` siblings -> InternalError.
    (2, InternalError),  # SQLITE_INTERNAL
    (258, InternalError),  # hypothetical extended SQLITE_INTERNAL sibling
    # dqlite leader-change codes share primary 10 (SQLITE_IOERR), so the mask -> OperationalError.
    (10250, OperationalError),  # SQLITE_IOERR_NOT_LEADER
    (10506, OperationalError),  # SQLITE_IOERR_LEADERSHIP_LOST
    (None, OperationalError),
]


@pytest.mark.parametrize("code,expected_cls", _MATRIX)
def test_classify_operational(code: int | None, expected_cls: type) -> None:
    assert _classify_operational(code) is expected_cls


@pytest.mark.parametrize("code,expected_cls", _MATRIX)
async def test_call_client_maps_code(code: int | None, expected_cls: type) -> None:
    """_call_client dispatches a client.OperationalError to the expected class, .code kept."""

    async def raise_op() -> None:
        raise _client_exc.OperationalError("boom", code or 0)

    with pytest.raises(expected_cls) as exc_info:
        await _call_client(raise_op())
    assert getattr(exc_info.value, "code", None) == (code or 0)
    assert isinstance(
        exc_info.value,
        OperationalError
        | IntegrityError
        | InternalError
        | DataError
        | ProgrammingError
        | InterfaceError,
    )


async def test_call_client_other_client_errors_still_map() -> None:
    """Non-OperationalError client errors still map through unchanged."""

    async def raise_data() -> None:
        raise _client_exc.DataError("bad value")

    with pytest.raises(DataError, match="bad value"):
        await _call_client(raise_data())

    async def raise_iface() -> None:
        raise _client_exc.InterfaceError("wrong state")

    with pytest.raises(InterfaceError, match="wrong state"):
        await _call_client(raise_iface())
