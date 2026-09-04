"""PEP 249 exception hierarchy and translation of client-layer errors."""

import sqlite3 as _sqlite3
from collections.abc import Awaitable
from functools import lru_cache
from typing import Final

import dqliteclient.exceptions as _client
from dqlitewire import (
    BARE_DATABASE_ERROR_CODES,
    DEFAULT_MAX_RAW_MESSAGE,
    DQLITE_NOTFOUND,
    DQLITE_PARSE,
    DQLITE_PROTO,
    LEADER_ERROR_CODES,
    SQLITE_AUTH,
    SQLITE_CONSTRAINT,
    SQLITE_ERROR,
    SQLITE_INTERNAL,
    SQLITE_IOERR_LEADERSHIP_LOST,
    SQLITE_IOERR_LEADERSHIP_LOST_LEGACY,
    SQLITE_MISMATCH,
    SQLITE_MISUSE,
    SQLITE_NOLFS,
    SQLITE_NOMEM,
    SQLITE_NOTFOUND,
    SQLITE_NOTICE,
    SQLITE_RANGE,
    SQLITE_TOOBIG,
    SQLITE_WARNING,
    EncodeError,
    cap_raw_message,
    is_dqlite_namespace_code,
    primary_sqlite_code,
)

__all__ = [
    "AMBIGUOUS_COMMIT_CODES",
    "CLUSTER_POLICY_REJECTION_PREFIX",
    "FAILED_TO_CONNECT_PREFIX",
    "AdapterLookupError",
    "AmbiguousCommitError",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
]

# Message prefixes sqlalchemy-dqlite matches on; keep them stable.
FAILED_TO_CONNECT_PREFIX: Final[str] = "Failed to connect: "
CLUSTER_POLICY_REJECTION_PREFIX: Final[str] = "Cluster policy rejection"

# LEADERSHIP_LOST is raised from the raft apply callback, i.e. after the entry was
# submitted, so the write is in doubt. NOT_LEADER is a clean pre-apply rejection.
AMBIGUOUS_COMMIT_CODES: Final[frozenset[int]] = frozenset(
    {SQLITE_IOERR_LEADERSHIP_LOST, SQLITE_IOERR_LEADERSHIP_LOST_LEGACY}
)

_NO_TRANSACTION_SUBSTRINGS: Final[tuple[str, ...]] = ("no transaction is active",)


class Warning(Exception):  # noqa: A001, N818 - PEP 249 mandated name
    """PEP 249 Warning class; exported for parity, never raised by this driver."""


class Error(Exception):
    """Base class of all driver errors.

    ``code`` is the SQLite/dqlite result code when the server supplied one;
    ``raw_message`` is the untruncated server text.
    """

    code: int | None
    raw_message: str

    def __init__(
        self,
        message: object = "",
        code: int | None = None,
        *,
        raw_message: str | None = None,
    ) -> None:
        text = message if isinstance(message, str) else str(message)
        super().__init__(_cap(text))
        self.code = code
        self.raw_message = _cap(text if raw_message is None else raw_message)

    @property
    def sqlite_errorcode(self) -> int | None:
        return self.code

    @property
    def sqlite_errorname(self) -> str | None:
        return _sqlite_errorname(self.code)

    def __repr__(self) -> str:
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


class InterfaceError(Error):
    """Misuse of the driver interface (closed handles, wrong thread or loop, bad arguments)."""


class DatabaseError(Error):
    """Error reported by the database or the cluster."""


class DataError(DatabaseError):
    """Problem with the processed data (bad bind value, unparsable server value)."""


class OperationalError(DatabaseError):
    """Error related to database operation, including transport and cluster faults."""


class AmbiguousCommitError(OperationalError):
    """COMMIT lost leadership after the entry was submitted; the write may or may not persist."""


class IntegrityError(DatabaseError):
    """Constraint violation (SQLITE_CONSTRAINT family)."""


class InternalError(DatabaseError):
    """Internal database error (SQLITE_INTERNAL family)."""


class ProgrammingError(DatabaseError):
    """Caller error: bad SQL, wrong parameter count, misuse of the API."""


class NotSupportedError(DatabaseError):
    """The requested feature has no counterpart in dqlite."""


class AdapterLookupError(ProgrammingError, LookupError):
    """``unregister_adapter`` found no adapter for the type."""


def _cap(text: str) -> str:
    capped = cap_raw_message(text, DEFAULT_MAX_RAW_MESSAGE)
    assert capped is not None
    return capped


# Primary result codes collide by value with stdlib authorizer constants, so name them here.
_PRIMARY_RESULT_CODE_NAMES: Final[dict[int, str]] = {
    0: "SQLITE_OK",
    1: "SQLITE_ERROR",
    2: "SQLITE_INTERNAL",
    3: "SQLITE_PERM",
    4: "SQLITE_ABORT",
    5: "SQLITE_BUSY",
    6: "SQLITE_LOCKED",
    7: "SQLITE_NOMEM",
    8: "SQLITE_READONLY",
    9: "SQLITE_INTERRUPT",
    10: "SQLITE_IOERR",
    11: "SQLITE_CORRUPT",
    12: "SQLITE_NOTFOUND",
    13: "SQLITE_FULL",
    14: "SQLITE_CANTOPEN",
    15: "SQLITE_PROTOCOL",
    16: "SQLITE_EMPTY",
    17: "SQLITE_SCHEMA",
    18: "SQLITE_TOOBIG",
    19: "SQLITE_CONSTRAINT",
    20: "SQLITE_MISMATCH",
    21: "SQLITE_MISUSE",
    22: "SQLITE_NOLFS",
    23: "SQLITE_AUTH",
    24: "SQLITE_FORMAT",
    25: "SQLITE_RANGE",
    26: "SQLITE_NOTADB",
    27: "SQLITE_NOTICE",
    28: "SQLITE_WARNING",
    100: "SQLITE_ROW",
    101: "SQLITE_DONE",
}


@lru_cache(maxsize=1)
def _extended_code_names() -> dict[int, str]:
    table: dict[int, str] = {}
    for name in dir(_sqlite3):
        value = getattr(_sqlite3, name)
        if name.startswith("SQLITE_") and isinstance(value, int) and value >= 256:
            table.setdefault(value, name)
    return table


def _sqlite_errorname(code: int | None) -> str | None:
    if code is None:
        return None
    if code in _PRIMARY_RESULT_CODE_NAMES:
        return _PRIMARY_RESULT_CODE_NAMES[code]
    # dqlite-namespace and legacy leader codes collide with unrelated stdlib constants.
    if is_dqlite_namespace_code(code) or code in LEADER_ERROR_CODES:
        return None
    return _extended_code_names().get(code)


_CODE_TO_CLASS: Final[dict[int, type[Error]]] = {
    SQLITE_CONSTRAINT: IntegrityError,
    SQLITE_MISMATCH: IntegrityError,
    SQLITE_INTERNAL: InternalError,
    SQLITE_NOTFOUND: InternalError,
    SQLITE_NOMEM: InternalError,
    SQLITE_TOOBIG: DataError,
    SQLITE_RANGE: InterfaceError,
    SQLITE_MISUSE: InterfaceError,
    SQLITE_NOLFS: DatabaseError,
    SQLITE_AUTH: DatabaseError,
    SQLITE_NOTICE: DatabaseError,
    SQLITE_WARNING: DatabaseError,
    **dict.fromkeys(BARE_DATABASE_ERROR_CODES, DatabaseError),
    DQLITE_PROTO: InterfaceError,
    DQLITE_NOTFOUND: ProgrammingError,
    DQLITE_PARSE: ProgrammingError,
}


def classify(code: int | None) -> type[Error]:
    """PEP 249 class for a server result code; unknown codes are ``OperationalError``."""
    if code is None:
        return OperationalError
    return _CODE_TO_CLASS.get(primary_sqlite_code(code), OperationalError)


def translate(exc: BaseException) -> Error | None:
    """Map a client-layer exception to its dbapi equivalent; ``None`` if it is not one."""
    raw = getattr(exc, "raw_message", None) or str(exc)
    if isinstance(exc, _client.OperationalError):
        return classify(exc.code)(exc.message, code=exc.code, raw_message=exc.raw_message)
    if isinstance(exc, _client.ClusterPolicyError):
        return InterfaceError(f"{CLUSTER_POLICY_REJECTION_PREFIX}; {exc}", raw_message=raw)
    if isinstance(exc, _client.DqliteConnectionError):
        return OperationalError(str(exc), code=exc.code, raw_message=raw)
    if isinstance(exc, _client.ClusterError | _client.ProtocolError):
        return OperationalError(str(exc), raw_message=raw)
    if isinstance(exc, _client.DataError):
        return DataError(str(exc), raw_message=raw)
    if isinstance(exc, EncodeError):
        return DataError(f"wire encode failed: {exc}", raw_message=raw)
    if isinstance(exc, _client.InterfaceError):
        return InterfaceError(str(exc), raw_message=raw)
    if isinstance(exc, _client.DqliteError):
        return DatabaseError(
            f"unrecognized client error ({type(exc).__name__}): {exc}", raw_message=raw
        )
    if isinstance(exc, OSError):
        return OperationalError(str(exc), raw_message=raw)
    return None


async def call[T](awaitable: Awaitable[T]) -> T:
    """Await a client coroutine, re-raising client errors as dbapi errors."""
    try:
        return await awaitable
    except Exception as exc:
        mapped = translate(exc)
        if mapped is None:
            raise
        raise mapped from exc


def is_no_transaction_error(exc: BaseException) -> bool:
    """True for the server's "no transaction is active" reply to COMMIT / ROLLBACK."""
    code = getattr(exc, "code", None)
    if code is None or primary_sqlite_code(code) != SQLITE_ERROR:
        return False
    raw = (getattr(exc, "raw_message", None) or str(exc)).lower()
    return any(s in raw for s in _NO_TRANSACTION_SUBSTRINGS)
