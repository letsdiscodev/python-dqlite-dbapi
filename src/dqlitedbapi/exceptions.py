"""PEP 249 exception hierarchy for dqlite."""

import sqlite3 as _stdlib_sqlite3
from functools import lru_cache
from typing import Final

from dqlitewire import DEFAULT_MAX_RAW_MESSAGE as _DEFAULT_MAX_RAW_MESSAGE
from dqlitewire import LEADER_ERROR_CODES as _LEADER_ERROR_CODES
from dqlitewire import cap_raw_message as _wire_cap_raw_message
from dqlitewire import is_dqlite_namespace_code as _is_dqlite_namespace_code

__all__ = [
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


# Primary codes (0-28) collide by value with stdlib authorizer-action constants, so a
# dir(sqlite3) walk picks the wrong (alphabetically-first) name; hardcode canonical names.
# Source of truth: https://www.sqlite.org/rescode.html
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
def _stdlib_extended_code_to_name() -> dict[int, str]:
    """Map stdlib extended SQLITE_* codes (>= 256) to names; codes < 256 collide with
    authorizer/opcode/limit/config constants so they are excluded here."""
    table: dict[int, str] = {}
    for name in dir(_stdlib_sqlite3):
        if not name.startswith("SQLITE_"):
            continue
        value = getattr(_stdlib_sqlite3, name)
        if not isinstance(value, int):
            continue
        if value < 256:
            # Primary codes are hand-curated in _PRIMARY_RESULT_CODE_NAMES (collision ambiguity).
            continue
        table.setdefault(value, name)
    return table


def _sqlite_errorname(code: int | None) -> str | None:
    """Symbolic SQLite name for code, or None for unknown/dqlite-namespace/leader codes."""
    if code is None:
        return None
    name = _PRIMARY_RESULT_CODE_NAMES.get(code)
    if name is not None:
        return name
    # dqlite-namespace codes collide by value with stdlib SQLITE_DBCONFIG_* opcodes
    # (e.g. DQLITE_NOTFOUND=1002); short-circuit to avoid surfacing bogus opcode names.
    if _is_dqlite_namespace_code(code):
        return None
    # Legacy leader codes (8202/8458) collide with stdlib IOERR_DATA/IOERR_CORRUPTFS;
    # suppress so all leader codes behave uniformly with the modern ones.
    if code in _LEADER_ERROR_CODES:
        return None
    return _stdlib_extended_code_to_name().get(code)


class Warning(Exception):  # noqa: A001, N818 - PEP 249 §7 mandated class name
    """PEP 249 Warning class; exported for parity but never raised."""

    pass


# Cap on raw_message; single source of truth at the wire layer (where the rationale lives).
_MAX_RAW_MESSAGE: Final[int] = _DEFAULT_MAX_RAW_MESSAGE


def _cap_raw_message(raw_message: str) -> str:
    capped = _wire_cap_raw_message(raw_message, _MAX_RAW_MESSAGE)
    # _wire_cap_raw_message returns None only for None input; this caller passes str.
    assert capped is not None
    return capped


class Error(Exception):
    """Base class for all database errors."""

    def __reduce__(
        self,
    ) -> tuple[type["Error"], tuple[object, ...], dict[str, object]]:
        # Default __reduce__ drops instance fields (code/raw_message); preserve them so
        # cross-process pickling keeps the server text SA's is_disconnect reads.
        return (self.__class__, self.args, self.__getstate__())

    def __getstate__(self) -> dict[str, object]:
        return self.__dict__.copy()

    def __setstate__(self, state: dict[str, object] | None) -> None:
        if state:
            self.__dict__.update(state)


class InterfaceError(Error):
    """Error related to the database interface; optionally carries code and raw_message."""

    code: int | None
    raw_message: str

    def __init__(
        self,
        message: object = "",
        code: int | None = None,
        *,
        raw_message: str | None = None,
    ) -> None:
        # Cap the displayed message (args[0]) so the wire-layer 64 KiB ceiling does not
        # amplify through pickled-exception / repr surfaces; non-str messages pass through.
        capped_message: object = _cap_raw_message(message) if isinstance(message, str) else message
        super().__init__(capped_message)
        self.code = code
        resolved = str(message) if raw_message is None else raw_message
        self.raw_message = _cap_raw_message(resolved)

    @property
    def sqlite_errorcode(self) -> int | None:
        """Stdlib sqlite3-parity alias for code (Python 3.11+)."""
        return self.code

    @property
    def sqlite_errorname(self) -> str | None:
        """Stdlib sqlite3-parity alias (3.11+): symbolic name of code, or None."""
        return _sqlite_errorname(self.code)

    def __repr__(self) -> str:
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


class DatabaseError(Error):
    """Error related to the database; optionally carries code and raw_message."""

    code: int | None
    raw_message: str

    def __init__(
        self,
        message: object = "",
        code: int | None = None,
        *,
        raw_message: str | None = None,
    ) -> None:
        # See InterfaceError.__init__ for why the displayed message is capped.
        capped_message: object = _cap_raw_message(message) if isinstance(message, str) else message
        super().__init__(capped_message)
        self.code = code
        resolved = str(message) if raw_message is None else raw_message
        self.raw_message = _cap_raw_message(resolved)

    @property
    def sqlite_errorcode(self) -> int | None:
        """Stdlib sqlite3-parity alias for code (Python 3.11+)."""
        return self.code

    @property
    def sqlite_errorname(self) -> str | None:
        """Stdlib sqlite3-parity alias (3.11+): symbolic name of code, or None."""
        return _sqlite_errorname(self.code)

    def __repr__(self) -> str:
        msg = self.args[0] if self.args else ""
        if self.code is None:
            return f"{type(self).__name__}({msg!r})"
        return f"{type(self).__name__}({msg!r}, code={self.code})"


class _DatabaseErrorWithCode(DatabaseError):
    """Internal marker base for the five coded PEP 249 DatabaseError subclasses.

    Do NOT use isinstance(exc, _DatabaseErrorWithCode) to detect code-bearing errors:
    bare DatabaseError (CORRUPT/NOTADB/FORMAT) also carries a code. Use
    getattr(exc, "code", None) is not None instead.
    """

    pass


class OperationalError(_DatabaseErrorWithCode):
    """Error related to database operation."""

    pass


class AmbiguousCommitError(OperationalError):
    """COMMIT raced a leader flip; the write may or may not have persisted.

    Retry only with idempotent DML or after an out-of-band state check: retrying
    non-idempotent DML risks silent duplicate writes.
    """

    pass


class IntegrityError(_DatabaseErrorWithCode):
    """Constraint violation (UNIQUE, NOT NULL, FOREIGN KEY, CHECK; SQLITE_CONSTRAINT family)."""

    pass


class InternalError(_DatabaseErrorWithCode):
    """Internal database error (SQLITE_INTERNAL family)."""

    pass


class ProgrammingError(_DatabaseErrorWithCode):
    """Programming error (e.g. table not found, SQL syntax error)."""

    pass


class NotSupportedError(DatabaseError):
    """Method or database API not supported by database."""

    pass


class DataError(_DatabaseErrorWithCode):
    """Error due to problems with the processed data (e.g. SQLITE_MISMATCH, SQLITE_TOOBIG)."""

    pass


class AdapterLookupError(ProgrammingError, LookupError):
    """Raised by unregister_adapter when the type has no registered adapter.

    Inherits LookupError too (stdlib parity: sqlite3 raises KeyError); ProgrammingError is
    first in the MRO so Error-rooted classification wins ambiguous catches.
    """

    pass
