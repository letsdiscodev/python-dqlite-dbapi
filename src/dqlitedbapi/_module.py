"""Module-level PEP 249 attributes shared by ``dqlitedbapi`` and ``dqlitedbapi.aio``."""

from typing import Final, Literal, NoReturn

from dqlitedbapi.exceptions import NotSupportedError

__version__: Final[str] = "0.4.0"

apilevel: Final[Literal["2.0"]] = "2.0"
threadsafety: Final[Literal[2]] = 2
paramstyle: Final[Literal["qmark"]] = "qmark"

# The SQLite feature floor every supported dqlite server provides (RETURNING needs 3.35).
sqlite_version_info: Final[tuple[int, int, int]] = (3, 35, 0)
sqlite_version: Final[str] = "3.35.0"

# stdlib sqlite3 parity constants; ``PARSE_*`` are accepted nowhere and enable nothing.
LEGACY_TRANSACTION_CONTROL: Final[int] = -1
PARSE_DECLTYPES: Final[int] = 1
PARSE_COLNAMES: Final[int] = 2


def register_converter(*args: object, **kwargs: object) -> NoReturn:
    raise NotSupportedError(
        "register_converter is not supported: the wire carries no declared column types; "
        "use row_factory to post-process rows"
    )


def complete_statement(*args: object, **kwargs: object) -> NoReturn:
    raise NotSupportedError("complete_statement is not supported")


def enable_callback_tracebacks(*args: object, **kwargs: object) -> NoReturn:
    raise NotSupportedError("enable_callback_tracebacks is not supported: there are no callbacks")
