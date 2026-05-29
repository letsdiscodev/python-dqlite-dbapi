"""Pre-wire SQL interception at the dbapi cursor layer.

Two interceptions rewrite or short-circuit caller SQL before it reaches the wire:

1. ``PRAGMA busy_timeout`` — handled locally (the server's VFS authorizer rejects it on the
   wire) by populating cursor result state directly.
2. Bare ``BEGIN`` — rewritten to ``BEGIN IMMEDIATE`` under the default ``"immediate"``
   session mode so the writer-lock is taken at BEGIN time, avoiding the
   ``SQLITE_BUSY_SNAPSHOT`` race in SELECT-then-INSERT. Other modes leave it untouched.
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING, Final

from dqlitewire import ValueType

if TYPE_CHECKING:
    from dqlitedbapi.cursor import Cursor

# Matches single-statement ``PRAGMA busy_timeout`` (getter and ``=N``/``(N)`` setter forms);
# a trailing semicolon with more content does NOT match, so multi-statement input routes to
# the wire's existing classifier.
_PRAGMA_BUSY_TIMEOUT_RE = re.compile(
    r"^\s*PRAGMA\s+busy_timeout\s*"
    r"(?:=\s*(?P<setter_eq>-?\d+)|\(\s*(?P<setter_paren>-?\d+)\s*\))?"
    r"\s*;?\s*$",
    re.IGNORECASE,
)


def try_intercept_busy_timeout(
    cursor: Cursor,
    statement: str,
    parameters: object,
) -> bool:
    """Populate cursor result state and return True for any ``PRAGMA busy_timeout`` form.

    On True the caller MUST short-circuit before the wire round-trip; the server's VFS
    authorizer would otherwise reject the PRAGMA. ``parameters`` is accepted for signature
    parity and ignored (PRAGMA takes none), matching stdlib.
    """
    if not isinstance(statement, str):
        return False
    match = _PRAGMA_BUSY_TIMEOUT_RE.match(statement)
    if match is None:
        return False
    setter_eq = match.group("setter_eq")
    setter_paren = match.group("setter_paren")
    setter_value = setter_eq if setter_eq is not None else setter_paren
    connection = cursor._connection
    if setter_value is not None:
        # SQLite clamps negative values to 0 (no wait) — stdlib parity.
        new_ms = max(0, int(setter_value))
        connection._busy_timeout = new_ms / 1000.0
    # Round rather than truncate: ``N / 1000.0`` is not exactly representable, so a plain
    # ``int(seconds * 1000)`` lands at ``N - 1``; ``round`` matches stdlib's exact echo.
    current_ms = round(connection._busy_timeout * 1000)
    # Report the wire-level INTEGER type code (== NUMBER) so ``description[i][1] == NUMBER``
    # introspection holds, as it does on the normal wire path.
    cursor._description = (("busy_timeout", int(ValueType.INTEGER), None, None, None, None, None),)
    cursor._rows = [(current_ms,)]
    cursor._rowcount = -1
    cursor._row_index = 0
    return True


# Matches only bare ``BEGIN`` / ``BEGIN TRANSACTION``; explicit DEFERRED/IMMEDIATE/EXCLUSIVE
# state caller intent and must NOT be upgraded.
_BEGIN_REWRITE_RE = re.compile(
    r"^\s*BEGIN(?:\s+TRANSACTION)?\s*;?\s*$",
    re.IGNORECASE,
)

# Recognised session-mode values for the kwarg / URL / env-var / SA execution-option.
_SESSION_MODE_VALUES: Final[frozenset[str]] = frozenset(
    {"immediate", "deferred", "exclusive", "read_only"}
)

# Import-time snapshot of the env default; used only as fallback by the live re-read below.
_DQLITE_SESSION_MODE_ENV: Final[str] = os.environ.get("DQLITE_SESSION_MODE", "").strip().lower()


def session_mode_default_from_env() -> str:
    """Env-var default for ``session_mode``; missing/empty -> ``"immediate"``.

    Re-read each call so ``monkeypatch.setenv`` is honoured. Invalid value raises ``ValueError``.
    """
    raw = os.environ.get("DQLITE_SESSION_MODE")
    value = (raw if raw is not None else _DQLITE_SESSION_MODE_ENV).strip().lower()
    if not value:
        return "immediate"
    if value not in _SESSION_MODE_VALUES:
        raise ValueError(
            f"DQLITE_SESSION_MODE={raw!r} is not one of {sorted(_SESSION_MODE_VALUES)}"
        )
    return value


def validate_session_mode(value: object) -> str:
    """Return the canonical lowercase ``session_mode``; raise ``ValueError`` if unrecognised."""
    if not isinstance(value, str):
        raise ValueError(f"session_mode must be a str, got {type(value).__name__}")
    normalised = value.lower()
    if normalised not in _SESSION_MODE_VALUES:
        raise ValueError(
            f"Invalid session_mode {value!r}; valid values are {sorted(_SESSION_MODE_VALUES)}"
        )
    return normalised


def try_rewrite_begin_to_immediate(
    statement: str,
    *,
    session_mode: str,
) -> str | None:
    """Return ``"BEGIN IMMEDIATE"`` for a bare BEGIN under ``"immediate"`` mode, else ``None``."""
    if session_mode != "immediate":
        return None
    if not isinstance(statement, str):
        return None
    if _BEGIN_REWRITE_RE.match(statement) is None:
        return None
    return "BEGIN IMMEDIATE"
