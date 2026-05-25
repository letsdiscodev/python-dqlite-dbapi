"""Pre-wire SQL interception at the dbapi cursor layer.

This module hosts two interceptions that rewrite or short-circuit
caller SQL before it reaches the dqlite wire:

1. ``PRAGMA busy_timeout`` — see ``try_intercept_busy_timeout`` below
   for the rationale (authorizer rejection of the canonical SQLite
   knob).

2. Bare ``BEGIN`` (and ``BEGIN DEFERRED`` / ``BEGIN TRANSACTION``) —
   rewritten to ``BEGIN IMMEDIATE`` so a SELECT-then-INSERT
   transaction acquires the writer-lock at BEGIN time and cannot be
   overtaken by a concurrent committer (the ``SQLITE_BUSY_SNAPSHOT``
   race). dqlite-server's own VFS recommends ``BEGIN IMMEDIATE``
   for write-bearing transactions (see ``dqlite-upstream/src/vfs.c``
   around the WAL-open path). Pass-through for ``BEGIN IMMEDIATE``
   and ``BEGIN EXCLUSIVE`` (caller intent already correct).

   Off-switch: ``connect(..., begin_immediate=False)`` per-connection,
   or ``DQLITE_BEGIN_IMMEDIATE=0`` env var at import time.

The PRAGMA interception writes cursor result state directly:

  - Setter (``PRAGMA busy_timeout = N`` or ``PRAGMA busy_timeout(N)``)
    updates ``connection._busy_timeout`` in seconds (N is ms per
    SQLite convention; we divide by 1000) and emits the new value as
    a single-row result, matching stdlib's PRAGMA result-set shape.
  - Getter (``PRAGMA busy_timeout``) emits the current value as a
    single-row result.

Case-insensitive and whitespace-tolerant per SQLite parser
conventions. Multi-statement strings ARE rejected (only a SINGLE
``PRAGMA busy_timeout`` statement is intercepted; multi-statement
inputs surface to the wire where the existing multi-statement
classifier rejects them with ``ProgrammingError`` — that path is
unchanged).

The interception writes the cursor's result state directly:

  - ``_description = (("busy_timeout", None, None, None, None, None, None),)``
  - ``_rows = [(N_ms_clamped,)]``
  - ``_rowcount = -1`` (PRAGMA does not have a meaningful row count)
  - ``_row_index = 0``

Then returns True so the caller short-circuits before the wire
round-trip.

The BEGIN rewrite, by contrast, returns the rewritten SQL string
(or ``None`` to leave the caller's SQL unchanged) — there is no
cursor state to populate; the caller substitutes the SQL and lets
the rewritten ``BEGIN IMMEDIATE`` flow through the regular wire
round-trip.
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dqlitedbapi.cursor import Cursor

# Match ``PRAGMA busy_timeout`` with optional setter forms:
#
#   PRAGMA busy_timeout
#   PRAGMA busy_timeout = 5000
#   PRAGMA busy_timeout=5000
#   pragma BUSY_TIMEOUT  = 5000
#   PRAGMA busy_timeout(5000)
#
# The trailing ``;?`` accommodates the canonical single-statement
# terminator. A semicolon followed by additional content (multi-
# statement) does NOT match — those route to the wire where the
# existing multi-statement classifier rejects them.
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
    """Return True if the statement is ``PRAGMA busy_timeout`` (any
    form) and the cursor's result state has been populated.

    When True, the caller MUST short-circuit BEFORE the wire round-
    trip (the server would reject the PRAGMA via the VFS authorizer
    if it reached the wire, defeating the interception's purpose).

    ``parameters`` is accepted for caller-signature parity but
    PRAGMA does not take parameters. If parameters are non-empty
    we still match the PRAGMA and ignore them — the engine would
    do the same. (A future refinement could raise a friendlier
    error here, but stdlib silently accepts the same shape.)
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
        # Setter form: update connection._busy_timeout.
        # SQLite clamps negative values to 0 (no wait) — stdlib parity.
        new_ms = max(0, int(setter_value))
        connection._busy_timeout = new_ms / 1000.0
    # Both setter AND getter forms emit the (possibly updated) value
    # as a single-row result. Matches SQLite's PRAGMA convention:
    # ``PRAGMA busy_timeout = 5000`` returns ``(5000,)``,
    # ``PRAGMA busy_timeout`` (getter) also returns ``(5000,)``.
    current_ms = int(connection._busy_timeout * 1000)
    cursor._description = (("busy_timeout", None, None, None, None, None, None),)
    cursor._rows = [(current_ms,)]
    cursor._rowcount = -1
    cursor._row_index = 0
    # Match stdlib: PRAGMA does not set lastrowid.
    return True


# Match the BEGIN forms that should be upgraded to BEGIN IMMEDIATE:
#
#   BEGIN
#   BEGIN;
#   BEGIN TRANSACTION
#   BEGIN TRANSACTION;
#
# Explicit ``BEGIN DEFERRED`` / ``BEGIN IMMEDIATE`` / ``BEGIN
# EXCLUSIVE`` MUST NOT match — the caller (or the SA dialect's
# per-session ``dqlite_begin_mode`` opt-out) has stated explicit
# intent. The bare ``BEGIN`` form is what SA emits by default
# and what most callers reach for; it is the only ambiguous
# shape that benefits from the writer-safe upgrade.
_BEGIN_REWRITE_RE = re.compile(
    r"^\s*BEGIN(?:\s+TRANSACTION)?\s*;?\s*$",
    re.IGNORECASE,
)

# Env-var default for the rewrite. Read at module-import time;
# explicit ``connect(..., begin_immediate=...)`` always wins. Treats
# any of ``"0"`` / ``"false"`` / ``"off"`` / ``"no"``
# (case-insensitive) as "disabled". Anything else (including missing)
# leaves the rewrite ENABLED — the fix is on by default.
_BEGIN_IMMEDIATE_ENV_DISABLED: bool = os.environ.get(
    "DQLITE_BEGIN_IMMEDIATE", ""
).strip().lower() in ("0", "false", "off", "no")


def begin_immediate_default_from_env() -> bool:
    """Return the env-var-derived default for the BEGIN rewrite.

    Used by ``Connection.__init__`` when the caller does not pass an
    explicit ``begin_immediate`` kwarg. Re-read at each call so test
    fixtures that ``monkeypatch.setenv`` see the live value (the
    module-level cache is only used as the fallback when the env var
    was set at import time).
    """
    raw = os.environ.get("DQLITE_BEGIN_IMMEDIATE")
    if raw is None:
        # Honour whatever the import-time read decided.
        return not _BEGIN_IMMEDIATE_ENV_DISABLED
    return raw.strip().lower() not in ("0", "false", "off", "no")


def try_rewrite_begin_to_immediate(
    statement: str,
    *,
    enabled: bool,
) -> str | None:
    """Return ``"BEGIN IMMEDIATE"`` if ``statement`` is a plain BEGIN
    form (and the rewrite is ``enabled``), else ``None`` (caller
    leaves the SQL unchanged).

    A ``None`` return means: either the rewrite is disabled, or the
    SQL is not a recognised plain-BEGIN form (e.g. explicit
    ``BEGIN IMMEDIATE`` / ``BEGIN EXCLUSIVE`` / non-BEGIN statement).
    The caller passes the original SQL through unchanged in that
    case.

    The trailing ``;`` is consumed in the match but the rewrite emits
    the bare ``BEGIN IMMEDIATE`` keyword (no trailing ``;``). dqlite
    accepts either; the bare form is canonical.
    """
    if not enabled:
        return None
    if not isinstance(statement, str):
        return None
    if _BEGIN_REWRITE_RE.match(statement) is None:
        return None
    return "BEGIN IMMEDIATE"
