"""Pre-wire SQL interception at the dbapi cursor layer.

This module hosts two interceptions that rewrite or short-circuit
caller SQL before it reaches the dqlite wire:

1. ``PRAGMA busy_timeout`` — see ``try_intercept_busy_timeout`` below
   for the rationale (authorizer rejection of the canonical SQLite
   knob).

2. Bare ``BEGIN`` (and ``BEGIN TRANSACTION``) — rewritten to
   ``BEGIN IMMEDIATE`` when the connection's ``session_mode`` is
   ``"immediate"`` (the default). The writer-lock is acquired at
   BEGIN time so the SELECT-then-INSERT pattern cannot be overtaken
   by a concurrent committer (the ``SQLITE_BUSY_SNAPSHOT`` race).
   dqlite-server's own VFS recommends ``BEGIN IMMEDIATE`` for
   write-bearing transactions. Pass-through for ``BEGIN DEFERRED``
   / ``BEGIN IMMEDIATE`` / ``BEGIN EXCLUSIVE`` (caller intent
   already correct).

   Other session modes (``"deferred"`` / ``"exclusive"`` /
   ``"read_only"``) leave bare ``BEGIN`` untouched — the SQLite
   engine treats it as DEFERRED, which is the right semantic for
   read-only sessions and for callers who explicitly want the
   legacy DEFERRED behaviour. The ``"exclusive"`` mode is reached
   via the SA dialect's ``do_begin`` emitting the explicit
   ``BEGIN EXCLUSIVE`` literal, which doesn't match the rewrite
   regex anyway.

   Configuration: ``Connection(session_mode="…")`` kwarg, URL
   ``?session_mode=…``, or ``DQLITE_SESSION_MODE`` env var.

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

  - ``_description = (("busy_timeout", <INTEGER code>, None, None, None, None, None),)``
    (the value is always an integer, so the ``type_code`` is the
    wire-level INTEGER code, which compares equal to ``NUMBER``)
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
from typing import TYPE_CHECKING, Final

from dqlitewire import ValueType

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
    # Round rather than truncate: the timeout is stored in seconds
    # (``new_ms / 1000.0``), and a plain ``int()`` of ``seconds * 1000``
    # lands at ``N - 1`` for many values because ``N / 1000.0`` is not
    # exactly representable. ``round()`` round-trips the value the caller
    # set, matching stdlib ``sqlite3``'s exact ``PRAGMA busy_timeout``
    # echo.
    current_ms = round(connection._busy_timeout * 1000)
    # The value is always an ``int``, so report the wire-level INTEGER
    # type code (which compares equal to the ``NUMBER`` Type Object) as
    # the column ``type_code`` — matching the contract the normal wire
    # path honours for every column. Emitting ``None`` here would be the
    # one synthetic result that breaks ``description[i][1] == NUMBER``
    # introspection.
    cursor._description = (("busy_timeout", int(ValueType.INTEGER), None, None, None, None, None),)
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
# per-session ``dqlite_session_mode`` opt-out) has stated explicit
# intent. The bare ``BEGIN`` form is what SA emits by default
# and what most callers reach for; it is the only ambiguous
# shape that benefits from the writer-safe upgrade.
_BEGIN_REWRITE_RE = re.compile(
    r"^\s*BEGIN(?:\s+TRANSACTION)?\s*;?\s*$",
    re.IGNORECASE,
)

# Recognised session-mode values. The dbapi layer accepts these as
# the ``session_mode`` kwarg / URL form / env-var value, the SA
# dialect accepts the same set as ``dqlite_session_mode``
# execution-option values, and the cursor's BEGIN-rewrite intercept
# reads ``conn._dqlite_session_mode`` against this set.
#
# - ``"immediate"`` (default): bare ``BEGIN`` is rewritten to
#   ``BEGIN IMMEDIATE`` — writer-safe, no SQLITE_BUSY_SNAPSHOT race.
# - ``"deferred"``: legacy SQLite DEFERRED — bare ``BEGIN`` passes
#   through unchanged. Read-only sessions that want to avoid the
#   writer-lock serialisation tax can opt in.
# - ``"exclusive"``: dialect emits the explicit ``BEGIN EXCLUSIVE``
#   literal for stronger lock semantics.
# - ``"read_only"``: bare ``BEGIN`` passes through (DEFERRED form)
#   AND ``PRAGMA query_only = 1`` is set on the connection so the
#   engine rejects every write at PREPARE with SQLITE_READONLY.
_SESSION_MODE_VALUES: Final[frozenset[str]] = frozenset(
    {"immediate", "deferred", "exclusive", "read_only"}
)

# Env-var default for the session mode. Read at module-import time;
# explicit ``connect(..., session_mode=...)`` always wins. Missing or
# empty → ``"immediate"`` (the writer-safe default). Any value
# outside ``_SESSION_MODE_VALUES`` raises at validation; here we just
# normalise and return the raw string.
_DQLITE_SESSION_MODE_ENV: Final[str] = os.environ.get("DQLITE_SESSION_MODE", "").strip().lower()


def session_mode_default_from_env() -> str:
    """Return the env-var-derived default for the connection's
    session mode.

    Used by ``Connection.__init__`` when the caller does not pass an
    explicit ``session_mode`` kwarg. Re-read at each call so test
    fixtures that ``monkeypatch.setenv`` see the live value (the
    module-level cache is only used as the fallback when the env var
    was set at import time).

    Missing / empty env var → ``"immediate"`` (writer-safe default).
    Invalid value raises ``ValueError`` so misconfiguration surfaces
    at construct time, not at first ``do_begin``.
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
    """Coerce + validate a ``session_mode`` value.

    Returns the canonical lowercase form. Raises ``ValueError`` for
    anything outside ``_SESSION_MODE_VALUES`` so misconfiguration
    surfaces at the boundary (constructor or
    ``execution_options(...)`` call), not at the first ``do_begin``.

    Accepts ``None`` as "use the env-var default" (caller is
    responsible for substituting via ``session_mode_default_from_env``
    when they want that semantics).
    """
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
    """Return ``"BEGIN IMMEDIATE"`` if ``statement`` is a plain BEGIN
    form AND the connection's session mode is ``"immediate"``, else
    ``None`` (caller leaves the SQL unchanged).

    The rewrite fires only when ``session_mode == "immediate"`` — the
    writer-safe default. Other modes (``"deferred"``, ``"exclusive"``,
    ``"read_only"``) leave bare ``BEGIN`` untouched:

    - ``"deferred"`` / ``"read_only"``: the SQLite engine interprets
      bare ``BEGIN`` as DEFERRED, which is what these modes want.
      Rewriting to IMMEDIATE would take a useless writer-lock.
    - ``"exclusive"``: the SA dialect's ``do_begin`` emits the
      explicit ``BEGIN EXCLUSIVE`` literal, which doesn't match the
      bare-BEGIN regex anyway. The exclusive case never reaches this
      function with a bare BEGIN under normal SA usage.

    A ``None`` return also covers the case where ``statement`` is not
    a recognised plain-BEGIN form (e.g. explicit ``BEGIN IMMEDIATE``
    / ``BEGIN DEFERRED`` / ``BEGIN EXCLUSIVE`` / non-BEGIN statement).
    Caller passes the original SQL through unchanged.
    """
    if session_mode != "immediate":
        return None
    if not isinstance(statement, str):
        return None
    if _BEGIN_REWRITE_RE.match(statement) is None:
        return None
    return "BEGIN IMMEDIATE"
