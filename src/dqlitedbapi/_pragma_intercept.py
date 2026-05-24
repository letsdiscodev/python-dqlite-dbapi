"""``PRAGMA busy_timeout`` interception at the dbapi cursor layer.

dqlite's C VFS authorizer rejects ``PRAGMA busy_timeout`` (see prior
issue ``done/dbapi-pragma-deny-list-no-regression-pin.md``), so the
canonical SQLite escape hatch — ``PRAGMA busy_timeout = N`` — would
otherwise raise ``DatabaseError("not authorized")`` on the dqlite
server. To preserve stdlib parity (and to give callers a single
canonical knob), this module intercepts the PRAGMA at the dbapi
cursor layer BEFORE the wire round-trip:

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
"""

from __future__ import annotations

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
