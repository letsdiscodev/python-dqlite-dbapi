"""Statement classification and the driver's SQL rewrites."""

import os
import re
from collections.abc import Sequence, Sized
from dataclasses import dataclass
from typing import Any, Final

from dqliteclient.sql import (
    blank_literals_and_comments,
    leading_keyword,
    split_statements,
    strip_leading_comments,
)
from dqlitedbapi.exceptions import ProgrammingError

SESSION_MODES: Final[frozenset[str]] = frozenset(
    {"immediate", "deferred", "exclusive", "read_only"}
)

_ROW_RETURNING: Final[frozenset[str]] = frozenset({"SELECT", "VALUES", "PRAGMA", "EXPLAIN"})
_DML: Final[frozenset[str]] = frozenset({"INSERT", "UPDATE", "DELETE", "REPLACE"})
_TX_CONTROL: Final[frozenset[str]] = frozenset(
    {"BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE"}
)
_RETURNING_RE: Final = re.compile(r"\bRETURNING\b", re.IGNORECASE)
_BARE_BEGIN_RE: Final = re.compile(r"^\s*BEGIN(?:\s+TRANSACTION)?\s*;?\s*$", re.IGNORECASE)
_BUSY_TIMEOUT_PRAGMA_RE: Final = re.compile(
    r"^\s*PRAGMA\s+busy_timeout\s*(?:=\s*(?P<eq>-?\d+)|\(\s*(?P<paren>-?\d+)\s*\))?\s*;?\s*$",
    re.IGNORECASE,
)
_INT32_MAX: Final[int] = 2**31 - 1


@dataclass(frozen=True, slots=True)
class Statement:
    keyword: str
    """Leading keyword after comments and any CTE prefix, upper-cased."""
    returns_rows: bool
    is_dml: bool
    is_insert: bool
    is_pragma: bool
    is_commit: bool
    is_tx_control: bool


def classify(sql: str) -> Statement:
    blanked = blank_literals_and_comments(sql)
    keyword = leading_keyword(blanked)
    if keyword == "WITH":
        keyword = leading_keyword(_strip_leading_cte(strip_leading_comments(blanked).upper()))
    returns_rows = (
        keyword in _ROW_RETURNING or keyword == "WITH" or _RETURNING_RE.search(blanked) is not None
    )
    return Statement(
        keyword=keyword,
        returns_rows=returns_rows,
        is_dml=keyword in _DML,
        is_insert=keyword in ("INSERT", "REPLACE"),
        is_pragma=keyword == "PRAGMA",
        is_commit=keyword in ("COMMIT", "END"),
        is_tx_control=keyword in _TX_CONTROL,
    )


def _strip_leading_cte(upper: str) -> str:
    """Return the statement following ``WITH [RECURSIVE] name [(cols)] AS (...), ...``;
    the input unchanged if it cannot be parsed."""
    pos = len("WITH")
    pos = _skip_spaces(upper, pos)
    if upper.startswith("RECURSIVE", pos):
        pos = _skip_spaces(upper, pos + len("RECURSIVE"))
    while True:
        as_idx = upper.find(" AS", pos)
        if as_idx == -1 or (as_idx + 3 < len(upper) and upper[as_idx + 3] not in " ("):
            return upper
        body = upper.find("(", as_idx + 3)
        if body == -1:
            return upper
        depth = 1
        i = body + 1
        while i < len(upper) and depth:
            if upper[i] == "(":
                depth += 1
            elif upper[i] == ")":
                depth -= 1
            i += 1
        if depth:
            return upper
        pos = _skip_spaces(upper, i)
        if pos < len(upper) and upper[pos] == ",":
            pos = _skip_spaces(upper, pos + 1)
            continue
        return upper[pos:]


def _skip_spaces(s: str, pos: int) -> int:
    while pos < len(s) and s[pos].isspace():
        pos += 1
    return pos


def validate_operation(sql: object) -> str:
    if not isinstance(sql, str):
        raise ProgrammingError(f"operation must be a str SQL statement, got {type(sql).__name__}")
    if "\x00" in sql:
        raise ProgrammingError("the query contains a null character")
    statements = [s for s in split_statements(sql) if strip_leading_comments(s)]
    if not statements:
        raise ProgrammingError("empty statement")
    if len(statements) > 1:
        raise ProgrammingError("You can only execute one statement at a time.")
    return sql


def validate_parameters(params: object) -> None:
    """Reject parameter containers that are not an ordered, sized sequence."""
    if params is None:
        return
    if isinstance(params, str | bytes | bytearray | memoryview):
        raise ProgrammingError(
            f"parameters must be a sequence of values, not {type(params).__name__!r}; "
            "did you mean a one-element tuple like (value,)?"
        )
    if isinstance(params, dict) or hasattr(params, "keys"):
        raise ProgrammingError(
            "qmark paramstyle requires a sequence; got a mapping. "
            "Use a list or tuple matching the ? placeholders positionally."
        )
    if isinstance(params, set | frozenset):
        raise ProgrammingError(
            "qmark paramstyle requires an ordered sequence; got a set. "
            "Use a list or tuple matching the ? placeholders positionally."
        )
    if not isinstance(params, Sized):
        raise ProgrammingError(
            f"qmark paramstyle requires a sized sequence (tuple/list); got "
            f"{type(params).__name__!r}. Materialise iterators into a list first."
        )


def placeholder_count(sql: str) -> int:
    return blank_literals_and_comments(sql).count("?")


def check_placeholder_count(sql: str, params: Sequence[Any] | None) -> None:
    expected = placeholder_count(sql)
    supplied = 0 if params is None else len(params)
    if expected != supplied:
        raise ProgrammingError(
            f"Incorrect number of bindings supplied. The current statement uses "
            f"{expected}, and there are {supplied} supplied."
        )


def validate_executemany_sequence(seq: object) -> None:
    if seq is None:
        raise ProgrammingError("executemany() seq_of_parameters must be an iterable, not None")
    if isinstance(seq, str | bytes | bytearray | memoryview | set | frozenset) or type(seq) is dict:
        raise ProgrammingError(
            f"executemany seq_of_parameters must be an iterable of parameter sets "
            f"(e.g. a list of tuples), not {type(seq).__name__}"
        )


def rewrite_begin(sql: str, session_mode: str) -> str:
    """Qualify a bare ``BEGIN`` according to the session mode."""
    if _BARE_BEGIN_RE.match(sql) is None:
        return sql
    if session_mode == "immediate":
        return "BEGIN IMMEDIATE"
    if session_mode == "exclusive":
        return "BEGIN EXCLUSIVE"
    return sql


def busy_timeout_pragma(sql: str) -> tuple[bool, int | None]:
    """``(matched, new_ms)`` for ``PRAGMA busy_timeout [= N | (N)]``; ``new_ms`` is
    ``None`` for the getter form. Out-of-int32 values collapse to 0 as in SQLite."""
    m = _BUSY_TIMEOUT_PRAGMA_RE.match(sql)
    if m is None:
        return False, None
    raw = m.group("eq") or m.group("paren")
    if raw is None:
        return True, None
    n = int(raw)
    return True, n if 0 < n <= _INT32_MAX else 0


def validate_session_mode(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError(f"session_mode must be a str, got {type(value).__name__}")
    mode = value.lower()
    if mode not in SESSION_MODES:
        raise ValueError(
            f"Invalid session_mode {value!r}; valid values are {sorted(SESSION_MODES)}"
        )
    return mode


def default_session_mode() -> str:
    """``DQLITE_SESSION_MODE`` from the environment, else ``"immediate"``."""
    raw = os.environ.get("DQLITE_SESSION_MODE", "").strip()
    return validate_session_mode(raw) if raw else "immediate"
