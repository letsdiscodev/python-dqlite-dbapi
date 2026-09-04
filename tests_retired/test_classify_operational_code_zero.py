"""``_classify_operational(0)`` falls through ``_CODE_TO_EXCEPTION`` to
the default ``OperationalError`` (upstream emits code 0 "empty
statement" on empty/comment-only SQL)."""

from __future__ import annotations

from dqlitedbapi.cursor import _classify_operational
from dqlitedbapi.exceptions import OperationalError


def test_classify_operational_code_zero_returns_operational_error_class() -> None:
    cls = _classify_operational(0)
    assert cls is OperationalError


def test_classify_operational_code_none_returns_operational_error_class() -> None:
    """Wire-decode / ProtocolError-wrapped errors carry code=None and
    fall through to OperationalError too."""
    cls = _classify_operational(None)
    assert cls is OperationalError
