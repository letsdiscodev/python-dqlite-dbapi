"""Pin: ``_classify_operational(0)`` routes ``FailureResponse(code=0,
"empty statement")`` to a clean ``OperationalError``.

Upstream's gateway emits ``failure(req, 0, "empty statement")`` on
empty / comment-only SQL; the wire layer accepts the code (after the
fix). The dbapi classifier dispatches by primary code via the
``_CODE_TO_EXCEPTION`` table; code 0 falls through to the default
``OperationalError`` class and is exposed to the user with the
original message preserved.
"""

from __future__ import annotations

from dqlitedbapi.cursor import _classify_operational
from dqlitedbapi.exceptions import OperationalError


def test_classify_operational_code_zero_returns_operational_error_class() -> None:
    """Code 0 is not mapped in _CODE_TO_EXCEPTION; the classifier
    falls through to the default OperationalError class."""
    cls = _classify_operational(0)
    assert cls is OperationalError


def test_classify_operational_code_none_returns_operational_error_class() -> None:
    """Wire-decode / ProtocolError-wrapped errors carry code=None
    and fall through to OperationalError too."""
    cls = _classify_operational(None)
    assert cls is OperationalError
