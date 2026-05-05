"""Pin: dbapi ``Error`` and its subclasses preserve ``raw_message``
and ``code`` across pickle / deepcopy. Without ``__reduce__``,
SA's ``is_disconnect`` (which reads ``raw_message`` first) would
silently miss "wire decode failed" / "timed out" past the byte-1024
boundary on cross-process error capture (Celery, multiprocessing
pool).
"""

import copy
import pickle

import pytest

import dqlitedbapi
from dqlitedbapi.exceptions import (
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)


@pytest.mark.parametrize(
    "cls",
    [
        OperationalError,
        IntegrityError,
        DatabaseError,
        DataError,
        InternalError,
        ProgrammingError,
    ],
)
def test_error_pickle_preserves_raw_message_and_code(cls: type) -> None:
    e = cls("truncated msg", 42, raw_message="full server text " * 100)
    blob = pickle.dumps(e)
    restored = pickle.loads(blob)
    assert restored.raw_message == e.raw_message
    assert restored.code == 42
    assert isinstance(restored, cls)


def test_not_supported_error_pickle_round_trip() -> None:
    """``NotSupportedError`` takes a single message arg (no code, no
    raw_message) — pin the simple shape."""
    e = NotSupportedError("dqlite has no foo")
    restored = pickle.loads(pickle.dumps(e))
    assert isinstance(restored, NotSupportedError)
    assert str(restored) == str(e)


def test_warning_pickle_round_trip() -> None:
    """``Warning`` is the PEP 249 sibling of ``Error`` (NOT a
    subclass) — pin the round-trip independently."""
    e = dqlitedbapi.Warning("truncation notice")
    restored = pickle.loads(pickle.dumps(e))
    assert isinstance(restored, dqlitedbapi.Warning)
    assert str(restored) == str(e)


def test_interface_error_pickle_preserves_raw_message_and_code() -> None:
    e = InterfaceError("wire problem", 1001, raw_message="DQLITE_PROTO ...")
    blob = pickle.dumps(e)
    restored = pickle.loads(blob)
    assert restored.raw_message == "DQLITE_PROTO ..."
    assert restored.code == 1001


def test_error_deepcopy_preserves_raw_message_and_code() -> None:
    """deepcopy uses the same ``__reduce__`` path; pin too."""
    e = OperationalError("msg", 10, raw_message="raw text")
    e2 = copy.deepcopy(e)
    assert e2.raw_message == "raw text"
    assert e2.code == 10
