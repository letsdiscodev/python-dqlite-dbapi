"""Pin: the async surface re-exports the stdlib-sqlite3-parity
NotSupportedError stubs that the sync surface already provides.

ISSUE-Q8 mirrored ``register_adapter`` to the async surface; this
finishes the symmetry for ``register_converter`` /
``complete_statement`` / ``enable_callback_tracebacks`` so a
cross-driver caller porting from aiosqlite gets a
``dbapi.NotSupportedError`` rather than ``AttributeError``
(which escapes the dbapi.Error hierarchy).
"""

import pytest

import dqlitedbapi.aio
from dqlitedbapi.exceptions import NotSupportedError


def test_aio_register_converter_raises_notsupported() -> None:
    with pytest.raises(NotSupportedError):
        dqlitedbapi.aio.register_converter("DATE", lambda b: b)


def test_aio_complete_statement_raises_notsupported() -> None:
    with pytest.raises(NotSupportedError):
        dqlitedbapi.aio.complete_statement("SELECT 1;")


def test_aio_enable_callback_tracebacks_raises_notsupported() -> None:
    with pytest.raises(NotSupportedError):
        dqlitedbapi.aio.enable_callback_tracebacks(True)


def test_aio_all_includes_the_three_stubs() -> None:
    assert "register_converter" in dqlitedbapi.aio.__all__
    assert "complete_statement" in dqlitedbapi.aio.__all__
    assert "enable_callback_tracebacks" in dqlitedbapi.aio.__all__
