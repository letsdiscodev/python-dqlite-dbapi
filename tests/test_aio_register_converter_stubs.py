"""Async surface re-exports NotSupportedError stubs so a cross-driver caller gets
``dbapi.NotSupportedError``, not an ``AttributeError`` outside the Error hierarchy."""

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
