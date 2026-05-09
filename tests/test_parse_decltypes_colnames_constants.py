"""Pin: ``PARSE_DECLTYPES`` and ``PARSE_COLNAMES`` are exposed as
module-level constants on both the sync and async dbapi surfaces with
the same literal values stdlib ``sqlite3`` uses (1 and 2).

The ``detect_types=`` connect kwarg is still rejected by the
``unknown_kwargs`` gate (no converter machinery in this driver); the
constants are exposed so cross-driver porting code that references
the names doesn't hit ``AttributeError`` outside the dbapi error
hierarchy.
"""

from __future__ import annotations

import sqlite3

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.exceptions import NotSupportedError


def test_sync_module_exposes_parse_decltypes() -> None:
    assert dqlitedbapi.PARSE_DECLTYPES == 1
    assert dqlitedbapi.PARSE_DECLTYPES == sqlite3.PARSE_DECLTYPES


def test_sync_module_exposes_parse_colnames() -> None:
    assert dqlitedbapi.PARSE_COLNAMES == 2
    assert dqlitedbapi.PARSE_COLNAMES == sqlite3.PARSE_COLNAMES


def test_async_module_exposes_parse_decltypes() -> None:
    assert dqlitedbapi.aio.PARSE_DECLTYPES == 1


def test_async_module_exposes_parse_colnames() -> None:
    assert dqlitedbapi.aio.PARSE_COLNAMES == 2


def test_parse_constants_in_sync_all() -> None:
    assert "PARSE_DECLTYPES" in dqlitedbapi.__all__
    assert "PARSE_COLNAMES" in dqlitedbapi.__all__


def test_parse_constants_in_async_all() -> None:
    assert "PARSE_DECLTYPES" in dqlitedbapi.aio.__all__
    assert "PARSE_COLNAMES" in dqlitedbapi.aio.__all__


def test_detect_types_still_rejected_at_connect_kwarg_gate() -> None:
    """Exposing the constants doesn't change the connect-time rejection."""
    with pytest.raises(NotSupportedError, match="detect_types"):
        dqlitedbapi.connect(
            "127.0.0.1:9999",
            detect_types=dqlitedbapi.PARSE_DECLTYPES,
        )


def test_detect_types_still_rejected_at_aconnect_kwarg_gate() -> None:
    with pytest.raises(NotSupportedError, match="detect_types"):
        dqlitedbapi.aio.connect(
            "127.0.0.1:9999",
            detect_types=dqlitedbapi.aio.PARSE_COLNAMES,
        )
