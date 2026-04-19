"""Cross-entry-point timeout validation.

All three DB-API entry points — ``dqlitedbapi.connect``,
``dqlitedbapi.aio.connect``, and ``dqlitedbapi.aio.aconnect`` — share a
single ``_validate_timeout`` helper (see ISSUE-203). These tests pin
that each entry point rejects the same set of bad values with the same
error phrasing and exception type.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.exceptions import ProgrammingError

_BAD_TIMEOUTS = (
    -1.0,
    0.0,
    float("inf"),
    float("-inf"),
    float("nan"),
)


@pytest.mark.parametrize("timeout", _BAD_TIMEOUTS)
def test_sync_connect_rejects_bad_timeout(timeout: float) -> None:
    with pytest.raises(ProgrammingError, match="timeout must be a positive finite number"):
        dqlitedbapi.connect("localhost:9001", timeout=timeout)


@pytest.mark.parametrize("timeout", _BAD_TIMEOUTS)
def test_aio_connect_rejects_bad_timeout(timeout: float) -> None:
    with pytest.raises(ProgrammingError, match="timeout must be a positive finite number"):
        dqlitedbapi.aio.connect("localhost:9001", timeout=timeout)


@pytest.mark.parametrize("timeout", _BAD_TIMEOUTS)
async def test_aio_aconnect_rejects_bad_timeout(timeout: float) -> None:
    with pytest.raises(ProgrammingError, match="timeout must be a positive finite number"):
        await dqlitedbapi.aio.aconnect("localhost:9001", timeout=timeout)


def test_error_phrasing_includes_value() -> None:
    """The error repeats the offending value so operators can spot
    typos ("`0.1` vs `0` vs `0,1`") without cross-referencing the
    callsite. Regression guard against accidental phrasing drift.
    """
    with pytest.raises(ProgrammingError) as excinfo:
        dqlitedbapi.connect("localhost:9001", timeout=-3.5)
    assert "-3.5" in str(excinfo.value)
