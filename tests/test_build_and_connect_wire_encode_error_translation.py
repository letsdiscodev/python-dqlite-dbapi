"""Pin: ``_build_and_connect``'s exception chain translates raw wire
``EncodeError`` to a PEP 249 ``DataError``. The cursor-path
``_call_client`` already has the symmetric arm
(``dqlitedbapi/cursor.py``); the connect path was missing the
matching translation, so a NUL byte in ``database=`` propagated as
a raw ``dqlitewire.EncodeError`` past every ``except dbapi.Error:``
block — a PEP 249 §7 violation.

Reproduction shape: stub ``open_database`` on a synthetic protocol
object to raise wire ``EncodeError`` (the same way real
``OpenRequest(name="db\\x00name").encode()`` would). Drive
``_build_and_connect`` and assert the translation lands.
"""

from __future__ import annotations

from typing import Any

import pytest

from dqlitedbapi.connection import _build_and_connect
from dqlitedbapi.exceptions import DataError, Error
from dqlitewire import EncodeError as _WireEncodeError

pytestmark = pytest.mark.asyncio


async def test_build_and_connect_translates_wire_encode_error_to_data_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the underlying ``DqliteConnection.connect()`` raises a
    raw ``dqlitewire.EncodeError`` (e.g., NUL byte in ``database=``
    name), ``_build_and_connect`` must translate to PEP 249
    ``DataError`` so ``except dbapi.Error:`` catches it.
    """
    # Stub _resolve_leader so we don't need a real cluster.
    from dqlitedbapi import connection as _conn_mod

    async def _fake_resolve(*args: Any, **kwargs: Any) -> str:
        return "localhost:9001"

    monkeypatch.setattr(_conn_mod, "_resolve_leader", _fake_resolve)

    # Stub DqliteConnection.connect so it raises a raw wire
    # EncodeError as the real OpenRequest(name="db\x00name").encode()
    # would. We patch the connect() method on instances built by
    # the helper; ``DqliteConnection.__init__`` is fine to run.
    from dqliteclient.connection import DqliteConnection

    async def _raises_wire_encode_error(self: DqliteConnection) -> None:
        raise _WireEncodeError(
            "Text value contains embedded null byte at byte offset 2; "
            "null-terminated encoding would lose data"
        )

    monkeypatch.setattr(DqliteConnection, "connect", _raises_wire_encode_error)

    with pytest.raises(DataError) as excinfo:
        await _build_and_connect(
            "localhost:9001",
            database="db\x00name",
            timeout=5.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=0.5,
        )
    assert "embedded null byte" in str(excinfo.value) or "wire encode" in str(excinfo.value)
    # PEP 249 §7 umbrella catches it
    assert isinstance(excinfo.value, Error)


async def test_build_and_connect_wire_encode_error_caught_by_dbapi_error_umbrella(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: cross-driver ``except dbapi.Error:`` blocks
    must catch the translated error, not the raw wire class.
    """
    from dqliteclient.connection import DqliteConnection
    from dqlitedbapi import connection as _conn_mod

    async def _fake_resolve(*args: Any, **kwargs: Any) -> str:
        return "localhost:9001"

    async def _raises(self: DqliteConnection) -> None:
        raise _WireEncodeError("oversize text payload (5000 > 4096)")

    monkeypatch.setattr(_conn_mod, "_resolve_leader", _fake_resolve)
    monkeypatch.setattr(DqliteConnection, "connect", _raises)

    with pytest.raises(Error):  # umbrella
        await _build_and_connect(
            "localhost:9001",
            database="x" * 5000,
            timeout=5.0,
            max_total_rows=None,
            max_continuation_frames=None,
            trust_server_heartbeat=False,
            close_timeout=0.5,
        )
