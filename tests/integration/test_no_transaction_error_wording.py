"""Pin the actual ``"no transaction is active"`` wording the dqlite server emits.

``Connection.commit`` / ``Connection.rollback`` swallow ``OperationalError``
when the message matches a substring whitelist. The substring approach is
brittle by design — string matching across SQLite/dqlite version
boundaries is fragile. This test runs ``COMMIT`` / ``ROLLBACK`` directly
through a cursor (bypassing ``Connection.commit``'s swallow) and pins the
exact (code, message) tuple the server emits today.

If the wording drifts (capitalisation, punctuation, locale), the test
fails — surface the change in CI rather than have ``conn.commit()``
silently start raising in production. Do *not* paper over a wording
change by adjusting the substring; raise a follow-up issue capturing the
new wording.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator, Generator

import pytest

import dqlitedbapi
from dqlitedbapi.aio import aconnect
from dqlitedbapi.exceptions import OperationalError


@pytest.fixture
def conn() -> Generator[dqlitedbapi.Connection]:
    address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:19001")
    c = dqlitedbapi.connect(address, timeout=5.0)
    try:
        yield c
    finally:
        c.close()


@pytest.fixture
async def aconn() -> AsyncGenerator[dqlitedbapi.aio.AsyncConnection]:
    address = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:19001")
    c = await aconnect(address, timeout=5.0)
    try:
        yield c
    finally:
        await c.close()


@pytest.mark.integration
class TestNoTransactionErrorWording:
    def test_stray_commit_emits_known_error(self, conn: dqlitedbapi.Connection) -> None:
        cur = conn.cursor()
        try:
            with pytest.raises(OperationalError) as ei:
                cur.execute("COMMIT")
            assert ei.value.code == 1, f"unexpected code: {ei.value.code}"
            msg = str(ei.value).lower()
            assert "no transaction is active" in msg, f"unexpected msg: {ei.value!s}"
        finally:
            cur.close()

    def test_stray_rollback_emits_known_error(self, conn: dqlitedbapi.Connection) -> None:
        cur = conn.cursor()
        try:
            with pytest.raises(OperationalError) as ei:
                cur.execute("ROLLBACK")
            assert ei.value.code == 1, f"unexpected code: {ei.value.code}"
            msg = str(ei.value).lower()
            assert "no transaction is active" in msg, f"unexpected msg: {ei.value!s}"
        finally:
            cur.close()

    def test_commit_swallows_no_tx_via_connection_method(
        self, conn: dqlitedbapi.Connection
    ) -> None:
        # Force the connection to actually open the underlying socket.
        conn.connect()
        # conn.commit() (not cur.execute('COMMIT')) silently succeeds for a
        # never-begun tx.
        conn.commit()

    def test_rollback_swallows_no_tx_via_connection_method(
        self, conn: dqlitedbapi.Connection
    ) -> None:
        conn.connect()
        conn.rollback()


@pytest.mark.integration
class TestAsyncNoTransactionErrorWording:
    async def test_stray_commit_emits_known_error(
        self, aconn: dqlitedbapi.aio.AsyncConnection
    ) -> None:
        cur = aconn.cursor()
        try:
            with pytest.raises(OperationalError) as ei:
                await cur.execute("COMMIT")
            assert ei.value.code == 1, f"unexpected code: {ei.value.code}"
            msg = str(ei.value).lower()
            assert "no transaction is active" in msg, f"unexpected msg: {ei.value!s}"
        finally:
            await cur.close()

    async def test_stray_rollback_emits_known_error(
        self, aconn: dqlitedbapi.aio.AsyncConnection
    ) -> None:
        cur = aconn.cursor()
        try:
            with pytest.raises(OperationalError) as ei:
                await cur.execute("ROLLBACK")
            assert ei.value.code == 1, f"unexpected code: {ei.value.code}"
            msg = str(ei.value).lower()
            assert "no transaction is active" in msg, f"unexpected msg: {ei.value!s}"
        finally:
            await cur.close()

    async def test_commit_swallows_no_tx_via_connection_method(
        self, aconn: dqlitedbapi.aio.AsyncConnection
    ) -> None:
        await aconn.connect()
        await aconn.commit()

    async def test_rollback_swallows_no_tx_via_connection_method(
        self, aconn: dqlitedbapi.aio.AsyncConnection
    ) -> None:
        await aconn.connect()
        await aconn.rollback()
