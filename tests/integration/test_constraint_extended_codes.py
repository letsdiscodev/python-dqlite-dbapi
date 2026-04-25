"""Server-side constraint extended codes survive wire → client → dbapi.

The unit-level matrix in ``test_error_code_mapping.py`` exercises
``_classify_operational`` in isolation against constructed exceptions
with extended codes (e.g., ``19 | (5 << 8) = 1299`` for
``SQLITE_CONSTRAINT_NOTNULL``). The matrix does NOT exercise the
end-to-end wire path, so a regression at the wire / client layer that
stripped the high byte before reaching the dbapi would leave the
matrix passing while the live exception lost its extended-code shape.

These integration tests provoke real server-side constraint violations
through the wire, then assert the surfaced ``IntegrityError`` carries
both the masked primary (``code & 0xFF == 19``) AND a non-zero high
byte — the latter being the diagnostic signal that the extended code
made it through.
"""

from __future__ import annotations

import pytest

import dqlitedbapi
from dqlitedbapi import IntegrityError
from dqlitedbapi.aio import aconnect


def _assert_extended_constraint_code(exc: IntegrityError) -> None:
    """A real SQLITE_CONSTRAINT_* extended code has primary 19 and a
    non-zero high byte. Asserting both pins the wire round-trip."""
    code = getattr(exc, "code", None)
    assert code is not None, f"no code attribute on IntegrityError: {exc!r}"
    assert code & 0xFF == 19, f"primary code is not SQLITE_CONSTRAINT (19): {code}"
    high = code >> 8
    assert high > 0, f"extended code high byte is zero (was masked away?): {code}"


@pytest.mark.integration
def test_unique_violation_carries_extended_code(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS integ_ext_unique")
        cur.execute("CREATE TABLE integ_ext_unique (id INTEGER PRIMARY KEY, x TEXT UNIQUE)")
        cur.execute("INSERT INTO integ_ext_unique (x) VALUES ('a')")
        with pytest.raises(IntegrityError) as exc_info:
            cur.execute("INSERT INTO integ_ext_unique (x) VALUES ('a')")
        _assert_extended_constraint_code(exc_info.value)
        conn.rollback()


@pytest.mark.integration
def test_not_null_violation_carries_extended_code(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS integ_ext_notnull")
        cur.execute("CREATE TABLE integ_ext_notnull (id INTEGER PRIMARY KEY, x TEXT NOT NULL)")
        with pytest.raises(IntegrityError) as exc_info:
            cur.execute("INSERT INTO integ_ext_notnull (x) VALUES (NULL)")
        _assert_extended_constraint_code(exc_info.value)
        conn.rollback()


@pytest.mark.integration
def test_check_constraint_carries_extended_code(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS integ_ext_check")
        cur.execute(
            "CREATE TABLE integ_ext_check (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))"
        )
        with pytest.raises(IntegrityError) as exc_info:
            cur.execute("INSERT INTO integ_ext_check (n) VALUES (-1)")
        _assert_extended_constraint_code(exc_info.value)
        conn.rollback()


@pytest.mark.integration
def test_primary_key_violation_carries_extended_code(cluster_address: str) -> None:
    with dqlitedbapi.connect(cluster_address) as conn:
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS integ_ext_pk")
        cur.execute("CREATE TABLE integ_ext_pk (id INTEGER PRIMARY KEY)")
        cur.execute("INSERT INTO integ_ext_pk (id) VALUES (1)")
        with pytest.raises(IntegrityError) as exc_info:
            cur.execute("INSERT INTO integ_ext_pk (id) VALUES (1)")
        _assert_extended_constraint_code(exc_info.value)
        conn.rollback()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_async_unique_violation_carries_extended_code(cluster_address: str) -> None:
    conn = await aconnect(cluster_address)
    try:
        cur = conn.cursor()
        await cur.execute("DROP TABLE IF EXISTS integ_ext_unique_async")
        await cur.execute(
            "CREATE TABLE integ_ext_unique_async (id INTEGER PRIMARY KEY, x TEXT UNIQUE)"
        )
        await cur.execute("INSERT INTO integ_ext_unique_async (x) VALUES ('a')")
        with pytest.raises(IntegrityError) as exc_info:
            await cur.execute("INSERT INTO integ_ext_unique_async (x) VALUES ('a')")
        _assert_extended_constraint_code(exc_info.value)
        await conn.rollback()
    finally:
        await conn.close()
