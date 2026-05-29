"""Uppercase SQLITE_VERSION[_INFO] stay private; only the lowercase PEP 249 pair is public."""

from __future__ import annotations


def test_sqlite_version_uppercase_not_public_on_dbapi() -> None:
    import dqlitedbapi

    assert not hasattr(dqlitedbapi, "SQLITE_VERSION"), (
        "SQLITE_VERSION must be private (use sqlite_version)"
    )
    assert not hasattr(dqlitedbapi, "SQLITE_VERSION_INFO"), (
        "SQLITE_VERSION_INFO must be private (use sqlite_version_info)"
    )


def test_sqlite_version_uppercase_not_public_on_aio() -> None:
    from dqlitedbapi import aio as aio_mod

    assert not hasattr(aio_mod, "SQLITE_VERSION"), (
        "SQLITE_VERSION must be private (use sqlite_version)"
    )
    assert not hasattr(aio_mod, "SQLITE_VERSION_INFO"), (
        "SQLITE_VERSION_INFO must be private (use sqlite_version_info)"
    )


def test_lowercase_public_form_remains() -> None:
    import dqlitedbapi
    from dqlitedbapi import aio as aio_mod

    assert isinstance(dqlitedbapi.sqlite_version, str)
    assert isinstance(dqlitedbapi.sqlite_version_info, tuple)
    assert isinstance(aio_mod.sqlite_version, str)
    assert isinstance(aio_mod.sqlite_version_info, tuple)
    assert dqlitedbapi.sqlite_version == aio_mod.sqlite_version
    assert dqlitedbapi.sqlite_version_info == aio_mod.sqlite_version_info
