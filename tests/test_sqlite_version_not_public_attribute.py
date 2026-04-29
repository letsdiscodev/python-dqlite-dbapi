"""Pin: ``SQLITE_VERSION`` and ``SQLITE_VERSION_INFO`` are NOT
public attributes of ``dqlitedbapi`` or ``dqlitedbapi.aio``.

The PEP 249 documented public surface for the SQLite version is the
lowercase pair (``sqlite_version`` / ``sqlite_version_info``). The
uppercase pair is internal plumbing — re-exported from a private
``_constants`` module — and must not be accessible on the package
namespace.

Without aliasing the imports as ``_SQLITE_VERSION`` /
``_SQLITE_VERSION_INFO``, both names leak as accidentally public
attributes; users could rely on them and a future refactor would
silently break their code.
"""

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
    """Sanity: the PEP 249 public surface still works."""
    import dqlitedbapi
    from dqlitedbapi import aio as aio_mod

    assert isinstance(dqlitedbapi.sqlite_version, str)
    assert isinstance(dqlitedbapi.sqlite_version_info, tuple)
    assert isinstance(aio_mod.sqlite_version, str)
    assert isinstance(aio_mod.sqlite_version_info, tuple)
    # Match between aio and sync.
    assert dqlitedbapi.sqlite_version == aio_mod.sqlite_version
    assert dqlitedbapi.sqlite_version_info == aio_mod.sqlite_version_info
