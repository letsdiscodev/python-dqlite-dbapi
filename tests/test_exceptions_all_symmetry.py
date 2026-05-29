"""Pin: ``dqlitedbapi.exceptions.__all__`` covers every public Error/Warning subclass.

``AmbiguousCommitError`` was missing from ``__all__`` and is the regression anchor.
"""

from __future__ import annotations

import dqlitedbapi.exceptions as exc_mod


def test_exceptions_all_includes_every_public_error_class() -> None:
    public_classes = {
        name
        for name in dir(exc_mod)
        if not name.startswith("_")
        and isinstance(getattr(exc_mod, name), type)
        and issubclass(getattr(exc_mod, name), (exc_mod.Error, exc_mod.Warning))
    }
    missing = public_classes - set(exc_mod.__all__)
    assert not missing, (
        f"dqlitedbapi.exceptions.__all__ is missing public Error/Warning "
        f"subclasses: {sorted(missing)}"
    )


def test_ambiguous_commit_error_in_exceptions_all() -> None:
    assert "AmbiguousCommitError" in exc_mod.__all__


def test_wildcard_import_from_exceptions_includes_ambiguous_commit_error() -> None:
    """Wildcard import from ``dqlitedbapi.exceptions`` brings the class into scope."""
    ns: dict[str, object] = {}
    exec("from dqlitedbapi.exceptions import *", ns)
    assert "AmbiguousCommitError" in ns
