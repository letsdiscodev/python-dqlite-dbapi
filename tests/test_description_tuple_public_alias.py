"""Pin: ``DescriptionTuple`` is a public alias on the dbapi
``types`` module and re-exported from the package root, so
downstream typed wrappers (``sqlalchemy-dqlite``) can consume it
without importing an underscore-prefixed sibling.
"""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import types as dqlite_types


def test_description_tuple_is_publicly_exported_from_types_module() -> None:
    """The public alias must exist on ``dqlitedbapi.types``."""
    assert hasattr(dqlite_types, "DescriptionTuple")


def test_description_tuple_is_in_types_module_all() -> None:
    """``__all__`` membership is the canonical public-surface marker."""
    assert "DescriptionTuple" in dqlite_types.__all__


def test_description_tuple_is_publicly_exported_from_package_root() -> None:
    """A user can write ``from dqlitedbapi import DescriptionTuple``
    rather than crossing into the ``types`` submodule."""
    assert hasattr(dqlitedbapi, "DescriptionTuple")
    assert dqlitedbapi.DescriptionTuple is dqlite_types.DescriptionTuple
    assert "DescriptionTuple" in dqlitedbapi.__all__
