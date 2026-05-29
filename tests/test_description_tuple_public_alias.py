"""``DescriptionTuple`` is public on the ``types`` module and re-exported from the
package root, so downstream typed wrappers can consume it without an underscore sibling.
"""

from __future__ import annotations

import dqlitedbapi
from dqlitedbapi import types as dqlite_types


def test_description_tuple_is_publicly_exported_from_types_module() -> None:
    assert hasattr(dqlite_types, "DescriptionTuple")


def test_description_tuple_is_in_types_module_all() -> None:
    assert "DescriptionTuple" in dqlite_types.__all__


def test_description_tuple_is_publicly_exported_from_package_root() -> None:
    assert hasattr(dqlitedbapi, "DescriptionTuple")
    assert dqlitedbapi.DescriptionTuple is dqlite_types.DescriptionTuple
    assert "DescriptionTuple" in dqlitedbapi.__all__


def test_description_tuple_is_pep_695_type_alias() -> None:
    """Public type aliases use PEP 695 ``type X = ...`` (a ``TypeAliasType``)."""
    import typing

    assert isinstance(dqlite_types.DescriptionTuple, typing.TypeAliasType), (
        "DescriptionTuple must be declared as 'type DescriptionTuple = ...' "
        "to match the workspace's PEP 695 discipline"
    )
