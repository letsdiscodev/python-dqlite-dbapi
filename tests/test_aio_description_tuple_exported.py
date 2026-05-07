"""Pin: ``DescriptionTuple`` is exported from ``dqlitedbapi.aio``,
matching the sync ``dqlitedbapi`` surface.

The async surface re-exports the type aliases the sync surface
exports (Date / Time / Binary / STRING / etc.). ``DescriptionTuple``
was promoted from ``_DescriptionTuple`` to public in a prior
round but the async-side ``__all__`` was missed.
"""

from __future__ import annotations

import dqlitedbapi
import dqlitedbapi.aio
from dqlitedbapi.types import DescriptionTuple


def test_async_surface_exports_description_tuple_in_all() -> None:
    assert "DescriptionTuple" in dqlitedbapi.aio.__all__


def test_async_surface_description_tuple_resolves_to_canonical() -> None:
    assert dqlitedbapi.aio.DescriptionTuple is DescriptionTuple
    assert dqlitedbapi.aio.DescriptionTuple is dqlitedbapi.DescriptionTuple
