"""Pin: ``dqlitedbapi.aio.__version__`` needs its own ``Final[str]`` (a
from-import alias does not inherit ``Final`` from the source)."""

from typing import Final


def test_aio_version_is_final_annotated() -> None:
    import dqlitedbapi.aio as aio

    assert aio.__annotations__.get("__version__") == Final[str], (
        "dqlitedbapi.aio.__version__ must be re-annotated Final[str] — "
        "the from-import alias does not inherit Final from the source."
    )


def test_aio_version_matches_sync_sibling() -> None:
    import dqlitedbapi
    import dqlitedbapi.aio

    assert dqlitedbapi.aio.__version__ == dqlitedbapi.__version__
