"""Pin: ``dqlitedbapi.aio.__version__`` is annotated ``Final[str]``.

The sync sibling at ``dqlitedbapi.__init__`` is ``Final[str]``;
``Final`` does not propagate through ``from X import Y`` aliases, so
the aio sub-package's re-export needs its own annotation. Mirrors
the workspace's ``__version__``-Final discipline applied across the
four sibling packages.
"""

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
