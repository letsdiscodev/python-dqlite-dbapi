"""Pin: PEP 249 §6.5 type objects (``STRING``, ``BINARY``, ``NUMBER``,
``DATETIME``, ``ROWID``) are ``Final``-annotated.

Workspace ``Final`` discipline established by the
``__version__``-Final precedent: every workspace-public module-level
constant gets a ``Final`` annotation so mypy strict catches
re-binding. The PEP 249 type-object family is the most consequential
re-bind hazard — these are the protocol's class-level comparators
against ``Cursor.description[i].type_code``.
"""


def test_pep249_type_objects_are_final_annotated() -> None:
    import dqlitedbapi.types as T

    annotations = T.__annotations__
    for name in ("STRING", "BINARY", "NUMBER", "DATETIME", "ROWID"):
        ann = annotations.get(name)
        assert ann is not None, (
            f"dqlitedbapi.types.{name} must be Final-annotated; "
            f"workspace discipline mirrors __version__-Final precedent."
        )
        # ann is ``Final[_DBAPIType]`` (a parameterised Final, equality
        # via origin/__args__ comparison).
        # Quick acceptance: the str repr starts with "typing.Final[".
        assert "Final[" in str(ann), (
            f"dqlitedbapi.types.{name} annotation is not Final[...] — got {ann!r}"
        )


def test_pep249_type_objects_runtime_values_unchanged() -> None:
    """Belt-and-braces: Final adds no runtime semantics; the type
    objects compare equal to their pre-Final selves."""
    import dqlitedbapi.types as T
    from dqlitewire.constants import ValueType

    # Spot-check identity-by-comparison.
    assert T.STRING == "TEXT"
    assert T.BINARY == ValueType.BLOB
    assert T.NUMBER == ValueType.INTEGER
    assert T.DATETIME == "TIMESTAMP"
    assert T.ROWID == ValueType.INTEGER
