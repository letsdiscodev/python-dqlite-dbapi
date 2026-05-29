"""PEP 249 type objects (STRING/BINARY/NUMBER/DATETIME/ROWID) are Final-annotated."""


def test_pep249_type_objects_are_final_annotated() -> None:
    import dqlitedbapi.types as T

    annotations = T.__annotations__
    for name in ("STRING", "BINARY", "NUMBER", "DATETIME", "ROWID"):
        ann = annotations.get(name)
        assert ann is not None, (
            f"dqlitedbapi.types.{name} must be Final-annotated; "
            f"workspace discipline mirrors __version__-Final precedent."
        )
        assert "Final[" in str(ann), (
            f"dqlitedbapi.types.{name} annotation is not Final[...] — got {ann!r}"
        )


def test_pep249_type_objects_runtime_values_unchanged() -> None:
    """Final adds no runtime semantics; the type objects still compare as before."""
    import dqlitedbapi.types as T
    from dqlitewire.constants import ValueType

    assert T.STRING == "TEXT"
    assert T.BINARY == ValueType.BLOB
    assert T.NUMBER == ValueType.INTEGER
    assert T.DATETIME == "TIMESTAMP"
    assert T.ROWID == ValueType.INTEGER
