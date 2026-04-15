"""Test that __version__ is in __all__."""

import dqlitedbapi


class TestVersionExport:
    def test_version_in_all(self) -> None:
        assert "__version__" in dqlitedbapi.__all__

    def test_version_is_string(self) -> None:
        assert isinstance(dqlitedbapi.__version__, str)
