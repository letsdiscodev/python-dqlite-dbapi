"""Test that __version__ is in __all__."""

import dqlitedbapi
from dqlitedbapi import aio


class TestVersionExport:
    def test_version_in_all(self) -> None:
        assert "__version__" in dqlitedbapi.__all__

    def test_version_is_string(self) -> None:
        assert isinstance(dqlitedbapi.__version__, str)

    def test_aio_module_exports_version(self) -> None:
        assert hasattr(aio, "__version__")
        assert aio.__version__ == dqlitedbapi.__version__

    def test_aio_version_in_all(self) -> None:
        assert "__version__" in aio.__all__
