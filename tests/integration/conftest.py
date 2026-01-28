"""Integration test fixtures for dqlite-dbapi."""

import os

import pytest

DQLITE_TEST_CLUSTER = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: marks tests as requiring dqlite cluster")


@pytest.fixture
def cluster_address() -> str:
    """Get the test cluster address."""
    return DQLITE_TEST_CLUSTER
