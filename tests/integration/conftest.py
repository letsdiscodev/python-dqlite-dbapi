"""Integration test fixtures for dqlite-dbapi."""

import os

import pytest

DQLITE_TEST_CLUSTER = os.environ.get("DQLITE_TEST_CLUSTER", "localhost:9001")


@pytest.fixture
def cluster_address() -> str:
    return DQLITE_TEST_CLUSTER
