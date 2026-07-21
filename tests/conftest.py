import os
import sys

# Make scripts/ importable as top-level modules in tests (e.g. `enrich_targets`).
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import pytest  # noqa: E402

import discovery  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_discovery_providers():
    """Drop the cached discovery-provider singletons (and their lazily-loaded
    gate metadata) before and after every test, so provider state never leaks."""
    discovery._reset_discovery_providers()
    yield
    discovery._reset_discovery_providers()
