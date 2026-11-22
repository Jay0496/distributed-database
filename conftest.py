import os
import sys

ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


import pytest


@pytest.fixture
def db_path(tmp_path):
    """Convenience path for a temporary SQLite database file."""
    return str(tmp_path / "test.db")
