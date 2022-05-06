import os
import sys

import pytest

print("CONFTEST LOADED")

@pytest.fixture(scope="session")
def root_dir():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

@pytest.fixture(scope="session")
def examples_dir(root_dir):
    return os.path.join(root_dir, "examples")

@pytest.fixture(scope="session")
def bin_dir(root_dir):
    return os.path.join(root_dir, "bin")

@pytest.fixture()
def temp_dir(root_dir):
    import tempfile
    return tempfile.TemporaryDirectory()

