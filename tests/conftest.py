"""Shared fixtures for tests."""
import os
import sys
from pathlib import Path

import pytest

print("CONFTEST LOADED")


@pytest.fixture(scope="session")
def root_dir():
    """Root tm2py directory."""
    return Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(scope="session")
def examples_dir(root_dir):
    """Directory for example files."""
    return root_dir / "examples"


@pytest.fixture(scope="session")
def bin_dir(root_dir):
    """Directory for bin files."""
    return root_dir / "bin"


@pytest.fixture()
def temp_dir():
    """Create a temporary directory and clean it up upon test completion.

    Yields:
        Path: Path object of temporary directory location
    """
    import tempfile

    tf = tempfile.TemporaryDirectory()
    yield tf.name
    tf.cleanup()


@pytest.fixture(scope="session")
def inro_context():
    """Mocks necessary inro modules if they aren't successfully imported."""
    from tm2py.tools import mocked_inro_context

    try:
        import inro.emme.database.emmebank

        yield "inro"
    except ModuleNotFoundError:
        mocked_inro_context()
        yield "mocked"
