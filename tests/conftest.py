"""Shared fixtures for tests."""
import os
import sys
from unittest.mock import MagicMock

import pytest

print("CONFTEST LOADED")


@pytest.fixture(scope="session")
def root_dir():
    """Root tm2py directory."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def examples_dir(root_dir):
    """Directory for example files."""
    return os.path.join(root_dir, "examples")


@pytest.fixture(scope="session")
def bin_dir(root_dir):
    """Directory for bin files."""
    return os.path.join(root_dir, "bin")


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


def mocked_inro_context():
    """Mocking of modules which need to be mocked for tests."""
    sys.modules["inro.emme.database.emmebank"] = MagicMock()
    sys.modules["inro.emme.network"] = MagicMock()
    sys.modules["inro.emme.database.scenario"] = MagicMock()
    sys.modules["inro.emme.database.matrix"] = MagicMock()
    sys.modules["inro.emme.network.node"] = MagicMock()
    sys.modules["inro.emme.desktop.app"] = MagicMock()
    sys.modules["inro"] = MagicMock()
    sys.modules["inro.modeller"] = MagicMock()


@pytest.fixture(scope="session")
def inro_context():
    """Mocks necessary inro modules if they aren't successfully imported."""
    try:
        import inro.emme.database.emmebank

        yield "inro"
    except ModuleNotFoundError:
        mocked_inro_context()
        yield "mocked"
