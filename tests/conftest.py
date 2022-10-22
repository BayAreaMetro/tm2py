"""Shared fixtures for tests."""
import os
import sys
from pathlib import Path

import pytest

print("CONFTEST LOADED")


@pytest.fixture(scope="session")
def root_dir():
    """Root tm2py directory."""
    d = os.path.dirname(os.path.abspath(__file__))
    for i in range(3):
        if "examples" in os.listdir(d):
            return Path(d)
        d = os.path.dirname(d)


@pytest.fixture(scope="session")
def examples_dir(root_dir):
    """Directory for example files."""
    return root_dir / "examples"


@pytest.fixture(scope="session")
def bin_dir(root_dir):
    """Directory for bin files."""
    return root_dir / "bin"


# todo: why not use the existing tmp_path fixture?
# https://docs.pytest.org/en/7.1.x/how-to/tmp_path.html
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
def inro_context(pytestconfig):
    """Mocks necessary inro modules if they aren't successfully imported."""

    try:
        _inro = pytestconfig.getoption("inro")
        if _inro.lower() == "mock":
            print("Mocking inro environment.")
            mocked_inro_context()
        else:
            import inro.emme.database.emmebank

            print("Using inro environment.")
    except:
        try:
            import inro.emme.database.emmebank

            print("Using inro environment.")
        except ModuleNotFoundError:
            print("Mocking inro environment.")
            mocked_inro_context()
