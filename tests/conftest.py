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
def inro_context():
    """Mocks necessary inro modules if they aren't successfully imported."""
    from tm2py.tools import mocked_inro_context

    try:
        import inro.emme.database.emmebank

        yield "inro"
    except ModuleNotFoundError:
        mocked_inro_context()
        yield "mocked"

@pytest.fixture(autouse=True)
def skip_by_context(inro_context):
    if inro_context != "inro":
        pytest.skip('skipped without full inro: {}'.format(inro_context))   