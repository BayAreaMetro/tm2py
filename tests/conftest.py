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


def pytest_addoption(parser):
    """Parse command line arguments."""
    parser.addoption("--inro", action="store", default="notmock")
    print("pytest_addoption")


def mocked_inro_context():
    import unittest.mock

    """Mocking of modules which need to be mocked for tests."""
    sys.modules["inro.emme.database.emmebank"] = unittest.mock.MagicMock()
    sys.modules["inro.emme.network"] = unittest.mock.MagicMock()
    sys.modules["inro.emme.database.scenario"] = unittest.mock.MagicMock()
    sys.modules["inro.emme.database.matrix"] = unittest.mock.MagicMock()
    sys.modules["inro.emme.network.node"] = unittest.mock.MagicMock()
    sys.modules["inro.emme.desktop.app"] = unittest.mock.MagicMock()
    sys.modules["inro"] = unittest.mock.MagicMock()
    sys.modules["inro.modeller"] = unittest.mock.MagicMock()


@pytest.fixture(scope="session")
def inro_context(pytestconfig):
    """Mocks necessary inro modules if they aren't successfully imported."""

    try:
        # obey command line option
        _inro = pytestconfig.getoption("inro")
        print("_inro = [{}]".format(_inro))
        if _inro.lower() == "mock":
            print("Mocking inro environment.")
            mocked_inro_context()
            return "mock"
        else:
            # why import gdal first: https://github.com/BayAreaMetro/tm2py/blob/7a563f0c5cea2125f28bfaedc50205e70c532094/README.md?plain=1#L57
            import gdal
            import inro.emme.database.emmebank

            print("Using inro environment.")
            return "inro"
    except Exception as inst:
        print(type(inst))  # the exception instance
        print(inst.args)  # arguments stored in .args
        print(inst)  # __str__ allows args to be printed directly,

        # if commandline option fails, try using Emme and then failing that, using Mock
        try:
            # why import gdal first: https://github.com/BayAreaMetro/tm2py/blob/7a563f0c5cea2125f28bfaedc50205e70c532094/README.md?plain=1#L57
            import gdal
            import inro.emme.database.emmebank

            print("Using inro environment.")
            return "inro"
        except ModuleNotFoundError:
            print("Mocking inro environment.")
            mocked_inro_context()
            return "mock"

@pytest.fixture(scope="session")
def ctramp_context(pytestconfig):
    """Identifies if ctramp is available."""
    import subprocess
    try:
        from tm2py.tools import run_process
        commands = [
            "CALL CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\runtime;C:\\Windows\\System32;%JAVA_PATH%\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            'CALL CTRAMP\runtime\runMtxMgr.cmd %HOST_IP_ADDRESS% "%JAVA_PATH%"',
        ]
        run_process(commands, name="start_matrix_manager")

        print("Using ctramp environment.")
        return "ctramp"
    except subprocess.CalledProcessError:
        print("No ctramp environment.")
        return None


@pytest.fixture(scope="session")
def union_city(examples_dir, root_dir, inro_context):
    """Union City model run testing fixture."""
    from tm2py.controller import RunController
    from tm2py.examples import get_example

    EXAMPLE = "UnionCity"
    _example_root = examples_dir / EXAMPLE

    get_example(example_name="UnionCity", root_dir=root_dir)
    controller = RunController(
        [
            os.path.join(examples_dir, "scenario_config.toml"),
            os.path.join(examples_dir, "model_config.toml"),
        ],
        run_dir=_example_root,
    )

    #------Below this line, need Inro's Emme installed-----
    if inro_context != "inro": return
    #------------------------------------------------------

    controller.run()
    return controller
