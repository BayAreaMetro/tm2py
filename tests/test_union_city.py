"""Testing module for UnionCity subarea 'real' model runs."""

import glob
import os
import sys
from typing import Collection
from unittest.mock import MagicMock

import openmatrix as omx
import pandas as pd
import pytest

_EXAMPLES_DIR = r"examples"
NOTEBOOKS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "notebooks"
)
BIN_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bin"
)


def test_example_download():
    """Tests that example data can be downloaded."""
    # If (and only if) Emme is not installed, replace INRO libraries with MagicMock
    try:
        import inro.emme.database.emmebank
    except ModuleNotFoundError:
        sys.modules["inro.emme.database.emmebank"] = MagicMock()
        sys.modules["inro.emme.database.scenario"] = MagicMock()
        sys.modules["inro.emme.database.matrix"] = MagicMock()
        sys.modules["inro.emme.network"] = MagicMock()
        sys.modules["inro.emme.network.link"] = MagicMock()
        sys.modules["inro.emme.network.mode"] = MagicMock()
        sys.modules["inro.emme.network.node"] = MagicMock()
        sys.modules["inro.emme.desktop.app"] = MagicMock()
        sys.modules["inro"] = MagicMock()
        sys.modules["inro.modeller"] = MagicMock()

    import shutil

    from tm2py.examples import get_example

    name = "UnionCity"
    union_city_root = os.path.join(RUN_EXAMPLES_DIR, name)
    if os.path.exists(union_city_root):
        shutil.rmtree(union_city_root)

    get_example(
        example_name="UnionCity", example_subdir=RUN_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    # default retrieval_url points to Union City example on box

    # check that the root union city folder exists
    assert os.path.isdir(os.path.join(RUN_EXAMPLES_DIR, name))
    # check some expected files exists
    files_to_check = [
        os.path.join("inputs", "hwy", "tolls.csv"),
        os.path.join("inputs", "nonres", "2035_fromOAK.csv"),
        os.path.join("inputs", "landuse", "maz_data.csv"),
        os.path.join("emme_project", "mtc_emme.emp"),
        os.path.join("emme_project", "Database_highway", "emmebank"),
    ]
    for file_name in files_to_check:
        assert os.path.exists(
            os.path.join(RUN_EXAMPLES_DIR, name, file_name)
        ), f"get_example failed, missing {file_name}"
    # check zip file was removed
    assert not (os.path.exists(os.path.join(RUN_EXAMPLES_DIR, name, "test_data.zip")))


def diff_omx(ref_omx: str, run_omx: str) -> Collection[Collection[str]]:
    """Compare two OMX files, return missing and different matrices from reference.

    Args:
        ref_omx: reference OMX file
        run_omx: run OMX file
    """
    _ref_f = omx.open_file(ref_omx, "r")
    _run_f = omx.open_file(run_omx, "r")
    _ref_matrix_names = _ref_f.list_matrices()
    _run_matrix_names = _run_f.list_matrices()

    missing_matrices = [f for f in _ref_matrix_names if f not in _run_matrix_names]
    different_matrices = []
    for m_key in _ref_matrix_names:
        _ref_matrix = _ref_f[m_key].read()
        _run_matrix = _run_f[m_key].read()
        if not (_ref_matrix == _run_matrix).all():
            different_matrices.append(m_key)

    _ref_f.close()
    _run_f.close()
    return missing_matrices, different_matrices


@pytest.fixture(scope="module")
@pytest.mark.skipci
def union_city():
    """Union City model run testing fixture."""
    from tm2py.controller import RunController
    from tm2py.examples import get_example

    union_city_root = os.path.join(os.getcwd(), _EXAMPLES_DIR, "UnionCity")
    get_example(
        example_name="UnionCity", example_subdir=_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    controller = RunController(
        [
            os.path.join(_EXAMPLES_DIR, "scenario_config.toml"),
            os.path.join(_EXAMPLES_DIR, "model_config.toml"),
        ],
        run_dir=union_city_root,
    )
    controller.run()
    return controller


@pytest.mark.skipci
def test_highway_skims(union_city):
    """Test that the OMX highway skims match the reference."""
    run_dir = union_city.run_dir

    ref_dir_hwy_skims = os.path.join(run_dir, "ref_skim_matrices", "highway")
    ref_skim_files = glob.glob(os.path.join(ref_dir_hwy_skims, "*.omx"))

    run_dir_hwy_skims = os.path.join(run_dir, "skim_matrices", "highway")
    run_skim_files = glob.glob(os.path.join(run_dir_hwy_skims, "*.omx"))

    # check that the expected files are all there
    ref_skim_names = [os.path.basename(f) for f in ref_skim_files]
    run_skim_names = [os.path.basename(f) for f in run_skim_files]

    assert set(ref_skim_names) == set(
        run_skim_names
    ), f"Skim matrix names do not match expected\
        reference. \n Expected: {ref_skim_names}\n Actual: {run_skim_names}"

    missing_skims = []
    different_skims = []

    for ref_skim_f, run_skim_f in zip(ref_skim_files, run_skim_files):
        _missing_ms, _diff_ms = diff_omx(ref_skim_f, run_skim_f)
        missing_skims.extend([ref_skim_f + _m for _m in _missing_ms])
        different_skims.extend(ref_skim_f + _m for _m in _diff_ms)

    assert len(missing_skims) == 0, f"Missing skims: {missing_skims}"
    assert len(different_skims) == 0, f"Different skims: {different_skims}"

@pytest.mark.skipci
def test_transit():
    from tm2py.controller import RunController
    from tm2py.examples import get_example
    import toml

    union_city_root = get_example(
        example_name="UnionCity", example_subdir=RUN_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    scen_config_path = os.path.join(EXAMPLES_DIR, r"scenario_config.toml")
    with open(scen_config_path, "r") as fin:
        scen_config = toml.load(fin)
    scen_config["run"]["initial_components"] = [
        "prepare_network_transit",
        "transit_assign",
        "transit_skim",
    ]
    scen_config["run"]["global_iteration_components"] = []
    scen_config["run"]["start_iteration"] = 0
    scen_config["run"]["end_iteration"] = 1

    with tempfile.TemporaryDirectory() as temp_dir:
        scen_config_path = os.path.join(temp_dir, "scenario_config.toml")
        with open(scen_config_path, "w") as fout:
            toml.dump(scen_config, fout)
        controller = RunController(
            [
                scen_config_path,
                os.path.join(EXAMPLES_DIR, r"model_config.toml"),
            ],
            run_dir=union_city_root
        )
        controller.run()


def assert_csv_equal(ref_csv: str, run_csv: str):
    """Compare two csv files, return results of pd.testing.assert_frame_equal().

    Args:
        ref_csv (str): Reference CSV location
        run_csv (str): Model run CSV location

    Returns:
        Results of pd.testing.assert_frame_equal()
    """
    ref_df = pd.read_csv(ref_csv)
    run_df = pd.read_csv(run_csv)
    return pd.testing.assert_frame_equal(ref_df, run_df)


@pytest.mark.skipci
def test_maz_da_skims(union_city):
    """Test that the DA MAZ skims match the reference."""
    run_dir = union_city.run_dir

    ref_dir_hwy_skims = os.path.join(run_dir, "ref_skim_matrices", "highway")
    run_dir_hwy_skims = os.path.join(run_dir, "skim_matrices", "highway")

    ref_csv = os.path.join(ref_dir_hwy_skims, "HWYSKIM_MAZMAZ_DA.csv")
    run_csv = os.path.join(run_dir_hwy_skims, "HWYSKIM_MAZMAZ_DA.csv")

    return assert_csv_equal(ref_csv, run_csv)
