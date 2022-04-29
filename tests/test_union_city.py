import glob
import os
import sys

from unittest.mock import MagicMock
from typing import Collection, Union

import openmatrix as omx
import pandas as pd
import pytest


_EXAMPLES_DIR = r"examples"


def test_example_download():
    # If (and only if) Emme is not installed, replace INRO libraries with MagicMock
    try:
        import inro.emme.database.emmebank
    except ModuleNotFoundError:
        sys.modules["inro.emme.database.emmebank"] = MagicMock()
        sys.modules["inro.emme.network"] = MagicMock()
        sys.modules["inro.emme.database.scenario"] = MagicMock()
        sys.modules["inro.emme.database.matrix"] = MagicMock()
        sys.modules["inro.emme.network.node"] = MagicMock()
        sys.modules["inro.emme.desktop.app"] = MagicMock()
        sys.modules["inro"] = MagicMock()
        sys.modules["inro.modeller"] = MagicMock()

    import shutil
    from tm2py.examples import get_example

    name = "UnionCity"
    example_dir = os.path.join(os.getcwd(), _EXAMPLES_DIR)
    union_city_root = os.path.join(example_dir, name)
    if os.path.exists(union_city_root):
        shutil.rmtree(union_city_root)

    get_example(
        example_name="UnionCity", example_subdir=_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    # default retrieval_url points to Union City example on box

    # check that the root union city folder exists
    assert os.path.isdir(os.path.join(example_dir, name))
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
            os.path.join(example_dir, name, file_name)
        ), f"get_example failed, missing {file_name}"
    # check zip file was removed
    assert not (os.path.exists(os.path.join(example_dir, name, "test_data.zip")))


def diff_omx(ref_omx: str, run_omx: str) -> Collection[Collection[str]]:
    """
    Compare two OMX files, return missing and different matrices from reference.

    Args:
        ref_omx: reference OMX file
        run_omx: run OMX file
    """
    _ref_f = open(ref_omx, "r")
    _run_f = open(run_omx, "r")
    _ref_matrix_names = _ref_f.list_matrices().sort()
    _run_matrix_names = _run_f.list_matrices().sort()

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


@pytest.mark.skipci
def test_highway_skims(union_city):
    run_dir = union_city.controller.run_dir

    ref_dir_hwy_skims = os.path.join(run_dir, "ref_skim_matrices", "highway")
    ref_skim_files = glob.glob(ref_dir_hwy_skims, "*.omx").sort()

    run_dir_hwy_skims = os.path.join(run_dir, "skim_matrices", "highway")
    run_skim_files = glob.glob(run_dir_hwy_skims, "*.omx").sort()

    # check that the expected files are all there
    ref_skim_names = [os.path.filename(f) for f in ref_skim_files]
    run_skim_names = [os.path.filename(f) for f in run_skim_files]

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


def assert_csv_equal(ref_csv: str, run_csv: str):
    ref_df = pd.read_csv(ref_csv)
    run_df = pd.read_csv(run_csv)
    return pd.testing.assert_frame_equal(ref_df, run_df)


@pytest.mark.skipci
def test_maz_da_skims(union_city):
    run_dir = union_city.controller.run_dir

    ref_dir_hwy_skims = os.path.join(run_dir, "ref_skim_matrices", "highway")
    run_dir_hwy_skims = os.path.join(run_dir, "skim_matrices", "highway")

    ref_csv = os.path.join(ref_dir_hwy_skims, "HWYSKIM_MAZMAZ_DA.csv")
    run_csv = os.path.join(run_dir_hwy_skims, "HWYSKIM_MAZMAZ_DA.csv")

    return assert_csv_equal(ref_csv, run_csv)
