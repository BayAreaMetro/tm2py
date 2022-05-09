"""Testing module for UnionCity subarea 'real' model runs."""

import glob
import os
import sys
from typing import Collection, Union
from unittest.mock import MagicMock

import openmatrix as omx
import pandas as pd
import pytest


def test_example_download(examples_dir, root_dir, inro_context):
    """Tests that example data can be downloaded."""
    EXAMPLE = "UnionCity"

    import shutil

    from tm2py.examples import get_example

    example_root = os.path.join(examples_dir, EXAMPLE)
    if os.path.exists(example_root):
        shutil.rmtree(example_root)

    # default retrieval_url points to Union City example on box
    _ex_dir = get_example(example_name="UnionCity", root_dir=root_dir)

    # check that the root union city folder exists
    assert _ex_dir == example_root
    assert os.path.isdir(example_root)

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
            os.path.join(example_root, file_name)
        ), f"get_example failed, missing {file_name}"

    # check zip file was removed
    assert not (os.path.exists(os.path.join(example_root, "test_data.zip")))


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
def union_city(examples_dir, root_dir):
    """Union City model run testing fixture."""
    from tm2py.controller import RunController
    from tm2py.examples import get_example

    EXAMPLE = "UnionCity"
    _example_root = os.path.join(examples_dir, EXAMPLE)

    get_example(example_name="UnionCity", root_dir=root_dir)
    controller = RunController(
        [
            os.path.join(examples_dir, "scenario_config.toml"),
            os.path.join(examples_dir, "model_config.toml"),
        ],
        run_dir=_example_root,
    )
    controller.run()
    return controller


@pytest.mark.menow
@pytest.mark.xfail
def test_validate_input_fail(examples_dir, inro_context, temp_dir):

    import toml

    from tm2py.controller import RunController
    from tm2py.examples import get_example

    model_config_path = os.path.join(examples_dir, r"model_config.toml")
    with open(model_config_path, "r") as fin:
        bad_model_config = toml.load(fin)
    bad_model_config["highway"]["tolls"]["file_path"] = "foo.csv"

    bad_model_config_path = os.path.join(temp_dir, r"model_config.toml")
    with open(bad_model_config_path, "w") as fout:
        toml.dump(bad_model_config, fout)

    union_city_root = os.path.join(examples_dir, "UnionCity")

    controller = RunController(
        [
            os.path.join(examples_dir, r"scenario_config.toml"),
            bad_model_config_path,
        ],
        run_dir=union_city_root,
    )

    controller.run()


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
