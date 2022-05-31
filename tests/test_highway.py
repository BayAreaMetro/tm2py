"""Testing of highway network components"""
import glob
import os

import pytest
from conftest import inro_context
from tools import assert_csv_equal, diff_omx


def test_prepare_highway_network(examples_dir):
    "Tests that prepare highway network component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "prepare_network_highway")
    my_run.run_next()

    # TODO write assert


def test_highway_assign(examples_dir):
    "Tests that highway network assignment component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "highway")
    my_run.run_next()

    # TODO write assert


def test_highway_maz_assign(examples_dir):
    "Tests that highway MAZ network assignment component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "highway_maz_assign")
    my_run.run_next()

    # TODO write assert


def test_highway_maz_skim(examples_dir):
    "Tests that highway MAZ network skimming component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "highway_maz_skim")
    my_run.run_next()

    # TODO write assert


@pytest.mark.skipif(inro_context != "inro", reason="requires full inro context")
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


@pytest.mark.skipif(inro_context != "inro", reason="requires full inro context")
def test_maz_da_skims(union_city):
    """Test that the DA MAZ skims match the reference."""
    run_dir = union_city.run_dir

    ref_dir_hwy_skims = os.path.join(run_dir, "ref_skim_matrices", "highway")
    run_dir_hwy_skims = os.path.join(run_dir, "skim_matrices", "highway")

    ref_csv = os.path.join(ref_dir_hwy_skims, "HWYSKIM_MAZMAZ_DA.csv")
    run_csv = os.path.join(run_dir_hwy_skims, "HWYSKIM_MAZMAZ_DA.csv")

    return assert_csv_equal(ref_csv, run_csv)
