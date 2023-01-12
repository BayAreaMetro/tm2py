"""Testing of highway network components"""
import glob
import os

import pytest
from tools import assert_csv_equal, diff_omx

from tm2py.examples import get_example


def test_highway(examples_dir, root_dir):
    "Tests that prepare highway network component can be run."
    from tools import test_component

    get_example(example_name="UnionCity", root_dir=root_dir)

    my_run = test_component(examples_dir, ["prepare_network_highway", "highway"])
    my_run.run()

    # TODO write assert


def test_highway_skims(examples_dir):
    """Test that the OMX highway skims match the reference."""
    run_dir = os.path.join(examples_dir, "UnionCity")

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
