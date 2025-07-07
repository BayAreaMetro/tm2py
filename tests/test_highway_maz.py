"""Testing of highway network components"""
import glob
import os

import pytest
from conftest import inro_context
from tools import assert_csv_equal, diff_omx

from tm2py.examples import get_example


def test_highway_maz(inro_context, examples_dir, root_dir):
    "Tests that highway MAZ network assignment component can be run."
    from tools import test_component

    get_example(example_name="UnionCity", root_dir=root_dir)

    my_run = test_component(examples_dir, ["highway_maz_assign", "highway_maz_skim"])

    if inro_context != "inro":
        return
    my_run.run()

    # TODO write assert


def test_maz_da_skims(examples_dir):
    """Test that the DA MAZ skims match the reference."""
    run_dir = os.path.join(examples_dir, "UnionCity")

    ref_dir_hwy_skims = os.path.join(run_dir, "ref_skim_matrices", "highway")
    run_dir_hwy_skims = os.path.join(run_dir, "skim_matrices", "highway")

    ref_csv = os.path.join(ref_dir_hwy_skims, "HWYSKIM_MAZMAZ_DA.csv")
    run_csv = os.path.join(run_dir_hwy_skims, "HWYSKIM_MAZMAZ_DA.csv")

    return assert_csv_equal(ref_csv, run_csv)
