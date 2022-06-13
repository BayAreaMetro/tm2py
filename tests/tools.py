"Utilities for testing."

import os
from typing import Collection

import openmatrix as omx
import pandas as pd


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


def test_component(examples_dir, component, example_name="UnionCity"):
    from tm2py.controller import RunController

    base_configs = [
        examples_dir / "model_config.toml",
        examples_dir / "scenario_config.toml",
    ]
    my_components = [component]
    print(f"TESTING COMPONENTS: {my_components}")
    my_run = RunController(
        base_configs, run_dir=examples_dir / example_name, run_components=my_components
    )
    # TODO RUN COMPONENT
    print(f"RIGHT NOW JUST INITIATING - NOT RUNNING")
    print(my_run)
    return my_run
