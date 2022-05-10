"""Testing for the tools module."""

import os
import sys
from unittest.mock import MagicMock

import pytest

_EXAMPLE_URL = (
    r"https://mtcdrive.box.com/shared/static/3entr016e9teq2wt46x1os3fjqylfoge.zip"
)


def test_download_unzip(temp_dir, inro_context):
    """If (and only if) Emme is not installed, replace INRO libraries with MagicMock."""
    from tm2py.tools import _download, _unzip

    temp_file = os.path.join(temp_dir, "test_download.zip")
    unzip_directory = os.path.join(temp_dir, "test_download")

    print("Downloading test_download.zip")
    _download(_EXAMPLE_URL, temp_file)
    assert os.path.getsize(temp_file) > 0, "download failed"

    print("Unzipping test_download.zip")
    _unzip(temp_file, unzip_directory)
    assert os.path.exists(unzip_directory), "unzip failed, no directory"
    assert os.path.getsize(unzip_directory) > 0, "unzip failed, empty directory"

    print("Checking for expected files.")
    files_to_check = [
        os.path.join("inputs", "hwy", "tolls.csv"),
        os.path.join("inputs", "nonres", "2035_fromOAK.csv"),
    ]
    for file_name in files_to_check:
        assert os.path.exists(
            os.path.join(unzip_directory, file_name)
        ), f"unzip failed, missing {file_name}"


@pytest.mark.menow
def test_interpolate(inro_context):
    """Test interpolation."""
    import pandas as pd
    from pandas.testing import assert_frame_equal

    from tm2py.tools import interpolate_dfs

    _input_df = pd.DataFrame(
        {
            "prop1_2020": [20, 200, 2000],
            "prop2_2020": [40, 55, 60],
            "prop1_2030": [30, 300, 3000],
            "prop2_2030": [40, 55, 70],
        }
    )

    _2025_output_df = interpolate_dfs(_input_df, [2020, 2030], 2025)

    _2025_expected_output_df = pd.DataFrame(
        {
            "prop1": [25.0, 250.0, 2500.0],
            "prop2": [40.0, 55.0, 65.0],
        }
    )

    _2020_output_df = interpolate_dfs(_input_df, [2020, 2030], 2020)

    _2020_expected_output_df = pd.DataFrame(
        {
            "prop1": [20.0, 200.0, 2000.0],
            "prop2": [40.0, 55.0, 60.0],
        }
    )

    _2030_output_df = interpolate_dfs(_input_df, [2020, 2030], 2030)

    _2030_expected_output_df = pd.DataFrame(
        {
            "prop1": [30.0, 300.0, 3000.0],
            "prop2": [40.0, 55.0, 70.0],
        }
    )

    assert_frame_equal(_2025_output_df, _2025_expected_output_df)

    assert_frame_equal(_2020_output_df, _2020_expected_output_df)

    assert_frame_equal(_2030_output_df, _2030_expected_output_df)
