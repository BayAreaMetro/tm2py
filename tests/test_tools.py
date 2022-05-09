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
