"""Tools module for common resources / shared code and "utilities" in the tm2py package."""
import multiprocessing
import os
import re
import subprocess as _subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile

from typing import Collection, Union, List

from contextlib import contextmanager as _context

import pandas as pd

def parse_num_processors(value: Union[str, int, float]):
    """Convert input value (parse if string) to number of processors.

    Args:
        value: an int, float or string; string value can be "X" or "MAX-X"
    Returns:
        An int of the number of processors to use

    Raises:
        Exception: Input value exceeds number of available processors
        Exception: Input value less than 1 processors
    """
    max_processors = multiprocessing.cpu_count()
    if isinstance(value, str):
        result = value.upper()
        if result == "MAX":
            return max_processors
        if re.match("^[0-9]+$", value):
            return int(value)
        result = re.split(r"^MAX[\s]*-[\s]*", result)
        if len(result) == 2:
            return max(max_processors - int(result[1]), 1)
        raise Exception(f"Input value {value} is an int or string as 'MAX-X'")

    result = int(value)
    if result > max_processors:
        raise Exception(f"Input value {value} greater than available processors")
    if result < 1:
        raise Exception(f"Input value {value} less than 1 processors")
    return value


@_context
def _urlopen(url):
    """Access the url, following redirect if needed (i.e. box).

    Wrapper on urllib.request.urlopen. For use with a context manager (with statement).

    Args:
        url (str): source URL to access

    Returns:
        url.response object

    Raises:
        ValueError: HTTP error from urllib
    """
    request = urllib.request.Request(url)
    # Handle Redirects using solution shown by user: metatoaster on StackOverflow
    # https://stackoverflow.com/questions/62384020/python-3-7-urllib-request-doesnt-follow-redirect-url
    print(f"Opening URL: {url}")
    try:
        with urllib.request.urlopen(request) as response:
            print(f"No redirects found.")
            yield response
    except urllib.error.HTTPError as error:
        print("Redirect Error")
        if error.status != 307:
            raise ValueError(f"HTTP Error {error.status}") from error
        redirected_url = urllib.parse.urljoin(url, error.headers["Location"])
        print(f"Redirected to: {redirected_url}")
        with urllib.request.urlopen(redirected_url) as response:
            yield response


def _download(url: str, target_destination: str):
    """Download file with redirects (i.e. box).

    Args:
        url (str): source URL to download data from
        target_destination (str): destination file path to save download
    """
    with _urlopen(url) as response:
        total_length = int(response.headers.get("content-length"))
        print(f"Total Download Size: {total_length}")
        with open(target_destination, "wb") as out_file:
            out_file.write(response.read())


def _unzip(target_zip: str, target_dir: str):
    """Unzip file at target_zip to directory at target_dir.

    Args:
        target_zip: path to existing, valid, zip file
        target_dir: path to directory
    """
    with zipfile.ZipFile(target_zip, "r") as zip_ref:
        zip_ref.extractall(target_dir)


def download_unzip(
    url: str, out_base_dir: str, target_dir: str, zip_filename: str = "test_data.zip"
) -> None:
    """Download and unzips a file from a URL. The zip file is removed after extraction.

    Args:
        url (str): Full URL do download from.
        out_base_dir (str): Where to unzip the file.
        target_dir (str): What to unzip the file as.
        zip_filename (str, optional): Filename to store zip file as. Defaults to "test_data.zip".
    """
    target_zip = os.path.join(out_base_dir, zip_filename)
    if not os.path.isdir(out_base_dir):
        os.makedirs(out_base_dir)
    urllib.request.Request(url)
    _download(url, target_zip)
    _unzip(target_zip, target_dir)
    os.remove(target_zip)


@_context
def temp_file(mode: str = "w+", prefix: str = "", suffix: str = ""):
    """Temp file wrapper to return open file handle and named path.

    A named temporary file (using mkstemp) with specified prefix and
    suffix is created and opened with the specified mode. The file
    handle and path are returned. The file is closed and deleted on exit.

    Args:
        mode: mode to open file, [rw][+][b]
        prefix: optional text to start temp file name
        suffix: optional text to end temp file name
    """
    file_ref, file_path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
    file = os.fdopen(file_ref, mode=mode)
    try:
        yield file, file_path
    finally:
        if not file.closed:
            file.close()
        os.remove(file_path)


def run_process(commands: List[str], name: str = ""):
    """Run system level commands as blocking process and log output and error messages.

    Args:
        commands: list of one or more commands to execute
        name: optional name to use for the temp bat file
    """
    # when merged with develop_logging branch can use get_logger
    # logger = Logger.get_logger
    logger = None
    with temp_file("w", prefix=name, suffix=".bat") as (bat_file, bat_file_path):
        bat_file.write("\n".join(commands))
        bat_file.close()
        if logger:
            # temporary file to capture output error messages generated by Java
            # Note: temp file created in the current working directory
            with temp_file(mode="w+", suffix="_error.log") as (err_file, _):
                try:
                    output = _subprocess.check_output(
                        bat_file_path, stderr=err_file, shell=True
                    )
                    logger.log(output.decode("utf-8"))
                except _subprocess.CalledProcessError as error:
                    logger.log(error.output)
                    raise
                finally:
                    err_file.seek(0)
                    error_msg = err_file.read()
                    if error_msg:
                        logger.log(error_msg)
        else:
            _subprocess.check_call(bat_file_path, shell=True)

def interpolate_dfs(
    df: pd.DataFrame,
    ref_points: Collection[Union[float,int]],
    target_point: Union[float,int],
    ref_col_name: str = "ends_with",
) -> pd.DataFrame:
    """Interpolate for the model year assuming linear growth between the reference years.
    
    Args:
        df (pd.DataFrame): dataframe to interpolate on, with ref points contained in column
            name per ref_col_name.  
        ref_points (Collection[Union[float,int]]): reference years to interpolate between
        target_point (Union[float,int]): target year
        ref_col_name (str, optional): column name to use for reference years. 
            Defaults to "ends_with".
    """
    if ref_col_name not in ["ends_with"]:
        raise NotImplementedError(f"{ref_col_name} not implemented")
    if len(ref_points) != 2:
        raise NotImplementedError(f"{ref_points} reference points not implemented")

    ref_points.sort()
    _start_point, _end_point = ref_points
    try:
        assert _start_point <= target_point <= _end_point
    except:
        raise ValueError(f"Target Point: {target_point} not within range of \
            Reference Points: {ref_points}")

    _start_ref_df = df[[c for c in df.columns if c.endswith(f'{_start_point}')]].copy()
    _end_ref_df = df[[c for c in df.columns if c.endswith(f'{_end_point}')]].copy()

    try:
        assert len(_start_ref_df.columns) == len(_end_ref_df.columns)
    except:
        raise ValueError(f"{_start_point} and {_end_point} have different number of columns:\n\
           {_start_point} Columns: {_start_ref_df.columns}\n\
           {_end_point} Columns: {_end_ref_df.columns}\
        ")

    _start_ref_df.rename(columns=lambda x: x.replace(f"_{_start_point}", ""), inplace=True)
    _end_ref_df.rename(columns=lambda x: x.replace(f"_{_end_point}", ""), inplace=True)
    _scale_factor = float(target_point - _start_point) / (_end_point - _start_point)

    interpolated_df = (1 - _scale_factor) * _start_ref_df\
        + _scale_factor * _end_ref_df

    return interpolated_df