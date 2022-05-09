"""Tools module for common resources / shared code and "utilities" in the tm2py package."""
import multiprocessing
import os
import re
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from contextlib import contextmanager as _context
from typing import Union


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
