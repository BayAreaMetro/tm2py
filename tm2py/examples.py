"""Download and unzip examples for tm2py, used in tests"""


import os

from .tools import download_unzip

_ROOT_DIR = r".."

_DEFAULT_EXAMPLE_URL = (
    r"https://mtcdrive.box.com/shared/static/3entr016e9teq2wt46x1os3fjqylfoge.zip"
)
_DEFAULT_EXAMPLE_SUBDIR = r"examples"
_DEFAULT_EXAMPLE_NAME = "UnionCity"


def get_example(
    example_name: str = _DEFAULT_EXAMPLE_NAME,
    example_subdir: str = _DEFAULT_EXAMPLE_SUBDIR,
    root_dir: str = _ROOT_DIR,
    retrieval_url: str = _DEFAULT_EXAMPLE_URL,
) -> str:
    """Returns example directory; downloads if necessary from retrieval URL.

    Args:
        example_name (str, optional): Used to retrieve sub-folder or create it if doesn't exist.
            Defaults to _DEFAULT_EXAMPLE_NAME.
        example_subdir (str, optional): Where to find examples within root dir. Defaults
            to _DEFAULT_EXAMPLE_SUBDIR.
        root_dir (str, optional): Root dir of project. Defaults to _ROOT_DIR.
        retrieval_url (str, optional): URL to retrieve example data zip from. Defaults
            to _DEFAULT_EXAMPLE_URL.

    Raises:
        FileNotFoundError: If can't find the files after trying to download it.

    Returns:
        str: Path to example data.
    """
    _example_dir = os.path.join(root_dir, example_subdir)
    _this_example_dir = os.path.join(_example_dir, example_name)
    if os.path.isdir(_this_example_dir):
        return _this_example_dir

    download_unzip(retrieval_url, _example_dir, _this_example_dir)
    if not os.path.isdir(_this_example_dir):
        raise FileNotFoundError(f"example {_this_example_dir} not found")

    return _this_example_dir
