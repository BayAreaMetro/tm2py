import os
from unittest.mock import MagicMock
import sys
import pytest


_EXAMPLES_DIR = r"examples"
NOTEBOOKS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "notebooks"
)
BIN_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "bin"
)


def test_example_download():
    # If (and only if) Emme is not installed, replace INRO libraries with MagicMock
    try:
        import inro.emme.database.emmebank
    except ModuleNotFoundError:
        sys.modules["inro.emme.database.emmebank"] = MagicMock()
        sys.modules["inro.emme.network"] = MagicMock()
        sys.modules["inro.emme.database.scenario"] = MagicMock()
        sys.modules["inro.emme.database.matrix"] = MagicMock()
        sys.modules["inro.emme.network.node"] = MagicMock()
        sys.modules["inro.emme.desktop.app"] = MagicMock()
        sys.modules["inro"] = MagicMock()
        sys.modules["inro.modeller"] = MagicMock()

    import shutil
    from tm2py.examples import get_example

    name = "UnionCity"
    example_dir = os.path.join(os.getcwd(), _EXAMPLES_DIR)
    union_city_root = os.path.join(example_dir, name)
    if os.path.exists(union_city_root):
        shutil.rmtree(union_city_root)

    get_example(
        example_name="UnionCity", example_subdir=_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    # default retrieval_url points to Union City example on box

    # check that the root union city folder exists
    assert os.path.isdir(os.path.join(example_dir, name))
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
            os.path.join(example_dir, name, file_name)
        ), f"get_example failed, missing {file_name}"
    # check zip file was removed
    assert not (os.path.exists(os.path.join(example_dir, name, "test_data.zip")))


@pytest.mark.skipci
def test_highway():
    from tm2py.controller import RunController
    from tm2py.examples import get_example
    import openmatrix as _omx

    union_city_root = os.path.join(os.getcwd(), _EXAMPLES_DIR, "UnionCity")
    get_example(
        example_name="UnionCity", example_subdir=_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    controller = RunController(
        [
            os.path.join(_EXAMPLES_DIR, r"scenario_config.toml"),
            os.path.join(_EXAMPLES_DIR, r"model_config.toml"),
        ],
        run_dir=union_city_root
    )
    controller.run()

    root = os.path.join(controller.run_dir, r"skim_matrices\highway")
    ref_root = os.path.join(controller.run_dir, r"ref_skim_matrices\highway")
    open_files = []
    file_names = [name for name in os.listdir(root) if name.endswith(".omx")]
    different_skims = []
    try:
        for name in file_names:
            skims = _omx.open_file(os.path.join(root, name))
            open_files.append(skims)
            ref_skims = _omx.open_file(os.path.join(ref_root, name))
            open_files.append(ref_skims)
            for key in skims.list_matrices():
                data = skims[key].read()
                ref_data = ref_skims[key].read()
                if not (data == ref_data).all():
                    different_skims.append(key)
    finally:
        for f in open_files:
            f.close()
    assert (
        len(different_skims) == 0
    ), f"there are {len(different_skims)} different skims: {','.join(different_skims)}"

    count_different_lines = 0
    with open(os.path.join(root, "HWYSKIM_MAZMAZ_DA.csv")) as data:
        with open(os.path.join(ref_root, "HWYSKIM_MAZMAZ_DA.csv")) as ref_data:
            for line in data:
                ref_line = next(ref_data)
                if ref_line != line:
                    count_different_lines += 1
    assert (
        count_different_lines == 0
    ), f"HWYSKIM_MAZMAZ_DA.csv differs on {count_different_lines} lines"


@pytest.mark.skipci
def test_bin_tm2py_on_union():
    """Test that that bin entry point runs"""

    from tm2py.examples import get_example
    union_city_root = get_example(
        example_name="UnionCity", example_subdir=_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    scen_config = os.path.abspath(os.path.join(_EXAMPLES_DIR, r"scenario_config.toml"))
    model_config = os.path.abspath(os.path.join(_EXAMPLES_DIR, r"model_config.toml"))
    bin_tm2py = os.path.join(BIN_DIR, "tm2py")
    os.popen(f"{bin_tm2py} -s {scen_config} -m {model_config} -r {union_city_root}")


@pytest.mark.skipci
def test_notebook_tm2py_on_union():
    """Test that that notebook example entry point runs"""
    cwd = os.getcwd()
    import json
    with open(os.path.join(NOTEBOOKS_DIR, "Run model.ipynb"), 'r') as notebook:
        notebook_data = json.load(notebook)
    code = []
    for cell in notebook_data["cells"]:
        if cell["cell_type"] == "code":
            code.extend(cell["source"])
            code.append("\n")
    print(code)
    try:
        os.chdir(NOTEBOOKS_DIR)
        exec("".join(code))
    finally:
        os.chdir(cwd)
