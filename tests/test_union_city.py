import os
from unittest.mock import MagicMock
import sys
import pytest


_EXAMPLES_DIR = r"examples"


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
        "scenario_config.toml",
        "model_config.toml",
        os.path.join("inputs", "landuse", "maz_data.csv"),
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
    import toml

    union_city_root = os.path.join(os.getcwd(), _EXAMPLES_DIR, "UnionCity")
    get_example(
        example_name="UnionCity", example_subdir=_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    scen_config_path = os.path.join(union_city_root, r"scenario_config.toml")
    with open(scen_config_path, 'r') as fin:
        scen_config = toml.load(fin)
    scen_config["run"]["initial_components"] = [
        "prepare_network_highway",
        "highway",
        "highway_maz_skim",
    ]
    scen_config["run"]["start_iteration"] = 0
    scen_config["run"]["end_iteration"] = 1
    with open(scen_config_path, 'w') as fout:
        toml.dump(scen_config, fout)

    controller = RunController(
        [
            scen_config_path,
            os.path.join(union_city_root, r"model_config.toml"),
        ]
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
def test_transit():
    from tm2py.controller import RunController
    from tm2py.examples import get_example
    import toml

    union_city_root = os.path.join(os.getcwd(), _EXAMPLES_DIR, "UnionCity")
    get_example(
        example_name="UnionCity", example_subdir=_EXAMPLES_DIR, root_dir=os.getcwd()
    )
    scen_config_path = os.path.join(union_city_root, r"scenario_config.toml")
    with open(scen_config_path, 'r') as fin:
        scen_config = toml.load(fin)
    scen_config["run"]["initial_components"] = [
        "prepare_network_transit",
        "transit_assign",
        "transit_skim"
    ]
    scen_config["run"]["start_iteration"] = 0
    scen_config["run"]["end_iteration"] = 1
    with open(scen_config_path, 'w') as fout:
        toml.dump(scen_config, fout)
    controller = RunController(
        [
            scen_config_path,
            os.path.join(union_city_root, r"model_config.toml"),
        ]
    )
    controller.run()
