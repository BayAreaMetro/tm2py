"""Testing module for UnionCity subarea 'real' model runs."""

import os

import pytest

def test_example_download(examples_dir, root_dir, inro_context):
    """Tests that example data can be downloaded."""
    EXAMPLE = "UnionCity"

    import shutil

    from tm2py.examples import get_example

    example_root = os.path.join(examples_dir, EXAMPLE)
    if os.path.exists(example_root):
        shutil.rmtree(example_root)

    # default retrieval_url points to Union City example on box
    _ex_dir = get_example(example_name="UnionCity", root_dir=root_dir)

    # check that the root union city folder exists
    assert _ex_dir == example_root
    assert os.path.isdir(example_root)

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
            os.path.join(example_root, file_name)
        ), f"get_example failed, missing {file_name}"

    # check zip file was removed
    assert not (os.path.exists(os.path.join(example_root, "test_data.zip")))


@pytest.fixture(scope="session")
def union_city(examples_dir, root_dir, inro_context):
    """Union City model run testing fixture."""
    from tm2py.controller import RunController
    from tm2py.examples import get_example

    EXAMPLE = "UnionCity"
    _example_root = os.path.join(examples_dir, EXAMPLE)

    get_example(example_name="UnionCity", root_dir=root_dir)
    controller = RunController(
        [
            os.path.join(examples_dir, "scenario_config.toml"),
            os.path.join(examples_dir, "model_config.toml"),
        ],
        run_dir=_example_root,
    )
    controller.run()
    return controller


def test_validate_input_fail(examples_dir, inro_context, temp_dir):
    """Test that validate_input fails when required inputs are missing."""
    import toml

    from tm2py.controller import RunController
    from tm2py.examples import get_example

    model_config_path = os.path.join(examples_dir, r"model_config.toml")
    with open(model_config_path, "r") as fin:
        bad_model_config = toml.load(fin)
    bad_model_config["highway"]["tolls"]["file_path"] = "foo.csv"

    bad_model_config_path = os.path.join(temp_dir, r"bad_model_config.toml")
    with open(bad_model_config_path, "w") as fout:
        toml.dump(bad_model_config, fout)

    union_city_root = os.path.join(examples_dir, "UnionCity")

    with pytest.raises(Exception) as e_info:
        RunController(
            [
                os.path.join(examples_dir, r"scenario_config.toml"),
                bad_model_config_path,
            ],
            run_dir=union_city_root,
        )
        assert e_info.type is FileNotFoundError
