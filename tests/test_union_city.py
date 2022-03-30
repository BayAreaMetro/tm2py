import os
import pytest


_EXAMPLES_ROOT_DIR = r"examples"


@pytest.mark.skipci
def test_highway():
    from tm2py.controller import RunController
    import openmatrix as _omx

    union_city_root = os.path.join(os.getcwd(), _EXAMPLES_ROOT_DIR, "UnionCity")
    if not os.path.isdir(union_city_root):
        root_dir = os.path.dirname(os.path.dirname(__file__))
        if os.name == "nt":
            os.popen(f"{root_dir}\\bin\\get_test_data.bat {_EXAMPLES_ROOT_DIR}")
            import zipfile

            with zipfile.ZipFile(
                os.path.join(os.getcwd(), _EXAMPLES_ROOT_DIR, "test_data.zip"), "r"
            ) as zip_ref:
                zip_ref.extractall(union_city_root)
            os.remove("test_data.zip")
        else:
            os.popen(f"{root_dir}\\bin\\get_test_data {_EXAMPLES_ROOT_DIR}")

    controller = RunController(
        [
            os.path.join(union_city_root, r"scenario_config.toml"),
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
