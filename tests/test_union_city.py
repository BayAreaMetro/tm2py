import os

import pytest
import openmatrix as _omx


_UNION_CITY_ROOT_DIR = ""


@pytest.mark.skipci
def test_highway():
    import tm2py.controller as _controller

    controller = _controller.RunController(
        [
            os.path.join(_UNION_CITY_ROOT_DIR, r"example_union\scenario_arch.toml"),
            os.path.join(_UNION_CITY_ROOT_DIR, r"example_union\model.toml"),
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
    ), f"HWYSKIM_MAZMAZ_DA.csv differes on {count_different_lines} lines"
