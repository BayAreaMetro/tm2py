"Test airport passenger access model."

import os

import pytest

from tm2py.examples import get_example


@pytest.mark.menow
def test_air_pax_model(inro_context, examples_dir, root_dir):
    "Tests that airport access model be run."
    from tools import test_component

    get_example(example_name="UnionCity", root_dir=root_dir)

    my_run = test_component(examples_dir, "air_passenger")
    my_run.run_next()

    # TODO write assert
