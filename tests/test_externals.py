"Test external travel model."

import os

import pytest

from tm2py.examples import get_example


# @pytest.mark.menow
def test_external_travel(examples_dir, root_dir):
    "Tests that internal/external travel component can be run."
    from tools import test_component

    get_example(example_name="UnionCity", root_dir=root_dir)

    my_run = test_component(examples_dir, "internal_external")
    my_run.run_next()

    # TODO write assert
