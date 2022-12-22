"Test commercial vehicle model."

import os

import pytest
from conftest import inro_context

from tm2py.examples import get_example


def test_commercial_vehicle(examples_dir, root_dir):
    "Tests that commercial vehicle component can be run."
    from tools import test_component

    get_example(example_name="UnionCity", root_dir=root_dir)

    my_run = test_component(examples_dir, "truck")
    my_run.run_next()

    # TODO write assert
