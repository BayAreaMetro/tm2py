"Test external travel model."

import os

import pytest
from conftest import inro_context

from tm2py.examples import get_example


@pytest.mark.menow
def test_transit(examples_dir, root_dir):
    "Tests that internal/external travel component can be run."
    from tools import test_component

    get_example(example_name="UnionCity", root_dir=root_dir)

    my_run = test_component(
        examples_dir, ["prepare_network_transit", "transit_assign", "transit_skim"]
    )

    my_run.run()

    # TODO write assert
