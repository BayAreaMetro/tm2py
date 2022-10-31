"Test airport passenger access model."

import os

import pytest

from tm2py.examples import get_example


def test_air_pax_model(inro_context, examples_dir, root_dir):
    "Tests that airport access model be run."
    from tools import test_component

    get_example(example_name="UnionCity", root_dir=root_dir)

    my_run = test_component(examples_dir, "air_passenger")

    #------Below this line, need Inro's Emme installed-----
    if inro_context != "inro": return

    my_run.run_next()

    # TODO write assert
