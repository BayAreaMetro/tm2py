"Test household travel model."

import os

import pytest


@pytest.mark.skipci
def test_household_travel(examples_dir):
    "Tests that household travel component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "household")
    my_run.run_next()

    # TODO write assert
