"Test household travel model."

import os

import pytest


@pytest.mark.skipci
def test_household_travel(ctramp_context,examples_dir):
    "Tests that household travel component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "household")

    #------Below this line, need Inro's Emme installed-----
    if ctramp_context != "ctramp": return
    my_run.run_next()

    # TODO write assert
