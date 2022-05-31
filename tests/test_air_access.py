"Test airport passenger access model."

import os

import pytest


@pytest.mark.menow
def test_air_pax_model(examples_dir):
    "Tests that airport access model be run."
    from tools import test_component
    my_run = test_component(examples_dir, "air_passenger")
    my_run.run_next()

    # TODO write assert