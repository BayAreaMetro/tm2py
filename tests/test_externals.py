"Test external travel model."

import os

import pytest


# @pytest.mark.menow
def test_external_travel(examples_dir):
    "Tests that internal/external travel component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "internal_external")
    my_run.run_next()

    # TODO write assert
