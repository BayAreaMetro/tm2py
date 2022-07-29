"Test commercial vehicle model."

import os

import pytest


def test_commercial_vehicle(examples_dir):
    "Tests that commercial vehicle component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "truck")
    my_run.run_next()

    # TODO write assert
