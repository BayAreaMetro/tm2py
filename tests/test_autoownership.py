"Test auto ownership model."

import os

import pytest


def test_home_access(examples_dir):
    "Tests that home accessibility can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "home_accessibility")
    my_run.run_next()

    # TODO write assert
