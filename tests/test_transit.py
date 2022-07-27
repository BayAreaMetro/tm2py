"Test external travel model."

import os

import pytest


@pytest.mark.menow
def test_prep_transit_network(examples_dir):
    "Tests that internal/external travel component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "prepare_network_transit")
    my_run.run_next()

    # TODO write assert


@pytest.mark.menow
def test_transit_assign(examples_dir):
    "Tests that transit assign component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "transit_assign")
    my_run.run_next()

    # TODO write assert


@pytest.mark.menow
def test_transit_skim(examples_dir):
    "Tests that transit skim component can be run."
    from tools import test_component

    my_run = test_component(examples_dir, "transit_skim")
    my_run.run_next()

    # TODO write assert
