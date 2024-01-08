"""Testing module for the run configuration data classes."""

import os
import sys
from unittest.mock import MagicMock

import pytest


def test_config_read(examples_dir, inro_context):
    """Configuration should load parameters to the correct namespaces."""
    SCENARIO_CONFIG = "scenario_config.toml"
    MODEL_CONFIG = "model_config.toml"

    from tm2py.config import Configuration

    _scenario_config = os.path.join(examples_dir, SCENARIO_CONFIG)
    _model_config = os.path.join(examples_dir, MODEL_CONFIG)

    my_config = Configuration.load_toml([_scenario_config, _model_config])

    assert my_config.run.start_iteration == 0
    assert my_config.run.end_iteration == 2
    assert my_config.scenario.year == 2015
    assert my_config.run.initial_components == (
        # "create_tod_scenarios",
        # "active_modes",
        "air_passenger",
        "prepare_network_highway",
        "highway",
        "highway_maz_skim",
        "prepare_network_transit",
        "transit_assign",
        "transit_skim",
    )
    assert my_config.time_periods[1].name == "am"
    assert my_config.highway.maz_to_maz.operating_cost_per_mile == 18.93
    assert len(my_config.time_periods) == 5
    assert my_config.highway.classes[0].description == "drive alone"
    assert my_config.logging.log_file_path.startswith("tm2py_debug_")
    assert my_config.logging.log_file_path.endswith(".log")
    assert my_config.logging.display_level == "STATUS"


def test_config_read_badfile():
    """Should have good behavior when file isn't there."""
    from tm2py.config import Configuration

    try:
        Configuration.load_toml("this_is_not_a_valid_file.toml")
        raise AssertionError("Should have thrown an exception.")
    except FileNotFoundError:
        pass
