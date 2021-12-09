import os

import pytest

EXAMPLE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples"
)
TEST_CONFIG = os.path.join(EXAMPLE_DIR, "run_config.toml")


def test_config_read():
    """Configuration should load parameters to the correct namespaces."""
    from tm2py.config import Configuration

    my_config = Configuration(TEST_CONFIG)

    assert my_config.run.start_iteration == 0
    assert my_config.run.end_iteration == 1
    assert my_config.scenario.year == 2015
    assert my_config.run.initial_components == [
        "create_tod_scenarios",
        "active_modes",
        "air_passenger",
        "highway_maz_maz",
        "highway",
        "transit"
    ]
    assert len(my_config.time_periods) == 5
    assert my_config.highway.classes[0].description == "drive alone"


@pytest.mark.xfail
def test_config_read_badfile():
    """Should have good behavior when file isn't there."""
    from tm2py.config import Configuration

    my_config = Configuration("this_is_not_a_valid_file.toml")
