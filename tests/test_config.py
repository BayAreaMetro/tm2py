import os
import sys
from unittest.mock import MagicMock

import pytest

EXAMPLE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples"
)
TEST_CONFIG = os.path.join(EXAMPLE_DIR, "scenario_config.toml")
MODEL_CONFIG = os.path.join(EXAMPLE_DIR, "model_config.toml")


def test_config_read():
    """Configuration should load parameters to the correct namespaces."""
    # If (and only if) Emme is not installed, replace inro libraries with MagicMock
    try:
        import inro.emme.database.emmebank
    except ModuleNotFoundError:
        sys.modules["inro.emme.database.emmebank"] = MagicMock()
        sys.modules["inro.emme.network"] = MagicMock()
        sys.modules["inro.emme.database.scenario"] = MagicMock()
        sys.modules["inro.emme.database.matrix"] = MagicMock()
        sys.modules["inro.emme.network.node"] = MagicMock()
        sys.modules["inro.emme.desktop.app"] = MagicMock()
        sys.modules["inro"] = MagicMock()
        sys.modules["inro.modeller"] = MagicMock()
    from tm2py.config import Configuration

    my_config = Configuration.load_toml([TEST_CONFIG, MODEL_CONFIG])

    assert my_config.run.start_iteration == 0
    assert my_config.run.end_iteration == 1
    assert my_config.scenario.year == 2015
    assert my_config.time_periods[1].name == "am"
    assert my_config.highway.maz_to_maz.operating_cost_per_mile == 18.93
    assert len(my_config.time_periods) == 5
    assert my_config.highway.classes[0].description == "drive alone"


@pytest.mark.xfail
def test_config_read_badfile():
    """Should have good behavior when file isn't there."""
    from tm2py.config import Configuration

    Configuration.load_toml("this_is_not_a_valid_file.toml")
