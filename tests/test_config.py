import os
import pytest

EXAMPLE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples"
)
TEST_CONFIG = os.path.join(EXAMPLE_DIR, "run_config.toml")


def test_config_read():
    """Configuration should load parametres to the correct namespaces."""
    from tm2py.config import Configuration

    my_config = Configuration(TEST_CONFIG)

    assert my_config.global_iterations == 3
    assert my_config.scenario.inputs.highway == ""
    assert my_config.model_process.initialize_components == [
        "prepare_network",
        "air_passenger_demand",
        "active_mode_skim",
        "highway_assignment",
        "transit_assignment",
    ]
    assert len(my_config.time_periods) == 5
    assert my_config.highway.classes[0].name == "drive alone"


@pytest.mark.xfail
def test_config_read_badfile():
    """Should have good behavior when file isn't there."""
    from tm2py.config import Configuration

    my_config = Configuration("this_is_not_a_valid_file.toml")


def test_config_read_write_read():
    """If we read in the configuration that is written out by Configuration.save,
    it should result in the same configuration as the original.
    """
    from tm2py.config import Configuration
    import tempfile

    my_config_1 = Configuration(TEST_CONFIG)
    saved_config_file = os.path.join(tempfile.TemporaryDirectory(), "my_config.toml")
    my_config_1.save(saved_config_file)

    my_config_2 = Configuration(saved_config_file)

    assert my_config_1 == my_config_2
