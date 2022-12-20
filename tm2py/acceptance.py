"""Methods to create Acceptance Criteria summaries from a tm2py model run."""


from tm2py.config import Configuration
from typing import Union, List

import toml

class Acceptance:

    scenario_config: Configuration
    observed_dict: dict
    
    def make_acceptance(scenario_file: str, observed_config_file: str):

        scenario_config = Configuration.load_toml(path = scenario_file)

        with open(observed_config_file, "r", encoding="utf-8") as toml_file:
            observed_dict = toml.load(toml_file)

        # _validate() method to validate the scenario and observed data to fail fast
        # _make_roadway_network_comparisons() method to build roadway network comparisons


        return

    def _validate():

        #_validate_scenario()
        #_validate_observed()

        return

    def _make_roadway_network_comparisons(): 

        # placeholder    
        return