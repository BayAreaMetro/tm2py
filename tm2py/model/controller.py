"""Main module docsting

TODO: looks like missing Emme project (bad path) MAY cause 
process to block on UI input (must click OK)

"""


import argparse as _argparse

# from contextlib import contextmanager as _context
import os
# from typing import Union, List

# from tm2py.model.assignment.setup import PrepareEmmeNetworks
from tm2py.model.assignment.active_modes import ActiveModesSkim
from tm2py.model.assignment.highway import HighwayAssignment
from tm2py.model.assignment.highway_maz import AssignMAZSPDemand, SkimMAZCosts
from tm2py.model.assignment.transit import TransitAssignment
from tm2py.model.assignment.drive_access_skims import DriveAccessSkims
from tm2py.model.demand.air_passenger import AirPassenger
from tm2py.model.demand.household import HouseholdModel
from tm2py.model.demand.internal_external import InternalExternal
from tm2py.model.demand.truck import TruckModel
from tm2py.model.prepare.create_tod_scenarios import CreateTODScenarios
from tm2py.model.prepare.prepare_transit_network import PrepareTransitNetwork

from tm2py.core.config import Configuration
from tm2py.core.logging import Logger, LogStartEnd
from tm2py.core.component import Controller as _Controller


class Controller(_Controller):
    """docstring for Controller class"""

    def __init__(self, scenario_config: str, model_config: str = None):
        super().__init__()
        self._root_dir = os.path.dirname(scenario_config)
        # load config file(s)
        if model_config is not None:
            self._config = Configuration([scenario_config, model_config])
        else:
            self._config = Configuration(scenario_config)
        self._logger = Logger(self)
        self._top_sheet = Logger(self)
        self._trace = None

        self._components = {
            "active_modes": ActiveModesSkim(self),
            "air_passenger": AirPassenger(self),
            "drive_access": DriveAccessSkims(self),
            "household": HouseholdModel(self),
            "internal_external": InternalExternal(self),
            "truck": TruckModel(self),
            "maz_maz_assign": AssignMAZSPDemand(self),
            "create_tod_scenarios": CreateTODScenarios(self),
            "maz_maz_skim": SkimMAZCosts(self),
            "highway": HighwayAssignment(self),
            "transit": TransitAssignment(self),
            "prepare_transit": PrepareTransitNetwork(self),
        }
        self._iteration = 0

    @property
    def iteration(self):
        """Current iteration of model"""
        return self._iteration

    @LogStartEnd("model run")
    def run(self):
        """Main interface to run model"""
        self.initialize()
        self.validate_inputs()

        # with self.setup():
        # NOTE: E1101 due to dynamic generation of config from TOML file
        # pylint: disable=E1101
        start, end = self.config.run.start_iteration, self.config.run.global_iterations + 1
        if start == 0:
            self.run_create_tod_scenarios()
            self.run_active_mode_skim()
            self.run_air_passenger_model()
            self.run_maz_maz_assign()  # initialize flow to 0
            self.run_highway_assignment()
            self.run_maz_maz_skim()
            self.run_drive_access_generation()
            self.run_transit_assignment()
            start = 1

        for iteration in range(start, end):
            self._iteration = iteration
            self.run_household_model()
            self.run_internal_external_model()
            self.run_truck_model()
            self.run_maz_maz_assign()
            self.run_highway_assignment()
            self.run_drive_access_generation()
            self.run_transit_assignment()

    def initialize(self):
        """Placeholder for initialization"""
        self._iteration = 0

    def validate_inputs(self):
        """Validate input state prior to run"""
        for component in self._components.values():
            component.validate_inputs()

    def run_create_tod_scenarios(self):
        """Run prepare emme network component"""
        # NOTE: E1101 due to dynamic generation of config from TOML file
        if self.config.run.create_tod_scenarios:  # pylint: disable=E1101
            self._components["create_tod_scenarios"].run()

    def run_active_mode_skim(self):
        """Run prepare emme network component"""
        # NOTE: E1101 due to dynamic generation of config from TOML file
        if self.config.run.active_modes:  # pylint: disable=E1101
            self._components["active_modes"].run()

    def run_drive_access_generation(self):
        if self.config.run.drive_access[self.iteration]:  # pylint: disable=E1101
            self._components["drive_access"].run()

    def run_air_passenger_model(self):
        """Run air passenger model component"""
        if self.config.run.air_passenger:  # pylint: disable=E1101
            self._components["air_passenger"].run()

    def run_household_model(self):
        """Run resident model component"""
        if self.config.run.household[self.iteration-1]:  # pylint: disable=E1101
            self._components["household"].run()

    def run_internal_external_model(self):
        """Run internal external component"""
        if self.config.run.internal_external[self.iteration-1]:  # pylint: disable=E1101
            self._components["internal_external"].run()

    def run_truck_model(self):
        """Run truck model component"""
        if self.config.run.truck[self.iteration-1]:  # pylint: disable=E1101
            self._components["truck"].run()

#     def run_average_demand(self):
#         """Run average demand component"""

    def run_maz_maz_skim(self):
        if self.config.run.highway_maz_maz[self.iteration]:  # pylint: disable=E1101
            self._components["maz_maz_skim"].run()

    def run_maz_maz_assign(self):
        if self.config.run.highway_maz_maz[self.iteration]:  # pylint: disable=E1101
            if self.iteration == 0:
                self._components["maz_maz_assign"].initialize_flow()
            else:
                self._components["maz_maz_assign"].run()

    def run_highway_assignment(self):
        """Run highway assignment and skims component"""
        if self.config.run.highway[self.iteration]:  # pylint: disable=E1101
            self._components["highway"].run()

    def run_transit_assignment(self):
        """Run transit assignment and skims component"""
        if self.config.run.transit[self.iteration]:  # pylint: disable=E1101
            self._components["prepare_transit"].run()
            self._components["transit"].run()


if __name__ == "__main__":
    parser = _argparse.ArgumentParser(description="Main: run MTC TM2PY")
    parser.add_argument("-a", "--argument", help=r"An argument")
    args = parser.parse_args()

    controller = Controller()
    # controller.argument = args.argument
    controller.run()
