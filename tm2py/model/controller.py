"""Main module docsting

TODO: looks like missing Emme project (bad path) MAY cause 
process to block on UI input (must click OK)

"""


import argparse as _argparse

# from contextlib import contextmanager as _context
import os as _os

# from tm2py.model.assignment.setup import PrepareEmmeNetworks
from tm2py.model.assignment.active_modes import ActiveModesSkim
from tm2py.model.assignment.highway import HighwayAssignment
from tm2py.model.assignment.transit import TransitAssignment
from tm2py.model.demand.air_passenger import AirPassenger
from tm2py.model.demand.household import HouseholdModel
from tm2py.model.demand.internal_external import InternalExternal
from tm2py.model.demand.truck import TruckModel

from tm2py.core.config import Configuration
from tm2py.core.logging import Logger
from tm2py.core.component import Controller as _Controller


_join, _dir = _os.path.join, _os.path.dirname


class Controller(_Controller):
    """docstring for Controller class"""

    def __init__(self, config_path: str=None):
        super().__init__()
        if config_path is None:
            config_path = _join(_os.getcwd(), "config.toml")
        else:
            config_path = config_path
        self._root_dir = _dir(config_path)
        self._config = Configuration(config_path)  # load config file
        self._logger = Logger(self)
        self._top_sheet = Logger(self)
        self._trace = None

        self._components = {
            # "prepare_emme_networks": PrepareEmmeNetworks(self),
            "active_modes": ActiveModesSkim(self),
            "air_passenger": AirPassenger(self),
            "household": HouseholdModel(self),
            "internal_external": InternalExternal(self),
            "truck": TruckModel(self),
            "highway": HighwayAssignment(self),
            "transit": TransitAssignment(self),
        }
        self._iteration = 0

    @property
    def iteration(self):
        """Current iteration of model"""
        return self._iteration

    def run(self):
        """Main interface to run model"""
        self.initialize()
        self.validate_inputs()

        # with self.setup():
        # self.run_prepare_emme_networks()
        self.run_active_mode_skim()
        self.run_air_passenger_model()
        self.run_highway_assignment()
        self.run_transit_assignment()
        # NOTE: E1101 due to dynamic generation of config from TOML file
        # pylint: disable=E1101
        start, end = self.config.run.start_iteration, self.config.run.global_iterations + 1
        for iteration in range(start, end):
            self._iteration = iteration
            self.run_household_model()
            self.run_internal_external_model()
            self.run_truck_model()
            # self.run_average_demand()
            self.run_highway_assignment()
            self.run_transit_assignment()

    def initialize(self):
        """Placeholder for initialization"""
        self._iteration = 0

    def validate_inputs(self):
        """Validate input state prior to run"""
        for component in self._components.values():
            component.validate_inputs()

#     def run_prepare_emme_networks(self):
#        """Run prepare emme network component"""

    def run_active_mode_skim(self):
        """Run prepare emme network component"""
        # NOTE: E1101 due to dynamic generation of config from TOML file
        if self.config.run.active_modes:  # pylint: disable=E1101
            self._components["active_modes"].run()

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

    def run_highway_assignment(self):
        """Run highway assignment and skims component"""
        if self.config.run.highway[self.iteration]:  # pylint: disable=E1101
            self._components["highway"].run()

    def run_transit_assignment(self):
        """Run transit assignment and skims component"""
        if self.config.run.transit[self.iteration]:  # pylint: disable=E1101
            self._components["transit"].run()


if __name__ == "__main__":
    parser = _argparse.ArgumentParser(description="Main: run MTC TM2PY")
    parser.add_argument("-a", "--argument", help=r"An argument")
    args = parser.parse_args()

    controller = Controller()
    # controller.argument = args.argument
    controller.run()
