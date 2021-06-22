"""Main module docsting

"""


import argparse as _argparse
import os as _os


from contextlib import contextmanager as _context

# from tm2py.model.assignment.setup import PrepareEmmeNetworks
from tm2py.model.assignment.highway import HighwayAssignment

# from tm2py.model.assignment.transit import TransitAssignment
# from tm2py.model.assignment.nonmotoized import NonMotorizedSkim
# from tm2py.model.demand.ctramp import ResidentModel
# from tm2py.model.demand.truck import TruckModel
# from tm2py.model.demand.airport import AirportModel
# from tm2py.model.demand.something? import AverageDemand
from tm2py.core.config import Configuration
from tm2py.core.logging import Logger
from tm2py.core.component import Controller as _Controller # NOTE: to be renamed


_join, _dir = _os.path.join, _os.path.dirname


class Controller(_Controller):
    """docstring for Controller class"""

    def __init__(self):
        super().__init__()
        config_path = _join(_os.getcwd(), "config.properties")
        self._config = Configuration(config_path)  # load config file
        self._logger = Logger(self)
        self._top_sheet = Logger(self)
        self._trace = None

        self._components = {
            # "prepare_emme_networks": PrepareEmmeNetworks(self),
            # "non_motorized_skim": NonMotorizedSkim(self),
            # "airport_model": AirportModel(self),
            # "resident_model": ResidentModel(self),
            # "internal_external_model": InternalExternalModel(self),
            # "truck_model": TruckModel(self),
            # "average_demand": AverageDemand(self),
            "highway_assignment": HighwayAssignment(self),
            #"transit_assignment": None,  # TransitAssignment(self),
        }
        # self._components = [a for a in self.__dict__.values() if isinstance(a, Component)]
        self._iteration = 0


    @property
    def iteration(self):
        """Current iteration of model"""
        return self._iteration

    def run(self):
        """Main interface to run model"""
        self.initialize()
        self.validate_inputs()

        with self.setup():
            self.run_prepare_emme_networks()
            self.run_non_motorized_skim()
            self.run_airport_model()
            self.run_highway_assignment()
            self.run_transit_assignment()
            # self.run_export_skims()
            # or Export skims embeded in above assignment steps?
            for iteration in range(self._config.run.global_iterations):
                self._iteration = iteration
                self.run_resident_model()
                self.run_internal_external_model()
                self.run_truck_model()
                self.run_average_demand()
                self.run_highway_assignment()
                self.run_transit_assignment()

    def initialize(self):
        """Placeholder for initialization"""
        self._iteration = 0

    def validate_inputs(self):
        """Validate input state prior to run"""
        for component in self._components.values():
            component.validate_inputs()


    def run_prepare_emme_networks(self):
        pass

    def run_non_motorized_skim(self):
        pass

    def run_airport_model(self):
        pass

    def run_resident_model(self):
        pass

    def run_internal_external_model(self):
        pass

    def run_truck_model(self):
        pass

    def run_average_demand(self):
        pass

    def run_highway_assignment(self):
        if self.config.run[0].highway == True:
            self._component["highway_assignment"].run()

    def run_transit_assignment(self):
        if self.config.run[0].transit == True:
            self._component["transit_assignment"].run()


if __name__ == "__main__":
    parser = _argparse.ArgumentParser(description="Main: run MTC TM2PY")
    parser.add_argument("-a", "--argument", help=r"An argument")
    args = parser.parse_args()

    controller = Controller()
    # controller.argument = args.argument
    controller.run()
