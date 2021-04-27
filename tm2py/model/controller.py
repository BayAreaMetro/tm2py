"""Main module docsting

"""


import argparse as _argparse
import os as _os

# import tm2py.core.tools as _tools

from contextlib import contextmanager as _context

# from tm2py.model.assignment.highway import HighwayAssignment
# from tm2py.assignment.transit import TransitAssignment
from tm2py.core.config import Configuration
from tm2py.core.logging import Logger


_join, _dir = _os.path.join, _os.path.dirname


class Controller:
    """docstring for Controller class"""

    def __init__(self):
        super().__init__()
        config_path = _join(_os.getcwd(), "config.properties")
        self._config = Configuration(config_path)  # load config file
        self._logger = Logger(self)
        self._top_sheet = Logger(self)
        self._trace = None

        self.create_emme_networks = None
        self.non_motorized_skim = None
        self.airport_model = None
        self.start_household_manager = None  # HouseholdManager(self)
        self.start_matrix_manager = None  # MatrixManager(self)
        self.resident_model = None  # ResidentModel(self)
        self.stop_java = None  # StopJava(self)
        self.internal_external_model = None  # InternalExternalModel(self)
        self.truck_model = None  # TruckModel(self)
        self.average_demand = None  # AverageDemand(self)
        self.highway_assignment = None  # HighwayAssignment(self)
        self.transit_assignment = None  # TransitAssignment(self)

        self._components = [
            self.create_emme_networks,
            self.non_motorized_skim,
            self.airport_model,
            self.start_household_manager,
            self.start_matrix_manager,
            self.resident_model,
            self.stop_java,
            self.internal_external_model,
            self.truck_model,
            self.average_demand,
            self.highway_assignment,
            self.transit_assignment,
        ]
        self._iteration = 0

    @property
    def config(self):
        """Return configuration interface"""
        return self._config

    @property
    def top_sheet(self):
        """Placeholder for topsheet interface"""
        return self._top_sheet

    @property
    def logger(self):
        """Placeholder for logger interface"""
        return self._logger

    @property
    def trace(self):
        """Trace information"""
        return self._trace

    @property
    def iteration(self):
        """Current iteration of model"""
        return self._iteration

    def run(self):
        """Main interface to run model"""
        self.initialize()
        self.validate_inputs()

        with self.setup():
            self.create_emme_networks.run()
            self.non_motorized_skim.run()
            self.airport_model.run()

            self.highway_assignment.run()
            self.transit_assignment.run()
            # self.export_skims.run()
            # or Export skims embeded in above assignment steps?
            for iteration in self._config.global_iterations:
                self._iteration = iteration
                self.start_household_manager.run()
                # call CTRAMP\runtime\runHhMgr.cmd "%JAVA_PATH%" %HOST_IP_ADDRESS%
                self.start_matrix_manager.run()
                # call CTRAMP\runtime\runMtxMgr.cmd %HOST_IP_ADDRESS% "%JAVA_PATH%"
                self.resident_model.run()
                # call CTRAMP\runtime\runMTCTM2ABM.cmd %SAMPLERATE% %ITERATION% "%JAVA_PATH%"
                self.stop_java.run()
                # taskkill /im "java.exe" /F
                self.internal_external_model.run()
                self.truck_model.run()
                self.average_demand.run()
                self.highway_assignment.run()
                self.transit_assignment.run()

    def initialize(self):
        """Placeholder for initialization"""
        self._iteration = 0

    def validate_inputs(self):
        """Validate input state prior to run"""
        for component in self._components:
            component.validate_inputs()

    @_context
    def setup(self):
        """Placeholder setup and teardown"""
        try:
            yield
        finally:
            pass


if __name__ == "__main__":
    parser = _argparse.ArgumentParser(description="Main: run MTC TM2.1")
    parser.add_argument("-a", "--argument", help=r"An argument")
    args = parser.parse_args()

    controller = Controller()
    # controller.argument = args.argument
    controller.run()
