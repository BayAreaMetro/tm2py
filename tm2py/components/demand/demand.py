"""Demand loading from OMX to Emme database"""
from __future__ import annotations
from abc import ABC
from typing import Dict, Union, List, TYPE_CHECKING
import numpy as np

from tm2py.components.component import Component
from tm2py.emme.matrix import OMXManager

if TYPE_CHECKING:
    from tm2py.controller import RunController


class PrepareDemand(Component, ABC):
    """Abstract base class to import and average demand."""

    def __init__(self, controller: RunController):
        super().__init__(controller)
        self._emmebank = None

    def _read(self, path, name, num_zones, factor=None):
        with OMXManager(path, "r") as omx_file:
            demand = omx_file.read(name)
        if factor is not None:
            demand = factor * demand
        demand = self._redim_demand(demand, num_zones)
        return demand

    @staticmethod
    def _redim_demand(demand, num_zones):
        _shape = demand.shape
        if _shape < (num_zones, num_zones):
            demand = np.pad(
                demand, ((0, num_zones - _shape[0]), (0, num_zones - _shape[1]))
            )
        elif _shape > (num_zones, num_zones):
            ValueError(f"Provided demand matrix is larger ({_shape}) than the \
                specified number of zones: {num_zones}")
        
        return demand

    # Disable too many arguments recommendation
    # pylint: disable=R0913
    def _save_demand(self, name, demand, scenario, description="", apply_msa=False):
        matrix = self._emmebank.matrix(f'mf"{name}"')
        msa_iteration = self.controller.iteration
        if not apply_msa or msa_iteration <= 1:
            if not matrix:
                ident = self._emmebank.available_matrix_identifier("FULL")
                matrix = self._emmebank.create_matrix(ident)
                matrix.name = name
                matrix.description = description
        else:
            if not matrix:
                raise Exception(f"error averaging demand: matrix {name} does not exist")
            prev_demand = matrix.get_numpy_data(scenario.id)
            demand = prev_demand + (1.0 / msa_iteration) * (demand - prev_demand)

        matrix.set_numpy_data(demand, scenario.id)

    def _create_zero_matrix(self):
        zero_matrix = self._emmebank.matrix('ms"zero"')
        if zero_matrix is None:
            ident = self._emmebank.available_matrix_identifier("SCALAR")
            zero_matrix = self._emmebank.create_matrix(ident)
            zero_matrix.name = "zero"
            zero_matrix.description = "zero demand matrix"
        zero_matrix.data = 0


class PrepareHighwayDemand(PrepareDemand):
    """Import and average highway demand.

    Demand is imported from OMX files based on reference file paths and OMX
    matrix names in highway assignment config (highway.classes).
    The demand is average using MSA with the current demand matrices
    (in the Emmebank) if the controller.iteration > 1.

    Args:
        controller: parent RunController object
    """

    def __init__(self, controller: RunController):
        super().__init__(controller)
        self._emmebank_path = None

    # @LogStartEnd("prepare highway demand")
    def run(self):
        """Open combined demand OMX files from demand models and prepare for assignment."""
        self._emmebank_path = self.get_abs_path(self.config.emme.highway_database_path)

        self._emmebank = self.controller.emme_manager.emmebank(self._emmebank_path)
        self._create_zero_matrix()
        for time in self.time_period_names():
            for klass in self.config.highway.classes:
                self._prepare_demand(klass.name, klass.description, klass.demand, time)

    def _prepare_demand(
        self,
        name: str,
        description: str,
        demand_config: List[Dict[str, Union[str, float]]],
        time_period: str,
    ):
        """Load demand from OMX files and save to Emme matrix for highway assignment.

        Average with previous demand (MSA) if the current iteration > 1

        Args:
            name (str): the name of the highway assignment class
            description (str): the description for the highway assignment class
            demand_config (dict): the list of file cross-reference(s) for the demand to be loaded
                {"source": <name of demand model component>,
                 "name": <OMX key name>,
                 "factor": <factor to apply to demand in this file>}
            time_period (str): the time time_period ID (name)
        """
        scenario = self.get_emme_scenario(self._emmebank_path, time_period)
        num_zones = len(scenario.zone_numbers)
        demand = self._read_demand(demand_config[0], time_period, num_zones)
        for file_config in demand_config[1:]:
            demand = demand + self._read_demand(file_config, time_period, num_zones)
        demand_name = f"{time_period}_{name}"
        description = f"{time_period} {description} demand"
        self._save_demand(demand_name, demand, scenario, description, apply_msa=True)

    def _read_demand(self, file_config, time_period, num_zones):
        # Load demand from cross-referenced source file,
        # the named demand model component under the key highway_demand_file
        source = file_config["source"]
        name = file_config["name"].format(period=time_period.upper())
        factor = file_config.get("factor")
        path = self.get_abs_path(self.config[source].highway_demand_file)
        return self._read(path.format(period=time_period), name, num_zones, factor)


# class PrepareTransitDemand(PrepareDemand):
#     """Import transit demand."""
#
#     def run(self, time_period: Union[Collection[str], str] = None):
#         """Open combined demand OMX files from demand models and prepare for assignment.
#
#         Args:
#             time_period: list of str names of time_periods, or name of a single time_period
#         """
#         emmebank_path = self.get_abs_path(self.config.emme.transit_database_path)
#         self._emmebank = self.controller.emme_manager.emmebank(emmebank_path)
#         self._create_zero_matrix()
