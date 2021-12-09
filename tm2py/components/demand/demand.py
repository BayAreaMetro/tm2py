"""Demand loading from OMX to Emme database"""

from typing import Dict, Union, Collection, List
import numpy as np
import openmatrix as _omx

# from ...logger import Logger
from ..component import Component
from ...controller import RunController
from ...emme.manager import EmmeManager


class PrepareDemand(Component):
    """Import and average highway demand.

    Demand is imported from OMX files based on reference file paths and OMX
    matrix names in highway assignment config (highway.classes).
    The demand is average using MSA with the current demand matrices if the
    controller.iteration > 1.

    Args:
        controller: parent RunController object
    """

    def __init__(self, controller: RunController):
        super().__init__(controller)
        self._emmebank = None

    # @LogStartEnd("prepare highway demand")
    def run(self, time_periods: Union[Collection[str], str] = None):
        """Open combined demand OMX files from demand models and prepare for assignment.

        Args:
            time_periods: list of str names of time_periods, or name of a single time_period
        """
        if time_periods is None:
            time_periods = [tp.name for tp in self.config.time_periods]
        elif not isinstance(time_periods, Collection):
            time_periods = [time_periods]
        emme_manager = EmmeManager()
        emmebank_path = self.get_abs_path(self.config.emme.highway_database_path)
        self._emmebank = emme_manager.emmebank(emmebank_path)
        record = {}
        for period in time_periods:
            record[period] = tp_record = {}
            for klass in self.config.highway.classes:
                tp_record[klass.name] = self.prepare_demand(
                    klass.name, klass.description, klass.demand, period
                )
        return record

    def prepare_demand(
        self,
        name: str,
        description: str,
        demand_config: List[Dict[str, Union[str, float]]],
        time_period: str,
    ):
        """Load demand from OMX files and save to Emme matrix for highway assignment.

        Average with previous demand (MSA) if the current iteration (self.con

        Args:
            name (str): the name of the highway assignment class
            description (str): the description for the highway assignment class
            demand_config (dict): the list of file cross-reference(s) for the demand to be loaded
                {"source": <name of demand model component>,
                 "name": <OMX key name>,
                 "factor": <factor to apply to demand in this file>}
            time_period (str):
        """
        scenario_id = {tp.name: tp.emme_scenario_id for tp in self.config.time_periods}[
            time_period
        ]
        num_zones = len(self._emmebank.scenario(scenario_id).zone_numbers)
        demand = self._read_demand(demand_config[0], time_period, num_zones)
        for file_config in demand_config[1:]:
            demand = demand + self._read_demand(file_config, time_period, num_zones)
        demand_name = f"{time_period}_{name}"
        matrix = self._emmebank.matrix(f'mf"{demand_name}"')
        msa_iteration = self.controller.iteration
        if msa_iteration <= 1:
            if not matrix:
                ident = self._emmebank.available_matrix_identifier("FULL")
                matrix = self._emmebank.create_matrix(ident)
                matrix.name = demand_name
                matrix.description = f"{time_period} {description} demand"
        else:
            if not matrix:
                raise Exception(
                    f"error averaging demand: matrix {demand_name} does not exist"
                )
            prev_demand = matrix.get_numpy_data(scenario_id)
            demand = prev_demand + (1.0 / msa_iteration) * (demand - prev_demand)

        matrix.set_numpy_data(demand, scenario_id)
        record = {"emme matrix": matrix, "name": demand_name}
        # record["prepared demand"] = demand
        return record

    def _read_demand(self, file_config, time_period, num_zones):
        # Load demand from cross-referenced source file,
        # the named demand model component under the key highway_demand_file
        source = file_config["source"]
        name = file_config["name"].format(period=time_period.upper())
        factor = file_config.get("factor")
        path = self.get_abs_path(self.config[source].highway_demand_file)
        omx_file = _omx.open_file(
            self.get_abs_path(path.format(period=time_period)), "r"
        )
        demand = omx_file[name].read()
        omx_file.close()
        if factor is not None:
            demand = factor * demand
        demand = self._redim_demand(demand, num_zones)
        return demand

    @staticmethod
    def _redim_demand(demand, num_zones):
        _shape = demand.shape
        if _shape != (num_zones, num_zones):
            demand = np.pad(
                demand, ((0, num_zones - _shape[0]), (0, num_zones - _shape[1]))
            )
        return demand
