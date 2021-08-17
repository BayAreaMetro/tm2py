from typing import Union, Collection

import numpy as np

from ....logger import Logger
from ...component import Component
from ....controller import Controller

class PrepareDemand(Component):
    def __init__(self, controller: Controller):
        """Highway assignment and skims.
        Args:
            controller: parent Controller object
        """
        super().__init__(controller)
        self.iteration = controller.iteration.copy()
            
    def clean_demand(demand_matrix, num_zones):
        
        _shape = demand_matrix.shape
        if _shape != (num_zones, num_zones):
            demand = np.pad(
                demand_matrix, ((0, num_zones - _shape[0]), (0, num_zones - _shape[1]))
            )
        return demand

    def average_demand():
        """tktk

        TODO - COPY PASTED
        """
        prev_demand = matrix.get_numpy_data(scenario.id)
                    demand = prev_demand + (1.0 / msa_iteration) * (
                        demand - prev_demand
                    )
        matrix.set_numpy_data(demand, scenario.id)


    def initialize_demand(self, demand_matrix, time_period: str):
        """[summary]

        Args:
            time_period (str): [description]
        """
        if not self.controller.iteration <= 1:
            logger.warning()
        
        _demand_entry = demand_matrix.copy()
        _omx_filename = os.path.join(
            self.controller.run_dir,
            self.config.dir.demand,
            demand_matrix.file.format(time_period = time_period),
        )
        _demand = omx.get_matrix(_omx_filename, demand_matrix.matrix) 
        _demand = clean_demand(_demand,self.config.zones)

        _demand_entry["prepared demand"] = _demand
        _demand_entry["emme matrix"] = emmebank.create_matrix(
            _demand,
            name = _demand_entry.short_name,
            description = _demand_entry.name,
        )

        _demand_entry["emme matrix"].set_numpy_data(_demand, scenario.id)

        return _demand_entry


    def run(self, time_period:Union[Collection[str],str] = None):
        """Open combined demand files from omx and prepare for assignment.
        """
        if not time_period:
            time_period = self.time_periods
        if type(time_period) is Collection:
            demand = {tp: self.run(time_period = tp) for tp in time_period}
            return demand

        if self.iteration <= 1:
            yield {dm.name:self.initialize_demand(dm,time_period) for dm in self.config.highway.demand}
        else:
            yield {dm.name:self.average_demand(dm,time_period) for dm in self.config.highway.demand}
