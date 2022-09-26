"""Demand loading from OMX to Emme database."""

from __future__ import annotations

import itertools
from abc import ABC
from typing import TYPE_CHECKING, Dict, List, Union

import numpy as np

from tm2py.components.component import Component
from tm2py.emme.manager import Emmebank
from tm2py.emme.matrix import OMXManager
from tm2py.logger import LogStartEnd
from tm2py.matrix import redim_matrix

if TYPE_CHECKING:
    from tm2py.controller import RunController

NumpyArray = np.array


class EmmeDemand:
    """Abstract base class to import and average demand."""

    def __init__(self, controller: RunController):
        """Constructor for PrepareDemand class.

        Args:
            controller (RunController): Run controller for the current run.
        """
        super().__init__(controller)
        self._emmebank = None
        self._scenario = None
        self._source_ref_key = None

    def _read(self, path: str, name: str, factor: float = None) -> NumpyArray:
        """Read matrix array from OMX file at path with name, and multiple by factor (if specified).

        Args:
            path: full path to OMX file
            name: name of the OMX matrix / key
            factor: optional factor to apply to matrix
        """
        with OMXManager(path, "r") as omx_file:
            demand = omx_file.read(name)
        if factor is not None:
            demand = factor * demand
        demand = self._redim_demand(demand)
        # self.logger.log(f"{name} sum: {demand.sum()}", level=3)
        return demand

    def _save_demand(
        self,
        name: str,
        demand: NumpyArray,
        description: str = None,
        apply_msa: bool = False,
    ):
        """Save demand array to Emme matrix with name, optional description.

        Matrix will be created if it does not exist and the model is on iteration 0.

        Args:
            name: name of the matrix in the Emmebank
            demand: NumpyArray, demand array to save
            description: str, optional description to use in the Emmebank
            apply_msa: bool, default False: use MSA on matrix with current array
                values if model is on iteration >= 1
        """
        matrix = self.emmebank.matrix(f'mf"{name}"')
        msa_iteration = self.controller.iteration
        if not apply_msa or msa_iteration <= 1:
            if not matrix:
                ident = self._emmebank.available_matrix_identifier("FULL")
                matrix = self._emmebank.create_matrix(ident)
                matrix.name = name
                if description is not None:
                    matrix.description = description
        else:
            if not matrix:
                raise Exception(f"error averaging demand: matrix {name} does not exist")
            prev_demand = matrix.get_numpy_data(self._scenario.id)
            demand = prev_demand + (1.0 / msa_iteration) * (demand - prev_demand)
        self.logger.log(f"{name} sum: {demand.sum()}", level="DEBUG")
        matrix.set_numpy_data(demand, self._scenario.id)


def avg_matrix_msa(
    prev_avg_matrix: NumpyArray, this_iter_matrix: NumpyArray, msa_iteration: int
) -> NumpyArray:
    """Average matrices based on Method of Successive Averages (MSA).

    Args:
        prev_avg_matrix (NumpyArray): Previously averaged matrix
        this_iter_matrix (NumpyArray): Matrix for this iteration
        msa_iteration (int): MSA iteration

    Returns:
        NumpyArray: MSA Averaged matrix for this iteration.
    """
    if msa_iteration < 1:
        return this_iter_matrix
    result_matrix = prev_avg_matrix + (1.0 / msa_iteration) * (
        this_iter_matrix - prev_avg_matrix
    )
    return result_matrix


class PrepareHighwayDemand(EmmeDemand):
    """Import and average highway demand.

    Demand is imported from OMX files based on reference file paths and OMX
    matrix names in highway assignment config (highway.classes).
    The demand is average using MSA with the current demand matrices
    (in the Emmebank) if the controller.iteration > 1.

    Args:
        controller: parent RunController object
    """

    def __init__(self, controller: RunController):
        """Constructor for PrepareHighwayDemand.

        Args:
            controller (RunController): Reference to run controller object.
        """
        super().__init__(controller)
        self._emmebank = None

    def validate_inputs(self):
        # TODO
        pass

    @property
    def emmebank(self):
        if self._emmebank == None:
            self._emmebank = self.emme_manager.highway_emmebank
        return self._emmebank

    # @LogStartEnd("prepare highway demand")
    def run(self):
        """Open combined demand OMX files from demand models and prepare for assignment."""

        self._create_zero_matrix()
        for time in self.time_period_names:
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
        _scenario = self._emmebank.scenario(time_period)
        demand = self._read_demand(demand_config[0], {"period": time_period.upper()})
        for file_config in demand_config[1:]:
            demand = demand + self._read_demand(
                file_config, {"period": time_period.upper()}
            )
        demand_name = f"{time_period}_{name}"
        description = f"{time_period} {description} demand"
        self._save_demand(demand_name, demand, description, apply_msa=True)


class PrepareTransitDemand(Component):
    """Import transit demand.

    Demand is imported from OMX files based on reference file paths and OMX
    matrix names in transit assignment config (transit.classes).
    The demand is average using MSA with the current demand matrices (in the
    Emmebank) if transit.apply_msa_demand is true if the
    controller.iteration > 1.

    """

    def __init__(self, controller: "RunController"):
        """Constructor for PrepareTransitDemand.

        Args:
            controller: RunController object.
        """
        super().__init__(controller)
        self.config = self.controller.config.transit
        self._transit_emmebank = None

    @property
    def transit_emmebank(self):
        if not self._transit_emmebank:
            self._transit_emmebank = self.controller.emme_manager.emmebank(
                self.get_abs_path(self.controller.config.emme.transit_database_path)
            )
        return self._transit_emmebank

    @LogStartEnd("Prepare transit demand")
    def run(self):
        """Open combined demand OMX files from demand models and prepare for assignment."""
        self._source_ref_key = "transit_demand_file"
        self.transit_emmebank.zero_matrix()
        _time_period_tclass = itertools.product(
            self.time_period_names, self.config.classes
        )
        for _time_period, _tclass in _time_period_tclass:
            self._prepare_demand(
                _tclass.skim_set_id, _tclass.description, _tclass.demand, _time_period
            )

    def _prepare_demand(
        self,
        name: str,
        description: str,
        demand_config: List[Dict[str, Union[str, float]]],
        time_period: str,
    ):
        """Load demand from OMX files and save to Emme matrix for transit assignment.

        Average with previous demand (MSA) if the current iteration > 1 and
        config.transit.apply_msa_demand is True

        Args:
            name (str): the name of the transit assignment class in the OMX files, usually a number
            description (str): the description for the transit assignment class
            demand_config (dict): the list of file cross-reference(s) for the demand to be loaded
                {"source": <name of demand model component>,
                 "name": <OMX key name>,
                 "factor": <factor to apply to demand in this file>}
            time_period (str): the time _time_period ID (name)
        """
        self._scenario = self.emmebank.scenario(time_period)
        str_format = {"period": time_period.upper(), "skim_set_id": name}
        demand = self._read_demand(demand_config[0], str_format)
        for file_config in demand_config[1:]:
            demand = demand + self._read_demand(file_config, str_format)
        demand_name = f"TRN_{name}_{time_period}"
        description = f"{time_period} {description} demand"
        apply_msa = self.config.transit.apply_msa_demand
        self._save_demand(demand_name, demand, description, apply_msa=apply_msa)
