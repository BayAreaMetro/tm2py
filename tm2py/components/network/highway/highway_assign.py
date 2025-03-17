"""Highway assignment and skim component.

Performs equilibrium traffic assignment and generates resulting skims.
The assignmend is configured using the "highway" table in the source config.
See the config documentation for details. The traffic assignment runs according
to the list of assignment classes under highway.classes.

Other relevant parameters from the config are:
- emme.num_processors: number of processors as integer or "MAX" or "MAX-N"
- time_periods[].emme_scenario_id: Emme scenario number to use for each period
- time_periods[].highway_capacity_factor

The Emme network must have the following attributes available:

Link - attributes:
- "length" in feet
- "vdf", volume delay function (volume delay functions must also be setup)
- "@useclass", vehicle-class restrictions classification, auto-only, HOV only
- "@free_flow_time", the free flow time (in minutes)
- "@tollXX_YY", the toll for period XX and class subgroup (see truck
    class) named YY, used together with @tollbooth to generate @bridgetoll_YY
    and @valuetoll_YY
- "@maz_flow", the background traffic MAZ-to-MAZ SP assigned flow from highway_maz,
    if controller.iteration > 0
- modes: must be set on links and match the specified mode codes in
    the traffic config

 Network results - attributes:
- @flow_XX: link PCE flows per class, where XX is the class name in the config
- timau: auto travel time
- volau: total assigned flow in PCE

Notes:
- Output matrices are in miles, minutes, and cents (2010 dollars) and are stored/
as real values;
- Intrazonal distance/time is one half the distance/time to the nearest neighbor;
- Intrazonal bridge and value tolls are assumed to be zero
"""

from __future__ import annotations

import argparse
import json as _json
import os
import time as _time
from contextlib import contextmanager as _context
from copy import deepcopy as _copy
from typing import TYPE_CHECKING, Dict, List, Union

import numpy as np

from tm2py import tools
from tm2py.components.component import Component, Subcomponent
from tm2py.components.demand.prepare_demand import PrepareHighwayDemand
from tm2py.components.network.highway.highway_emme_spec import AssignmentSpecBuilder
from tm2py.emme.manager import (
    EmmeScenario,
    EmmeManager,
    EmmeManagerLight,
    BaseAssignmentLauncher,
    Emmebank,
)
from tm2py.emme.matrix import MatrixCache, OMXManager
from tm2py.emme.network import NetworkCalculator
from tm2py.logger import LogStartEnd, ProcessLogger


if TYPE_CHECKING:
    from tm2py.controller import RunController


class HighwayAssignment(Component):
    """Highway assignment and skims.
    Args:
        controller: parent RunController object
    """

    def __init__(self, controller: "RunController"):
        """Constructor for HighwayAssignment components.

        Args:
            controller (RunController): Reference to current run controller.
        """
        super().__init__(controller)

        self.config = self.controller.config.highway
        self._highway_emmebank = None

    @property
    def highway_emmebank(self):
        if not self._highway_emmebank:
            self._highway_emmebank = self.controller.emme_manager.highway_emmebank
        return self._highway_emmebank

    @property
    def classes(self):
        return [c.name for c in self.config.classes]

    @property
    def class_config(self):
        if not self._class_config:
            self._class_config = {c.name: c for c in self.config.classes}

        return self._class_config

    def validate_inputs(self):
        """Validate inputs files are correct, raise if an error is found."""
        # TODO
        pass

    @LogStartEnd("Highway assignment and skims", level="STATUS")
    def run(self):
        """Run highway assignment."""
        demand = PrepareHighwayDemand(self.controller)
        if self.controller.iteration == 0:
            self.highway_emmebank.create_zero_matrix()
            if self.controller.config.warmstart.warmstart:
                if self.controller.config.warmstart.use_warmstart_demand:
                    demand.run()
        else:
            demand.run()

        distribution = self.controller.config.emme.highway_distribution
        if distribution:
            launchers = self.setup_process_launchers(distribution[:-1])
            self.start_proccesses(launchers)
            # Run last configuration in process
            in_process_times = distribution[-1].time_periods
            num_processors = distribution[-1].num_processors
            self.run_in_process(in_process_times, num_processors)
            self.wait_for_processes(launchers)
        else:
            num_processors = self.controller.emme_manager.num_processors
            self.run_in_process(self.time_period_names, num_processors)

    def run_in_process(self, times, num_processors):
        self.logger.status(f"Running highway assignments in process: {', '.join(times)}")
        iteration = self.controller.iteration
        for time in times:
            project_path = self.emme_manager.project_path
            emmebank_path = self.highway_emmebank.path
            params = self._get_assign_params(time, num_processors)
            runner = AssignmentRunner(
                project_path,
                emmebank_path,
                iteration=iteration,
                logger=self.logger,
                **params,
            )
            runner.run()

    def setup_process_launchers(self, distribution):
        self.logger.status(
            f"Running highway assignments in {len(distribution)} separate processes"
        )
        iteration = self.controller.iteration
        warmstart = self.controller.config.warmstart.warmstart
        launchers = []
        time_params = {}
        for config in distribution:
            assign_launcher = AssignmentLauncher(
                self.highway_emmebank.emmebank, iteration
            )
            launchers.append(assign_launcher)
            for time in config.time_periods:
                params = self._get_assign_params(time, config.num_processors)
                assign_launcher.add_run(**params)
                time_params[time] = params

        # initialize all skim matrices - complete all periods in order
        for time in self.time_period_names:
            scenario = self.highway_emmebank.scenario(time)
            params = time_params.get(time)
            if params:
                for matrix_name in params["skim_matrices"]:
                    self.highway_emmebank.create_matrix(matrix_name, "FULL")

        return launchers

    def start_proccesses(self, launchers):
        for i, assign_launcher in enumerate(launchers):
            self.logger.status(f"Starting highway assignment process {i} {', '.join(assign_launcher.times)}")
            assign_launcher.setup()
            assign_launcher.run()

    def wait_for_processes(self, launchers):
        self.logger.status(f"Waiting for highway assignments to complete...")
        while launchers:
            _time.sleep(5)
            for assign_launcher in launchers[:]:
                if not assign_launcher.is_running:
                    self.logger.status(f"... assignment process complete for time(s): {', '.join(assign_launcher.times)}")
                    assign_launcher.teardown()
                    launchers.remove(assign_launcher)

    def _get_assign_params(self, time, num_processors):
        iteration = self.controller.iteration
        warmstart = self.controller.config.warmstart.warmstart
        builder = AssignmentSpecBuilder(
            time, iteration, warmstart, self.config, num_processors
        )
        # Must match signature of manager.BaseAssignmentLauncher.add_run
        #       time, scenario, assign_spec, demand_matrices, skim_matrices, omx_file_path
        params = dict(
            time=time,
            scenario_id=self.highway_emmebank.scenario(time).id,
            assign_spec=builder.assignment_spec,
            demand_matrices=builder.demand_matrices,
            skim_matrices=builder.skim_matrices,
            omx_file_path=self.get_abs_path(
                self.config.output_skim_path
                / self.config.output_skim_filename_tmpl.format(time_period=time)
            ),
        )
        self.logger.debug(
            "_get_assign_params: self.config.output_skim_path:{}".format(
                self.config.output_skim_path
            )
        )
        with self.logger._skip_emme_logging():
            self.logger.debug("_get_assign_params: params dictionary")
            self.logger.log_dict(params, level="DEBUG")
        return params


class AssignmentLauncher(BaseAssignmentLauncher):

    def get_assign_script_path(self):
        return __file__

    def get_config(self):
        config = []
        params = zip(
            self._times,
            self._scenarios,
            self._assign_specs,
            self._skim_matrices,
            self._demand_matrices,
            self._omx_file_paths,
        )
        for time, scenario, spec, skims, demands, omx_path in params:
            config.append(
                {
                    "project_path": self._run_project_path,
                    "emmebank_path": self._run_emmebank_path,
                    "scenario_id": scenario.id,
                    "time": time,
                    "iteration": self._iteration,
                    "assign_spec": spec,
                    "demand_matrices": demands,
                    "skim_matrices": skims,
                    "omx_file_path": omx_path,
                }
            )
        return config

    def get_result_attributes(self, scenario_id: str):
        attrs = ["auto_time", "auto_volume"]
        for scenario, spec in zip(self._scenarios, self._assign_specs):
            if scenario.id == scenario_id:
                attrs.extend([c["results"]["link_volumes"] for c in spec["classes"]])
        return attrs


# runs the actual assignment in local process (can also run in current process)
class AssignmentRunner:

    def __init__(
        self,
        project_path: str,
        emmebank_path: str,
        scenario_id: Union[str, int],
        time: str,
        iteration: int,
        assign_spec: Dict,
        demand_matrices: List[str],
        skim_matrices: List[str],
        omx_file_path: str,
        logger=None,
    ):
        """
        Constructor to run the highway assignment for the specified time period.

        Args:
            project_path (str): path to existing EMME project (*.emp file)
            emmebank_path (str): path to existing EMME databsae (emmebank) file
            scenario_id (str): existing scenario ID for assignment run
            time (str): time period ID (only used for logging messages)
            iteration (List[str]): global iteration number
            assign_spec (Dict): EMME SOLA assignment specification
            skim_matrices (List[str]): list of skim matrix ID.
            omx_file_path (str): path to resulting output of skim matrices to OMX
            logger (Logger): optional logger object if running in process. If not specified a new logger reference is created.
        """
        self.emme_manager = EmmeManagerLight(project_path)
        self.emme_manager.add_database(emmebank_path)
        self.emmebank = Emmebank(emmebank_path)
        self.scenario = self.emmebank.scenario(scenario_id)

        self.time = time
        self.iteration = iteration
        self.assign_spec = assign_spec
        self.skim_matrix_ids = skim_matrices
        self.demand_matrix_ids = demand_matrices
        self.omx_file_path = omx_file_path

        self._matrix_cache = None
        self._skim_matrix_objs = []
        if logger:
            self.logger = logger
        else:
            root = os.path.dirname(os.path.dirname(project_path))
            name = f"run_highway_{time}_{iteration}"
            run_log_file_path = os.path.join(root, f"{name}.log")
            log_on_error_file_path = os.path.join(root, f"{name}_error.log")
            self.logger = ProcessLogger(
                run_log_file_path, log_on_error_file_path, self.emme_manager
            )

    def run(self):
        with self._setup():
            if self.iteration > 0:
                self._copy_maz_flow()
            else:
                self._reset_background_traffic()
            for matrix_name in self.demand_matrix_ids:
                if not self.emmebank.matrix(matrix_name):
                    raise Exception(f"demand matrix {matrix_name} does not exist")

            self._create_skim_matrices()
            with self.logger._skip_emme_logging():
                self.logger.log_dict(self.assign_spec, level="DEBUG")
            with self.logger.log_start_end(
                "Run SOLA assignment (no path analyses)", level="INFO"
            ):
                assign = self.emme_manager.tool(
                    "inro.emme.traffic_assignment.sola_traffic_assignment"
                )
                assign(
                    self.assign_spec_no_analysis, self.scenario, chart_log_interval=1
                )

            with self.logger.log_start_end(
                "Calculates link level LOS based reliability", level="DETAIL"
            ):
                exf_pars = self.scenario.emmebank.extra_function_parameters
                vdfs = [
                    f for f in self.emmebank.functions() if f.type == "VOLUME_DELAY"
                ]
                net_calc = self._network_calculator
                for function in vdfs:
                    expression = function.expression
                    for el in ["el1", "el2", "el3", "el4"]:
                        expression = expression.replace(el, getattr(exf_pars, el))
                    if "@static_rel" in expression:
                        # split function into time component and reliability component
                        time_expr, reliability_expr = expression.split(
                            "*(1+@static_rel+"
                        )
                        net_calc.add_calc(
                            "@auto_time",
                            time_expr,
                            {"link": "vdf=%s" % function.id[2:]},
                        )
                        net_calc.add_calc(
                            "@reliability",
                            "(@static_rel+" + reliability_expr,
                            {"link": "vdf=%s" % function.id[2:]},
                        )
                net_calc.add_calc("@reliability_sq", "@reliability**2")
                net_calc.run()

            with self.logger.log_start_end(
                "Run SOLA assignment with path analyses and highway reliability",
                level="INFO",
            ):
                assign(self.assign_spec, self.scenario, chart_log_interval=1)

            # Subtract non-time costs from gen cost to get the raw travel time
            self._calc_time_skims()
            # Set intra-zonal for time and dist to be 1/2 nearest neighbour
            self._set_intrazonal_values()
            self._export_skims()
            # if self.logger.debug_enabled:
            #     self._log_debug_report(scenario, time)

    @property
    def assign_spec_no_analysis(self):
        """Return modified SOLA assignment specification with no analyses."""
        spec = _copy(self.assign_spec)
        for emme_cls_spec in spec["classes"]:
            del emme_cls_spec["path_analyses"]
        return spec

    @_context
    def _setup(self):
        """Setup and teardown for Emme Matrix cache and list of skim matrices."""
        with self.logger.log_start_end(
            f"Run {self.time} highway assignment", level="STATUS"
        ):
            self._matrix_cache = MatrixCache(self.scenario)
            self._skim_matrix_objs = []
            self._network_calculator = NetworkCalculator(self.emme_manager, self.scenario)
            try:
                yield
            finally:
                self._matrix_cache.clear()
                self._matrix_cache = None
                self._skim_matrix_objs = []
                self._network_calculator = None

    def _copy_maz_flow(self):
        """Copy maz_flow from MAZ demand assignment to ul1 for background traffic."""
        self._network_calculator("ul1", "0")

    def _reset_background_traffic(self):
        """Set ul1 for background traffic to 0 (no maz-maz flow)."""
        self.logger.log(
            "Set ul1 to 0 for background traffic", indent=True, level="DETAIL"
        )
        self._network_calculator("ul1", "@maz_flow")

    def _create_skim_matrices(self):
        """Create matrices to store skim results in Emme database.

        Also add the matrices to list of self._skim_matrix_objs.
        """
        create_matrix = self.emme_manager.tool("inro.emme.data.matrix.create_matrix")
        with self.logger.log_start_end("Creating skim matrices", level="DETAIL"):
            for matrix_name in self.skim_matrix_ids:
                matrix = self.emmebank.matrix(f'mf"{matrix_name}"')
                if not matrix:
                    matrix = create_matrix(
                        "mf", matrix_name, scenario=self.scenario, overwrite=True
                    )
                self._skim_matrix_objs.append(matrix)

    def _calc_time_skims(self):
        """Calculate the real time skim =gen_cost-per_fac*link_costs."""
        for emme_class_spec in self.assign_spec["classes"]:
            od_travel_times = emme_class_spec["results"]["od_travel_times"][
                "shortest_paths"
            ]
            if od_travel_times is not None:
                # Total link costs is always the first analysis
                cost = emme_class_spec["path_analyses"][0]["results"]["od_values"]
                factor = emme_class_spec["generalized_cost"]["perception_factor"]
                gencost_data = self._matrix_cache.get_data(od_travel_times)
                cost_data = self._matrix_cache.get_data(cost)
                time_data = gencost_data - (factor * cost_data)
                self._matrix_cache.set_data(od_travel_times, time_data)

    def _set_intrazonal_values(self):
        """Set the intrazonal values to 1/2 nearest neighbour for time and distance skims."""
        for matrix in self._skim_matrix_objs:
            if matrix.name.endswith(("time", "dist")):
                self.logger.debug(f"Setting intrazonals to 0.5*min for {matrix.name}")
                data = self._matrix_cache.get_data(matrix.name)
                # NOTE: sets values for external zones as well
                np.fill_diagonal(data, np.inf)
                data[np.diag_indices_from(data)] = 0.5 * np.nanmin(data, 1)
                self._matrix_cache.set_data(matrix.name, data)

    def _export_skims(self):
        """Export skims to OMX files by period."""
        # NOTE: skims in separate file by period
        self.logger.debug(
            f"export {len(self._skim_matrix_objs)} skim matrices to {self.omx_file_path}"
        )
        os.makedirs(os.path.dirname(self.omx_file_path), exist_ok=True)
        with OMXManager(
            self.omx_file_path, "w", self.scenario, matrix_cache=self._matrix_cache
        ) as omx_file:
            omx_file.write_matrices(self._skim_matrix_objs)

    def _log_debug_report(self, scenario: EmmeScenario, time_period: str):
        num_zones = len(scenario.zone_numbers)
        num_cells = num_zones * num_zones
        self.logger.debug(f"Highway skim summary for period {time_period}")
        self.logger.debug(
            f"Number of zones: {num_zones}. Number of O-D pairs: {num_cells}. "
            "Values outside -9999999, 9999999 are masked in summaries."
        )
        self.logger.debug(
            "name                            min       max      mean           sum"
        )
        for matrix in self._skim_matrices:
            values = self._matrix_cache.get_data(matrix)
            data = np.ma.masked_outside(values, -9999999, 9999999)
            stats = (
                f"{matrix.name:25} {data.min():9.4g} {data.max():9.4g} "
                f"{data.mean():9.4g} {data.sum(): 13.7g}"
            )
            self.logger.debug(stats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="RunHighwayAssignment", usage="%(prog)s [config]"
    )
    parser.add_argument("--config", help="path to config json kwargs or list of kwargs")
    args = parser.parse_args()
    with open(args.config, "r") as f:
        config = _json.load(f)
    if not isinstance(config, list):
        config = [config]
    for kwargs in config:
        runner = AssignmentRunner(**kwargs)
        runner.run()
