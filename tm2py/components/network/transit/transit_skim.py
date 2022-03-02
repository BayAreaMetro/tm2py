"""Transit skims module"""

from __future__ import annotations
from collections import defaultdict as _defaultdict
from contextlib import contextmanager as _context
import os
from typing import Union, Collection, TYPE_CHECKING

import numpy as np

from tm2py.components.component import Component
from tm2py.emme.matrix import MatrixCache, OMXManager
from tm2py.logger import LogStartEnd
from tm2py import tools

if TYPE_CHECKING:
    from tm2py.controller import RunController


class TransitSkim(Component):
    """Run transit skims"""

    def __init__(self, controller: RunController):
        super().__init__(controller)
        self._num_processors = tools.parse_num_processors(
            self.config.emme.num_processors
        )
        self._matrix_cache = None
        self._skim_matrices = []
        self._time_period = None
        self._scenario = None
        self._network = None
        self._assign_class = None

    @LogStartEnd("Transit skims")
    def run(self, time_period: Union[Collection[str], str] = None):
        """Run highway assignment

        Args:
            time_period: list of str names of time_periods, or name of a single _time_period
        """
        for self._time_period in self._process_time_period(time_period):
            with self._setup():
                self._create_skim_matrices()
                for self._assign_class in self.config.transit.classes:
                    self._run_skims()
                self._mask_transfers()
                self._mask_allpen()
                self._export_skims()
                self._report()

    @_context
    def _setup(self):
        emmebank_path = self.get_abs_path(self.config.emme.transit_database_path)
        emmebank = self.controller.emme_manager.emmebank(emmebank_path)
        self._scenario = self.get_emme_scenario(emmebank, self._time_period)
        self._network = self._scenario.get_partial_network(
            ["TRANSIT_SEGMENT"], include_attributes=False
        )
        self._matrix_cache = MatrixCache(self._scenario)
        self._skim_matrices = []
        with self.logger.log_start_end(f"period {self._time_period}"):
            with self.controller.emme_manager.logbook_trace(
                f"Transit skims for period {self._time_period}"
            ):
                try:
                    yield
                finally:
                    self._skim_matrices = []
                    self._scenario = None
                    self._network = None
                    self._assign_class = None
                    self._matrix_cache.clear()
                    self._matrix_cache = None

    def _tmplt_skim_matrices(self, names_only=False):
        tmplt_matrices = [
            ("FIRSTWAIT", "first wait time"),
            ("XFERWAIT", "transfer wait time"),
            ("TOTALWAIT", "total wait time"),
            ("XFERS", "num transfers"),
            ("XFERWALK", "transfer walk time"),
            ("TOTALWALK", "total walk time"),
            ("TOTALIVTT", "total in-vehicle time"),
            ("FARE", "fare"),
        ]
        for mode in self.config.transit.modes:
            if mode.assign_type == "TRANSIT":
                tmplt_matrices.append(
                    (
                        f"{mode.short_name}IVTT",
                        f"{mode.name} in-vehicle travel time"[:40],
                    )
                )
        if self.config.transit.use_ccr:
            tmplt_matrices.extend(
                [
                    ("LINKREL", "Link reliability"),
                    ("CROWD", "Crowding penalty"),
                    ("EAWT", "Extra added wait time"),
                    ("CAPPEN", "Capacity penalty"),
                ]
            )
        if names_only:
            return [x[0] for x in tmplt_matrices]
        return tmplt_matrices

    def _create_skim_matrices(self):
        time = self._time_period
        scenario = self._scenario
        create_matrix = self.controller.emme_manager.tool(
            "inro.emme.data.matrix.create_matrix"
        )
        msg = f"Creating {time} skim matrices"
        with self.controller.emme_manager.logbook_trace(msg):
            for klass in self.config.transit.classes:
                for skim_name, skim_desc in self._tmplt_skim_matrices():
                    name = f"{time}_{klass.name}_{skim_name}"
                    desc = f"{time} {klass.description}: {skim_desc}"
                    matrix = scenario.emmebank.matrix(f'mf"{name}"')
                    if not matrix:
                        matrix = create_matrix(
                            "mf", name, desc, scenario=scenario, overwrite=True
                        )
                    else:
                        matrix.description = desc
                    self._skim_matrices.append(matrix)

    def _run_skims(self):
        use_ccr = False
        if self.controller.iteration >= 1:
            use_ccr = self.config.transit.use_ccr
        with self.controller.emme_manager.logbook_trace(
            "First and total wait time, number of boardings, "
            "fares, and total and transfer walk time"
        ):
            self._skim_walk_wait_fares()
        with self.controller.emme_manager.logbook_trace("In-vehicle time by mode"):
            self._invehicle_time_by_mode()
        if use_ccr:
            with self.controller.emme_manager.logbook_trace("CCR related skims"):
                self._ccr_skims()

    def _skim_walk_wait_fares(self):
        # First and total wait time, number of boardings, fares, total walk time, in-vehicle time
        name = f"{self._time_period}_{self._assign_class.name}"
        all_modes = [
            m.id for m in self._network.modes() if m.type in ["TRANSIT", "AUX_TRANSIT"]
        ]
        spec = {
            "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
            "actual_first_waiting_times": f'mf"{name}_FIRSTWAIT"',
            "actual_total_waiting_times": f'mf"{name}_TOTALWAIT"',
            "by_mode_subset": {
                "modes": all_modes,
                "avg_boardings": f'mf"{name}_XFERS"',
                "actual_aux_transit_times": f'mf"{name}_TOTALWALK"',
            },
        }
        if self.config.transit.use_fares:
            spec["by_mode_subset"].update(
                {
                    "actual_in_vehicle_costs": f'mf"{name}_IN_VEHICLE_COST"',
                    "actual_total_boarding_costs": f'mf"{name}_FARE"',
                }
            )
        self._run_matrix_results(spec)
        xfer_modes = []
        for mode in self.config.transit.modes:
            if mode.type == "WALK":
                xfer_modes.append(mode.mode_id)
        spec = {
            "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
            "by_mode_subset": {
                "modes": xfer_modes,
                "actual_aux_transit_times": f'mf"{name}_XFERWALK"',
            },
        }
        self._run_matrix_results(spec)
        matrix_calc = self.controller.emme_manager.tool(
            "inro.emme.matrix_calculation.matrix_calculator"
        )
        spec_list = [
            {  # convert number of boardings to number of transfers
                "type": "MATRIX_CALCULATION",
                "constraint": {
                    "by_value": {
                        "od_values": f'mf"{name}_XFERS"',
                        "interval_min": 0,
                        "interval_max": 9999999,
                        "condition": "INCLUDE",
                    }
                },
                "result": f'mf"{name}_XFERS"',
                "expression": f'(mf"{name}_XFERS" - 1).max.0',
            },
            {
                "type": "MATRIX_CALCULATION",
                "constraint": {
                    "by_value": {
                        "od_values": f'mf"{name}_TOTALWAIT"',
                        "interval_min": 0,
                        "interval_max": 9999999,
                        "condition": "INCLUDE",
                    }
                },
                "result": f'mf"{name}_XFERWAIT"',
                "expression": f'(mf"{name}_TOTALWAIT" - mf"{name}_FIRSTWAIT").max.0',
            },
        ]
        if self.config.transit.use_fares:
            # sum in-vehicle cost and boarding cost to get the fare paid
            spec_list.append(
                {
                    "type": "MATRIX_CALCULATION",
                    "constraint": None,
                    "result": f'mf"{name}_FARE"',
                    "expression": f'(mf"{name}_FARE" + mf"{name}_IN_VEHICLE_COST)"',
                }
            )
        matrix_calc(
            spec_list, scenario=self._scenario, num_processors=self._num_processors
        )

    def _invehicle_time_by_mode(self):
        network = self._network
        matrix_calc = self.controller.emme_manager.tool(
            "inro.emme.matrix_calculation.matrix_calculator"
        )
        name = f"{self._time_period}_{self._assign_class.name}"
        mode_combinations = self._get_emme_mode_ids()
        total_ivtt_expr = []
        if self.config.transit.use_ccr and self.controller.iteration >= 1:
            # calculate in-vehicle travel time by mode
            # set temp attribute @mode_timtr to contain the non-congested in-vehicle
            # times for segments of the mode of interest
            create_temps = self.controller.emme_manager.temp_attributes_and_restore
            attrs = [["TRANSIT_SEGMENT", "@mode_timtr", "base time by mode"]]
            with create_temps(self._scenario, attrs):
                for mode_name, modes in mode_combinations:
                    network.create_attribute("TRANSIT_SEGMENT", "@mode_timtr")
                    for line in network.transit_lines():
                        if line.mode.id in modes:
                            for segment in line.segments():
                                segment["@mode_timtr"] = segment["@base_timtr"]
                    attributes = {"TRANSIT_SEGMENT": ["@mode_timtr"]}
                    self._copy_attribute_values(network, self._scenario, attributes)
                    network.delete_attribute("TRANSIT_SEGMENT", "@mode_timtr")
                    ivtt_matrix = f'mf"{name}_{mode_name}IVTT"'
                    total_ivtt_expr.append(ivtt_matrix)
                    self._run_strategy_analysis(
                        {"in_vehicle": "@mode_timtr"}, ivtt_matrix
                    )
        else:
            for mode_name, modes in mode_combinations:
                ivtt_matrix = f'mf"{name}_{mode_name}IVTT"'
                total_ivtt_expr.append(ivtt_matrix)
                spec = {
                    "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
                    "by_mode_subset": {
                        "modes": modes,
                        "actual_in_vehicle_times": ivtt_matrix,
                    },
                }
                self._run_matrix_results(spec)
        # sum total ivtt across all modes
        spec = {
            "type": "MATRIX_CALCULATION",
            "constraint": None,
            "result": f'mf"{name}_TOTALIVTT"',
            "expression": "+".join(total_ivtt_expr),
        }
        matrix_calc(spec, scenario=self._scenario, num_processors=self._num_processors)

    def _get_emme_mode_ids(self):
        attributes = {"TRANSIT_LINE": ["#src_mode"], "TRANSIT_SEGMENT": ["@base_timtr"]}
        self._copy_attribute_values(self._scenario, self._network, attributes)
        valid_modes = [
            mode
            for mode in self.config.transit.modes
            if mode.type in self._assign_class.mode_types
            and mode.assign_type == "TRANSIT"
        ]
        if self.config.transit.use_fares:
            # map to used modes in apply fares case
            fare_modes = _defaultdict(lambda: set([]))
            for line in self._network.transit_lines():
                fare_modes[line["#src_mode"]].add(line.mode.id)
            emme_mode_ids = [
                (mode.short_name, list(fare_modes[mode.mode_id]))
                for mode in valid_modes
            ]
        else:
            emme_mode_ids = [(mode.short_name, [mode.mode_id]) for mode in valid_modes]
        return emme_mode_ids

    def _ccr_skims(self):
        name = f"{self._time_period}_{self._assign_class.name}"
        # # Link unreliability
        # self._run_strategy_analysis({"in_vehicle": "@ul1"}, f'mf"{name}_LINKREL"')
        # Crowding penalty
        self._run_strategy_analysis({"in_vehicle": "@ccost"}, f'mf"{name}_CROWD"')
        # skim node reliability, Extra added wait time (EAWT)
        self._run_strategy_analysis({"boarding": "@eawt"}, f'mf"{name}_EAWT"')
        # skim capacity penalty
        self._run_strategy_analysis(
            {"boarding": "@capacity_penalty"}, f'mf"{name}_CAPPEN"'
        )

    def _run_matrix_results(self, spec):
        matrix_results = self.controller.emme_manager.tool(
            "inro.emme.transit_assignment.extended.matrix_results"
        )
        matrix_results(
            spec,
            class_name=self._assign_class.name,
            scenario=self._scenario,
            num_processors=self._num_processors,
        )

    def _run_strategy_analysis(self, components, matrix_name):
        strategy_analysis = self.controller.emme_manager.tool(
            "inro.emme.transit_assignment.extended.strategy_based_analysis"
        )
        spec = {
            "trip_components": components,
            "sub_path_combination_operator": "+",
            "sub_strategy_combination_operator": "average",
            "selected_demand_and_transit_volumes": {
                "sub_strategies_to_retain": "ALL",
                "selection_threshold": {"lower": -999999, "upper": 999999},
            },
            "analyzed_demand": None,
            "constraint": None,
            "results": {"strategy_values": matrix_name},
            "type": "EXTENDED_TRANSIT_STRATEGY_ANALYSIS",
        }
        strategy_analysis(
            spec,
            class_name=self._assign_class.name,
            scenario=self._scenario,
            num_processors=self._num_processors,
        )

    def _mask_allpen(self):
        # Reset skims to 0 if not both local and premium
        localivt_skim = self._matrix_cache.get_data(
            f'mf"{self._time_period}_ALLPEN_LBIVTT"'
        )
        totalivt_skim = self._matrix_cache.get_data(
            f'mf"{self._time_period}_ALLPEN_TOTALIVTT"'
        )
        has_premium = np.greater((totalivt_skim - localivt_skim), 0)
        has_both = np.greater(localivt_skim, 0) * has_premium
        for skim in self._tmplt_skim_matrices(names_only=True):
            mat_name = f'mf"{self._time_period}_ALLPEN_{skim}"'
            data = self._matrix_cache.get_data(mat_name)
            self._matrix_cache.set_data(mat_name, data * has_both)

    def _mask_transfers(self):
        # Reset skims to 0 if number of transfers is greater than max_transfers
        max_transfers = self.config.transit.max_transfers
        for klass in self.config.transit.classes:
            xfers = self._matrix_cache.get_data(
                f'mf"{self._time_period}_{klass.name}_XFERS"'
            )
            xfer_mask = np.less_equal(xfers, max_transfers)
            for skim in self._tmplt_skim_matrices(names_only=True):
                mat_name = f'mf"{self._time_period}_{klass.name}_{skim}"'
                data = self._matrix_cache.get_data(mat_name)
                self._matrix_cache.set_data(mat_name, data * xfer_mask)

    def _export_skims(self):
        """Export skims to OMX files by period."""
        # NOTE: skims in separate file by period
        omx_file_path = self.get_abs_path(
            self.config.transit.output_skim_path.format(period=self._time_period)
        )
        os.makedirs(os.path.dirname(omx_file_path), exist_ok=True)
        with OMXManager(
            omx_file_path,
            "w",
            self._scenario,
            matrix_cache=self._matrix_cache,
            mask_max_value=1e7,
        ) as omx_file:
            omx_file.write_matrices(self._skim_matrices)

    def _report(self):
        num_zones = len(self._scenario.zone_numbers)
        num_cells = num_zones * num_zones
        self.logger.log(
            f"Transit impedance summary for period {self._time_period}", level="DEBUG"
        )
        self.logger.log(
            f"Number of zones: {num_zones}. Number of O-D pairs: {num_cells}. "
            "Values outside -9999999, 9999999 are masked in summaries.",
            level="DEBUG",
        )
        self.logger.log(
            "name                            min       max      mean           sum",
            level="DEBUG",
        )
        for matrix in self._skim_matrices:
            values = self._matrix_cache.get_data(matrix)
            data = np.ma.masked_outside(values, -9999999, 9999999)
            stats = (
                f"{matrix.name:25} {data.min():9.4g} {data.max():9.4g} "
                f"{data.mean():9.4g} {data.sum(): 13.7g}"
            )
            self.logger.log(stats, level="DEBUG")

    @staticmethod
    def _copy_attribute_values(src, dst, attributes):
        for domain, attrs in attributes.items():
            values = src.get_attribute_values(domain, attrs)
            dst.set_attribute_values(domain, attrs, values)
