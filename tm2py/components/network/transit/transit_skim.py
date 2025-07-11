"""Transit skims module."""

from __future__ import annotations

import itertools
import os
from collections import defaultdict, namedtuple
from contextlib import contextmanager as _context
from math import inf
from time import time
from typing import TYPE_CHECKING, Collection, Dict, List, Tuple, Union

import numpy as np

from tm2py import tools
from tm2py.components.component import Component
from tm2py.emme.matrix import MatrixCache, OMXManager
from tm2py.logger import LogStartEnd
from tm2py.omx import NumpyArray

if TYPE_CHECKING:
    from tm2py.controller import RunController

Skimproperty = namedtuple("Skimproperty", "name desc")


class TransitSkim(Component):
    """Transit skim calculation methods."""

    def __init__(self, controller: "RunController"):
        """Constructor for TransitSkim class.

        Args:
            controller: The RunController instance.
        """
        super().__init__(controller)
        self.config = self.controller.config.transit
        self._emmebank = None
        self._num_processors = self.controller.emme_manager.num_processors_transit_skim
        self._networks = None
        self._scenarios = None
        self._matrix_cache = None
        self._skim_properties = None
        self._skim_matrices = {
            k: None
            for k in itertools.product(
                self.time_period_names,
                self.config.classes,
                self.skim_properties,
            )
        }
        self._skim_outputs = None

    def validate_inputs(self):
        """Validate inputs."""
        # TODO add input validation
        pass

    @property
    def emmebank(self):
        if not self._emmebank:
            self._emmebank = self.controller.emme_manager.transit_emmebank
        return self._emmebank

    @property
    def scenarios(self):
        if self._scenarios is None:
            self._scenarios = {
                tp: self.emmebank.scenario(tp) for tp in self.time_period_names
            }
        return self._scenarios

    @property
    def networks(self):
        if self._networks is None:
            self._networks = {
                tp: self.scenarios[tp].get_partial_network(
                    ["TRANSIT_SEGMENT"], include_attributes=False
                )
                for tp in self.time_period_names
            }
        return self._networks

    @property
    def matrix_cache(self):
        if self._matrix_cache is None:
            self._matrix_cache = {
                tp: MatrixCache(self.scenarios[tp]) for tp in self.time_period_names
            }
        return self._matrix_cache

    @LogStartEnd("Transit skims")
    def run(self):
        """Run transit skims."""
        self.emmebank_skim_matrices(
            self.time_period_names, self.config.classes, self.skim_properties
        )
        with self.logger.log_start_end(f"period transit skims"):
            for _time_period in self.time_period_names:
                with self.controller.emme_manager.logbook_trace(
                    f"Transit skims for period {_time_period}"
                ):
                    for _transit_class in self.config.classes:
                        self.run_skim_set(_time_period, _transit_class)
                        self._export_skims(_time_period, _transit_class)
                    if self.logger.debug_enabled:
                        self._log_debug_report(_time_period)

    @property
    def skim_matrices(self):
        return self._skim_matrices

    @property
    def skim_properties(self):
        """List of Skim Property named tuples: name, description.

        TODO put these in config.
        """
        if self._skim_properties is None:
            from collections import namedtuple

            # TODO config
            self._skim_properties = []

            _basic_skims = [
                ("IWAIT", "first wait time"),
                ("XWAIT", "transfer wait time"),
                ("WAIT", "total wait time"),
                ("FARE", "fare"),
                ("BOARDS", "num boardings"),
                ("WAUX", "auxiliary walk time"),
                ("DTIME", "access and egress drive time"),
                ("DDIST", "access and egress drive distance"),
                ("WACC", "access walk time"),
                ("WEGR", "egress walk time"),
                ("IVT", "total in-vehicle time"),
                ("IN_VEHICLE_COST", "in-vehicle cost"),
                ("CROWD", "Crowding penalty"),
            ]
            self._skim_properties += [
                Skimproperty(_name, _desc) for _name, _desc in _basic_skims
            ]
            for mode in self.config.modes:
                if (mode.assign_type == "TRANSIT") and (mode.type != "PNR_dummy"):
                    desc = mode.description or mode.name
                    self._skim_properties.append(
                        Skimproperty(
                            f"IVT{mode.name}",
                            f"{desc} in-vehicle travel time"[:40],
                        )
                    )
            if self.config.use_ccr:
                self._skim_properties.extend(
                    [
                        Skimproperty("LINKREL", "Link reliability"),
                        Skimproperty("EAWT", "Extra added wait time"),
                        Skimproperty("CAPPEN", "Capacity penalty"),
                    ]
                )
            if self.config.congested_transit_assignment:
                self._skim_properties.extend(
                    [
                        Skimproperty("TRIM", "used to trim demands"),
                        Skimproperty("XBOATIME", "transfer boarding time penalty"),
                        Skimproperty("DTOLL", "drive access or egress toll price"),
                    ]
                )
        return self._skim_properties

    @property
    def skim_outputs(self):
        """List of Skim Property named tuples: name, description.

        TODO put these in config.
        """
        if self._skim_outputs is None:
            from collections import namedtuple

            # TODO config
            self._skim_outputs = []
            
            _write_skims_to_omx = [
                "IWAIT",
                "XWAIT",
                "FARE",
                "BOARDS",
                "WAUX",
                "DTIME",
                "DDIST",
                "WACC",
                "WEGR",
                "IVT",
                "CROWD"
            ]

            for mode in self.config.modes:
                if (mode.assign_type == "TRANSIT") and (mode.type != "PNR_dummy"):
                    _write_skims_to_omx.append(f"IVT{mode.name}")

            self._skim_outputs = [
                Skimproperty(_name, _desc) 
                for _name, _desc in self._skim_properties
                if _name in _write_skims_to_omx
            ]

        return self._skim_outputs

    def emmebank_skim_matrices(
        self,
        time_periods: List[str] = None,
        transit_classes=None,
        skim_properties: Skimproperty = None,
    ) -> dict:
        """Gets skim matrices from emmebank, or lazily creates them if they don't already exist."""
        create_matrix = self.controller.emme_manager.tool(
            "inro.emme.data.matrix.create_matrix"
        )
        if time_periods is None:
            time_periods = self.time_period_names
        if not set(time_periods).issubset(set(self.time_period_names)):
            raise ValueError(
                f"time_periods ({time_periods}) must be subset of time_period_names ({self.time_period_names})."
            )

        if transit_classes is None:
            transit_classes = self.config.classes
        if not set(transit_classes).issubset(set(self.config.classes)):
            raise ValueError(
                f"time_periods ({transit_classes}) must be subset of time_period_names ({self.config.transit_classes})."
            )

        if skim_properties is None:
            skim_properties = self.skim_properties
        if not set(skim_properties).issubset(set(self.skim_properties)):
            raise ValueError(
                f"time_periods ({skim_properties}) must be subset of time_period_names ({self.skim_properties})."
            )

        _tp_tclass_skprop = itertools.product(
            time_periods, transit_classes, skim_properties
        )
        _tp_tclass_skprop_list = []

        for _tp, _tclass, _skprop in _tp_tclass_skprop:
            a = 1
            _name = f"{_tp}_{_tclass.name}_{_skprop.name}"
            _desc = f"{_tp} {_tclass.description}: {_skprop.desc}"
            _matrix = self.scenarios[_tp].emmebank.matrix(f'mf"{_name}"')
            if not _matrix:
                _matrix = create_matrix(
                    "mf", _name, _desc, scenario=self.scenarios[_tp], overwrite=True
                )
            else:
                _matrix.description = _desc

            self._skim_matrices[_name] = _matrix
            _tp_tclass_skprop_list.append(_name)

        skim_matrices = {
            k: v
            for k, v in self._skim_matrices.items()
            if k in list(_tp_tclass_skprop_list)
        }
        return skim_matrices

    def run_skim_set(self, time_period: str, transit_class: str):
        """Run the transit skim calculations for a given time period and assignment class.

        Results are stored in transit emmebank.

        Steps:
            1. determine if using transit capacity constraint
            2. skim walk, wait time, boardings, and fares
            3. skim in vehicle time by mode
            4. mask transfers above max amount
            5. mask if doesn't have required modes
        """
        use_ccr = False
        congested_transit_assignment = self.config.congested_transit_assignment
        if self.controller.iteration >= 1:
            use_ccr = self.config.use_ccr
        with self.controller.emme_manager.logbook_trace(
            "First and total wait time, number of boardings, "
            "fares, and total and transfer walk time"
        ):
            self.skim_walk_wait_boards_fares(time_period, transit_class)
        with self.controller.emme_manager.logbook_trace("In-vehicle time by mode"):
            self.skim_invehicle_time_by_mode(time_period, transit_class, use_ccr)
        with self.controller.emme_manager.logbook_trace(
            "Drive distance and time",
            "Walk auxiliary time, walk access time and walk egress time",
        ):
            self.skim_drive_walk(time_period, transit_class)
        with self.controller.emme_manager.logbook_trace("Calculate crowding"):
            self.skim_crowding(time_period, transit_class)
        if use_ccr:
            with self.controller.emme_manager.logbook_trace("CCR related skims"):
                self.skim_reliability_crowding_capacity(time_period, transit_class)

    def skim_walk_wait_boards_fares(self, time_period: str, transit_class: str):
        """Skim wait, walk, board, and fares for a given time period and transit assignment class.

        Skim the first and total wait time, number of boardings, (transfers + 1)
        fares, total walk time, total in-vehicle time.
        """
        _tp_tclass = f"{time_period}_{transit_class.name}"
        _network = self.networks[time_period]
        _transit_mode_ids = [
            m.id for m in _network.modes() if m.type in ["TRANSIT", "AUX_TRANSIT"]
        ]
        spec = {
            "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
            "actual_first_waiting_times": f'mf"{_tp_tclass}_IWAIT"',
            "actual_total_waiting_times": f'mf"{_tp_tclass}_WAIT"',
            "by_mode_subset": {
                "modes": _transit_mode_ids,
                "avg_boardings": f'mf"{_tp_tclass}_BOARDS"',
            },
        }
        if self.config.use_fares:
            spec["by_mode_subset"].update(
                {
                    "actual_in_vehicle_costs": f'mf"{_tp_tclass}_IN_VEHICLE_COST"',
                    "actual_total_boarding_costs": f'mf"{_tp_tclass}_FARE"',
                }
            )

        self.controller.emme_manager.matrix_results(
            spec,
            class_name=transit_class.name,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )

        self._calc_xfer_wait(time_period, transit_class.name)
        self._calc_boardings(time_period, transit_class.name)
        if self.config.use_fares:
            self._calc_fares(time_period, transit_class.name)

    def _calc_xfer_walk(self, time_period, transit_class_name):
        xfer_modes = [m.mode_id for m in self.config.modes if m.type == "WALK"]
        tp_tclass = f"{time_period}_{transit_class_name}"
        spec = {
            "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
            "by_mode_subset": {
                "modes": xfer_modes,
                "actual_aux_transit_times": f'mf"{tp_tclass}_XFERWALK"',
            },
        }
        self.controller.emme_manager.matrix_results(
            spec,
            class_name=transit_class_name,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )

    def _calc_xfer_wait(self, time_period, transit_class_name):
        """Calculate transfer wait from total wait time and initial wait time and add to Emmebank.

        TODO convert this type of calculation to numpy
        """
        tp_tclass = f"{time_period}_{transit_class_name}"
        spec = {
            "type": "MATRIX_CALCULATION",
            "constraint": {
                "by_value": {
                    "od_values": f'mf"{tp_tclass}_WAIT"',
                    "interval_min": 0,
                    "interval_max": 9999999,
                    "condition": "INCLUDE",
                }
            },
            "result": f'mf"{tp_tclass}_XWAIT"',
            "expression": f'(mf"{tp_tclass}_WAIT" - mf"{tp_tclass}_IWAIT").max.0',
        }

        self.controller.emme_manager.matrix_calculator(
            spec,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )

    def _calc_boardings(self, time_period: str, transit_class_name: str):
        """Calculate # boardings from # of transfers and add to Emmebank.

        TODO convert this type of calculation to numpy
        """
        _tp_tclass = f"{time_period}_{transit_class_name}"
        if ("PNR_TRN_WLK" in _tp_tclass) or ("WLK_TRN_PNR" in _tp_tclass):
            spec = {
                "type": "MATRIX_CALCULATION",
                "constraint": {
                    "by_value": {
                        "od_values": f'mf"{_tp_tclass}_BOARDS"',
                        "interval_min": 0,
                        "interval_max": 9999999,
                        "condition": "INCLUDE",
                    }
                },
                # CHECK should this be BOARDS or similar, not xfers?
                "result": f'mf"{_tp_tclass}_BOARDS"',
                "expression": f'(mf"{_tp_tclass}_BOARDS" - 1).max.0',
            }

            self.controller.emme_manager.matrix_calculator(
                spec,
                scenario=self.scenarios[time_period],
                num_processors=self._num_processors,
            )

    def _calc_fares(self, time_period: str, transit_class_name: str):
        """Calculate fares as sum in-vehicle cost and boarding cost to get the fare paid and add to Emmebank.

        TODO convert this type of calculation to numpy
        """
        _tp_tclass = f"{time_period}_{transit_class_name}"
        spec = {
            "type": "MATRIX_CALCULATION",
            "constraint": None,
            "result": f'mf"{_tp_tclass}_FARE"',
            "expression": f'(mf"{_tp_tclass}_FARE" + mf"{_tp_tclass}_IN_VEHICLE_COST")',
        }

        self.controller.emme_manager.matrix_calculator(
            spec,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )

    @staticmethod
    def _segments_with_modes(_network, _modes: Union[Collection[str], str]):
        _modes = list(_modes)
        segments = [
            li.segments() for li in _network.transit_lines() if li.mode.id in _modes
        ]
        return segments

    def _invehicle_time_by_mode_ccr(
        self, time_period: str, transit_class: str, mode_combinations
    ) -> List[str]:
        """Calculate in-vehicle travel time by mode using CCR and store results in Emmebank.

        Args:
            time_period (_type_): time period abbreviation
            transit_class (_type_): transit class name
            mode_combinations (_type_): TODO

        Returns:
            List of matrix names in Emmebank to sum together to get total in-vehicle travel time.
        """

        _network = self.networks[time_period]
        _scenario = self.scenarios[time_period]
        _tp_tclass = f"{time_period}_{transit_class.name}"
        _total_ivtt_expr = []
        create_temps = self.controller.emme_manager.temp_attributes_and_restore
        temp_attrs = [["TRANSIT_SEGMENT", "@mode_timtr", "base time by mode"]]
        with create_temps(_scenario, temp_attrs):
            for _mode_name, _modes in mode_combinations:
                _network.create_attribute("TRANSIT_SEGMENT", "@mode_timtr")
                _li_segs_with_mode = TransitSkim._segments_with_modes(_network, _modes)
                # set temp attribute @mode_timtr to contain the non-congested in-vehicle
                # times for segments of the mode of interest
                for line_segment in _li_segs_with_mode:
                    for segment in line_segment:
                        segment["@mode_timtr"] = segment["@base_timtr"]
                # not sure why we to copy this if we are deleting it in next line? - ES
                self.controller.emme_manager.copy_attribute_values(
                    self.networks[time_period],
                    _scenario,
                    {"TRANSIT_SEGMENT": ["@mode_timtr"]},
                )
                self.networks[time_period].delete_attribute(
                    "TRANSIT_SEGMENT", "@mode_timtr"
                )
                _ivtt_matrix_name = f'mf"{_tp_tclass}_IVT{_mode_name}"'
                _total_ivtt_expr.append(_ivtt_matrix_name)
                self._run_strategy_analysis(
                    time_period,
                    transit_class,
                    {"in_vehicle": "@mode_timtr"},
                    f"IVT{_mode_name}",
                )
        return _total_ivtt_expr

    def _invehicle_time_by_mode_no_ccr(
        self, time_period: str, transit_class: str, mode_combinations
    ) -> List[str]:
        """Calculate in-vehicle travel time by without CCR and store results in Emmebank.

        Args:
            time_period (_type_): time period abbreviation
            transit_class (_type_): transit class name
            mode_combinations (_type_): TODO

        Returns: List of matrix names in Emmebank to sum together to get total in-vehicle travel time.

        """
        _tp_tclass = f"{time_period}_{transit_class.name}"
        _total_ivtt_expr = []
        for _mode_name, modes in mode_combinations:
            _ivtt_matrix_name = f'mf"{_tp_tclass}_IVT{_mode_name}"'
            _total_ivtt_expr.append(_ivtt_matrix_name)
            spec = {
                "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
                "by_mode_subset": {
                    "modes": modes,
                    "actual_in_vehicle_times": _ivtt_matrix_name,
                },
            }
            self.controller.emme_manager.matrix_results(
                spec,
                class_name=transit_class.name,
                scenario=self.scenarios[time_period],
                num_processors=self._num_processors,
            )
        return _total_ivtt_expr

    def skim_invehicle_time_by_mode(
        self, time_period: str, transit_class: str, use_ccr: bool = False
    ) -> None:
        """Skim in-vehicle by mode for a time period and transit class and store results in Emmebank.

        Args:
            time_period (str): time period abbreviation
            transit_class (str): transit class name
            use_ccr (bool): if True, will use crowding, capacity, and reliability (ccr).
                Defaults to False

        """
        mode_combinations = self._get_emme_mode_ids(transit_class, time_period)
        if use_ccr:
            total_ivtt_expr = self._invehicle_time_by_mode_ccr(
                time_period, transit_class, mode_combinations
            )
        else:
            total_ivtt_expr = self._invehicle_time_by_mode_no_ccr(
                time_period, transit_class, mode_combinations
            )
        # sum total ivtt across all modes
        self._calc_total_ivt(time_period, transit_class, total_ivtt_expr)

    def _calc_total_ivt(
        self, time_period: str, transit_class: str, total_ivtt_expr: list[str]
    ) -> None:
        """Sums matrices to get total in vehicle time and stores in the Emmebank.

        Args:
            time_period (str): time period abbreviation
            transit_class (str): transit class name
            total_ivtt_expr (list[str]): List of matrix names in Emmebank which have IVT to sum to get total.
        """
        _tp_tclass = f"{time_period}_{transit_class.name}"
        spec = {
            "type": "MATRIX_CALCULATION",
            "constraint": None,
            "result": f'mf"{_tp_tclass }_IVT"',
            "expression": "+".join(total_ivtt_expr),
        }

        self.controller.emme_manager.matrix_calculator(
            spec,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )

    def skim_drive_walk(self, time_period: str, transit_class: str) -> None:
        """"""
        _tp_tclass = f"{time_period}_{transit_class.name}"
        # _network = self.networks[time_period]

        # drive time here is perception factor*(drive time + toll penalty),
        # will calculate the actual drive time and substract toll penalty in the following steps
        spec1 = {
            "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
            "by_mode_subset": {
                "modes": ["D"],
                "actual_aux_transit_times": f'mf"{_tp_tclass}_DTIME"',
                "distance": f'mf"{_tp_tclass}_DDIST"',
            },
        }
        # skim walk distance in walk time matrices first,
        # will calculate the actual walk time and overwrite the distance in the following steps
        spec2 = {
            "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
            "by_mode_subset": {
                "modes": ["w"],
                "distance": f'mf"{_tp_tclass}_WAUX"',
            },
        }
        spec3 = {
            "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
            "by_mode_subset": {
                "modes": ["a"],
                "distance": f'mf"{_tp_tclass}_WACC"',
            },
        }
        spec4 = {
            "type": "EXTENDED_TRANSIT_MATRIX_RESULTS",
            "by_mode_subset": {
                "modes": ["e"],
                "distance": f'mf"{_tp_tclass}_WEGR"',
            },
        }
        if transit_class.name not in ['WLK_TRN_WLK']:
            self.controller.emme_manager.matrix_results(
                spec1,
                class_name=transit_class.name,
                scenario=self.scenarios[time_period],
                num_processors=self._num_processors,
            )
        self.controller.emme_manager.matrix_results(
            spec2,
            class_name=transit_class.name,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )
        if transit_class.name not in ['PNR_TRN_WLK','KNR_TRN_WLK']:
            self.controller.emme_manager.matrix_results(
                spec3,
                class_name=transit_class.name,
                scenario=self.scenarios[time_period],
                num_processors=self._num_processors,
            )
        if transit_class.name not in ['WLK_TRN_PNR','WLK_TRN_KNR']:
            self.controller.emme_manager.matrix_results(
                spec4,
                class_name=transit_class.name,
                scenario=self.scenarios[time_period],
                num_processors=self._num_processors,
            )


        drive_perception_factor = self.config.drive_perception_factor
        walk_speed = self.config.walk_speed
        vot = self.config.value_of_time
        # divide drive time by mode specific perception factor to get the actual time
        # for walk time, use walk distance/walk speed
        # because the mode specific perception factors are hardcoded in the mode definition
        spec_list = [
            {
                "type": "MATRIX_CALCULATION",
                "constraint": None,
                "result": f'mf"{_tp_tclass}_DTIME"',
                "expression": f'mf"{_tp_tclass}_DTIME"/{drive_perception_factor}',
            },
            {
                "type": "MATRIX_CALCULATION",
                "constraint": None,
                "result": f'mf"{_tp_tclass}_DTIME"',
                "expression": f'mf"{_tp_tclass}_DTIME"',
            },
            {
                "type": "MATRIX_CALCULATION",
                "constraint": None,
                "result": f'mf"{_tp_tclass}_WAUX"',
                "expression": f'mf"{_tp_tclass}_WAUX"/({walk_speed}/60)',
            },
            {
                "type": "MATRIX_CALCULATION",
                "constraint": None,
                "result": f'mf"{_tp_tclass}_WACC"',
                "expression": f'mf"{_tp_tclass}_WACC"/({walk_speed}/60)',
            },
            {
                "type": "MATRIX_CALCULATION",
                "constraint": None,
                "result": f'mf"{_tp_tclass}_WEGR"',
                "expression": f'mf"{_tp_tclass}_WEGR"/({walk_speed}/60)',
            },
        ]
        self.controller.emme_manager.matrix_calculator(
            spec_list,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )

    def skim_penalty_toll(self, time_period: str, transit_class: str) -> None:
        """"""
        # transfer boarding time penalty
        self._run_strategy_analysis(
            time_period, transit_class, {"boarding": "@xboard_nodepen"}, "XBOATIME"
        )

        _tp_tclass = f"{time_period}_{transit_class.name}"
        if ("PNR_TRN_WLK" in _tp_tclass) or ("WLK_TRN_PNR" in _tp_tclass):
            spec = {  # subtract PNR boarding from total transfer boarding time penalty
                "type": "MATRIX_CALCULATION",
                "constraint": {
                    "by_value": {
                        "od_values": f'mf"{_tp_tclass}_XBOATIME"',
                        "interval_min": 0,
                        "interval_max": 9999999,
                        "condition": "INCLUDE",
                    }
                },
                "result": f'mf"{_tp_tclass}_XBOATIME"',
                "expression": f'(mf"{_tp_tclass}_XBOATIME" - 1).max.0',
            }

            self.controller.emme_manager.matrix_calculator(
                spec,
                scenario=self.scenarios[time_period],
                num_processors=self._num_processors,
            )

        # drive toll
        if ("PNR_TRN_WLK" in _tp_tclass) or ("KNR_TRN_WLK" in _tp_tclass):
            self._run_path_analysis(
                time_period,
                transit_class,
                "ORIGIN_TO_INITIAL_BOARDING",
                {"aux_transit": "@drive_toll"},
                "DTOLL",
            )
        elif ("WLK_TRN_PNR" in _tp_tclass) or ("WLK_TRN_KNR" in _tp_tclass):
            self._run_path_analysis(
                time_period,
                transit_class,
                "FINAL_ALIGHTING_TO_DESTINATION",
                {"aux_transit": "@drive_toll"},
                "DTOLL",
            )

    def _get_emme_mode_ids(
        self, transit_class, time_period
    ) -> List[Tuple[str, List[str]]]:
        """Get the Emme mode IDs used in the assignment.

        Loads the #src_mode attribute on lines if fares are used, and the
        @base_timtr on segments if ccr is used.

        Returns:
            List of tuples of two items, the original mode name (from config)
            to a list of mode IDs used in the Emme assignment. This list
            will be one item if fares are not used, but will contain the fare
            modes used in the journey levels mode-to-mode transfer table
            generated from Apply fares.
        """
        if self.config.use_fares:
            self.controller.emme_manager.copy_attribute_values(
                self.scenarios[time_period],
                self.networks[time_period],
                {"TRANSIT_LINE": ["#src_mode"]},
            )
        if self.config.use_ccr:
            self.controller.emme_manager.copy_attribute_values(
                self.scenarios[time_period],
                self.networks[time_period],
                {"TRANSIT_SEGMENT": ["@base_timtr"]},
            )
        valid_modes = [
            mode
            for mode in self.config.modes
            if mode.type in transit_class.mode_types
            and mode.assign_type == "TRANSIT"
            and mode.type != "PNR_dummy"
        ]
        if self.config.use_fares:
            # map to used modes in apply fares case
            fare_modes = defaultdict(lambda: set([]))
            for line in self.networks[time_period].transit_lines():
                fare_modes[line["#src_mode"]].add(line.mode.id)
            emme_mode_ids = [
                (mode.name, list(fare_modes[mode.mode_id]))
                for mode in valid_modes
                if len(list(fare_modes[mode.mode_id])) > 0
            ]
        else:
            emme_mode_ids = [(mode.name, [mode.mode_id]) for mode in valid_modes]
        return emme_mode_ids

    def skim_reliability_crowding_capacity(
        self, time_period: str, transit_class
    ) -> None:
        """Generate skim results for CCR assignment and stores results in Emmebank.

        Generates the following:
        1. Link Unreliability: LINKREL
        2. Crowding penalty: CROWD
        3. Extra added wait time: EAWT
        4. Capacity penalty: CAPPEN

        Args:
            time_period (str): time period abbreviation
            transit_class: transit class
        """

        # Link unreliability
        self._run_strategy_analysis(
            time_period, transit_class, {"in_vehicle": "ul1"}, "LINKREL"
        )
        # Crowding penalty
        self._run_strategy_analysis(
            time_period, transit_class, {"in_vehicle": "@ccost"}, "CROWD"
        )
        # skim node reliability, extra added wait time (EAWT)
        self._run_strategy_analysis(
            time_period, transit_class, {"boarding": "@eawt"}, "EAWT"
        )
        # skim capacity penalty
        self._run_strategy_analysis(
            time_period, transit_class, {"boarding": "@capacity_penalty"}, "CAPPEN"
        )

    def skim_crowding(self, time_period: str, transit_class) -> None:
        """"""
        # Crowding penalty
        self._run_strategy_analysis(
            time_period, transit_class, {"in_vehicle": "@ccost"}, "CROWD"
        )

    def _run_strategy_analysis(
        self,
        time_period: str,
        transit_class,
        components: Dict[str, str],
        matrix_name_suffix: str,
    ):
        """Runs strategy analysis in Emme and stores results in emmebank.

        Args:
            time_period (str): Time period name abbreviation
            transit_class (_type_): _description_
            components (Dict[str, str]): _description_
            matrix_name_suffix (str): Appended to time period and transit class name to create output matrix name.
        """
        _tp_tclass = f"{time_period}_{transit_class.name}"
        _matrix_name = f'mf"{_tp_tclass}_{matrix_name_suffix}"'
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
            "analyzed_demand": f"mfTRN_{transit_class.name}_{time_period}",
            "constraint": None,
            "results": {"strategy_values": _matrix_name},
            "type": "EXTENDED_TRANSIT_STRATEGY_ANALYSIS",
        }
        strategy_analysis(
            spec,
            class_name=transit_class.name,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )

    def _run_path_analysis(
        self,
        time_period: str,
        transit_class,
        portion_of_path: str,
        components: Dict[str, str],
        matrix_name_suffix: str,
    ):
        """Runs path analysis in Emme and stores results in emmebank.

        Args:
            time_period (str): Time period name abbreviation
            transit_class (_type_): _description_
            components (Dict[str, str]): _description_
            matrix_name_suffix (str): Appended to time period and transit class name to create output matrix name.
        """
        _tp_tclass = f"{time_period}_{transit_class.name}"
        _matrix_name = f'mf"{_tp_tclass}_{matrix_name_suffix}"'
        path_analysis = self.controller.emme_manager.tool(
            "inro.emme.transit_assignment.extended.path_based_analysis"
        )

        spec = {
            "portion_of_path": portion_of_path,
            "trip_components": components,
            "path_operator": "+",
            "path_selection_threshold": {"lower": -999999, "upper": 999999},
            "path_to_od_aggregation": {
                "operator": "average",
                "aggregated_path_values": _matrix_name,
            },
            "analyzed_demand": None,
            "constraint": None,
            "type": "EXTENDED_TRANSIT_PATH_ANALYSIS",
        }
        path_analysis(
            spec,
            class_name=transit_class.name,
            scenario=self.scenarios[time_period],
            num_processors=self._num_processors,
        )

    def mask_if_not_required_modes(self, time_period: str, transit_class) -> None:
        """
        Enforce the `required_mode_combo` parameter by setting IVTs to 0 if don't have required modes.

        Args:
            time_period (str): Time period name abbreviation
            transit_class (_type_): _description_
        """
        if not transit_class.required_mode_combo:
            return

        _ivt_skims = {}
        for mode in transit_class.required_mode_combo:
            transit_modes = [m for m in self.config.modes if m.type == mode]
            for transit_mode in transit_modes:
                if mode not in _ivt_skims.keys():
                    _ivt_skims[mode] = self.matrix_cache[time_period].get_data(
                        f'mf"{time_period}_{transit_class.name}_{transit_mode.name}IVTT"'
                    )
                else:
                    _ivt_skims[mode] += self.matrix_cache[time_period].get_data(
                        f'mf"{time_period}_{transit_class.name}_{transit_mode.name}IVTT"'
                    )

        # multiply all IVT skims together and see if they are greater than zero
        has_all = None
        for key, value in _ivt_skims.items():
            if has_all is not None:
                has_all = np.multiply(has_all, value)
            else:
                has_all = value

        self._mask_skim_set(time_period, transit_class, has_all)

    def mask_above_max_transfers(self, time_period: str, transit_class):
        """Reset skims to 0 if number of transfers is greater than max_transfers.

        Args:
            time_period (str): Time period name abbreviation
            transit_class (_type_): _description_
        """
        max_transfers = self.config.max_transfers
        xfers = self.matrix_cache[time_period].get_data(
            f'mf"{time_period}_{transit_class.name}_XFERS"'
        )
        xfer_mask = np.less_equal(xfers, max_transfers)
        self._mask_skim_set(time_period, transit_class, xfer_mask)

    def _mask_skim_set(self, time_period: str, transit_class, mask_array: NumpyArray):
        """Mask a skim set (set of skims for a given time period and transit class) based on an array.

        Array values of >0 are kept. Zero are not.

        TODO add in checks for mask_array dimensions and values

        Args:
            time_period (str): Time period name abbreviation
            transit_class (_type_): _description_
            mask_array (NumpyArray): _description_
        """
        mask_array = np.greater(mask_array, 0)
        mask_array = np.less(mask_array, inf)
        for skim_key, skim in self.emmebank_skim_matrices(
            time_periods=[time_period], transit_classes=[transit_class]
        ).items():
            skim_data = self.matrix_cache[time_period].get_data(skim.name)
            self.matrix_cache[time_period].set_data(skim.name, skim_data * mask_array)

    def _export_skims(self, time_period: str, transit_class: str):
        """Export skims to OMX files by period."""
        # NOTE: skims in separate file by period
        output_skim_path = self.get_abs_path(self.config.output_skim_path)
        omx_file_path = os.path.join(
            output_skim_path,
            self.config.output_skim_filename_tmpl.format(
                time_period=time_period, tclass=transit_class.name
            ),
        )
        os.makedirs(os.path.dirname(omx_file_path), exist_ok=True)

        _matrices = self.emmebank_skim_matrices(
            time_periods=[time_period], 
            transit_classes=[transit_class],
            skim_properties=self.skim_outputs

        )

        with OMXManager(
            omx_file_path,
            "w",
            self.scenarios[time_period],
            matrix_cache=self.matrix_cache[time_period],
            mask_max_value=1e7,
            growth_factor=1,
        ) as omx_file:
            omx_file.write_matrices(_matrices)

    def _log_debug_report(self, _time_period):
        num_zones = len(self.scenarios[_time_period].zone_numbers)
        num_cells = num_zones * num_zones
        self.logger.log(
            f"Transit impedance summary for period {_time_period}", level="DEBUG"
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

        temp = self.emmebank_skim_matrices(time_periods=[_time_period])

        for matrix_name in temp.keys():
            matrix_name = f'mf"{matrix_name}"'
            values = self.matrix_cache[_time_period].get_data(matrix_name)
            data = np.ma.masked_outside(values, -9999999, 9999999)
            stats = (
                f"{matrix_name:25} {data.min():9.4g} {data.max():9.4g} "
                f"{data.mean():9.4g} {data.sum(): 13.7g}"
            )
            self.logger.log(stats, level="DEBUG")

    @staticmethod
    def _copy_attribute_values(src, dst, attributes):
        for domain, attrs in attributes.items():
            values = src.get_attribute_values(domain, attrs)
            dst.set_attribute_values(domain, attrs, values)