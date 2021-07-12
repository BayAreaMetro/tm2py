"""Performs transit assignment and generates transit skims.

"""

from contextlib import contextmanager as _context
import os as _os

# from typing import List, Union, Any, Dict
import numpy as _numpy

# PyLint cannot build AST from compiled Emme libraries
# so disabling relevant import module checks
# pylint: disable=E0611, E0401, E1101
import inro.emme.database.emmebank as _emmebank

from tm2py.core.component import Component as _Component, Controller as _Controller
import tm2py.core.emme as _emme_tools
import tm2py.model.assignment.skim_transit_network as _skim_transit

_join, _dir = _os.path.join, _os.path.dirname


class TransitAssignment(_Component):
    """Run transit assignment and skims."""

    def __init__(self, controller: _Controller, root_dir: str = None):
        """Run transit assignment and skims.

        Args:
            controller: parent Controller object
            root_dir (str): root directory containing Emme project, demand matrices
        """
        super().__init__(controller)
        if root_dir is None:
            self._root_dir = _os.getcwd()
        else:
            self._root_dir = root_dir
        self._emme_manager = None

    @property
    def _modeller(self):
        return self._emme_manager.modeller

    def run(self):
        """Run transit assignment and skims."""
        project_path = _join(self._root_dir, "mtc_emme_transit", "mtc_emme.emp")
        self._emme_manager = _emme_tools.EmmeProjectCache()
        # Initialize Emme desktop if not already started
        self._emme_manager.project(project_path)
        modeller = self._modeller
        num_processors = _emme_tools.parse_num_processors(
            self.config.emme.num_processors
        )
        # Run assignment and skims for all specified periods
        with self._setup():
            time_periods = [time.name for time in self.config.periods]
            emmebank = _emmebank.Emmebank(
                _join(self._root_dir, "mtc_emme_transit", "Database", "emmebank")
            )
            for period in time_periods:
                scenario_id = self.config.emme.scenario_ids[period]
                scenario = emmebank.scenario(scenario_id)

                _skim_transit.initialize_matrices(
                    components=["transit_skims"],
                    periods=[period],
                    scenario=scenario,
                    delete_all_existing=False,
                )

                ctramp_output_folder = _os.path.join(self._root_dir, "ctramp_output")
                if self.controller.iteration > 1:
                    _skim_transit.import_demand_matrices(
                        period,
                        scenario,
                        ctramp_output_folder,
                        num_processors=num_processors,
                    )
                else:
                    _skim_transit.create_empty_demand_matrices(period, scenario)

                use_ccr = False

                _skim_transit.perform_assignment_and_skim(
                    modeller,
                    scenario,
                    period=period,
                    assignment_only=False,
                    num_processors=num_processors,
                    use_fares=True,
                    use_ccr=use_ccr,
                )
                output_omx_file = _os.path.join(
                    skims_path, "transit_skims_{}.omx".format(period)
                )
                _skim_transit.export_matrices_to_omx(
                    omx_file=output_omx_file,
                    periods=[period],
                    scenario=scenario,
                    big_to_zero=True,
                    max_transfers=3,
                )

                # if use_ccr and save_iter_flows:
                #     _skim_transit.save_per_iteration_flows(scenario)

                # if output_transit_boardings:
                #     desktop.data_explorer().replace_primary_scenario(scenario)
                #     output_transit_boardings_file = _os.path.join(
                #          _os.getcwd(), args.trn_path, "boardings_by_line_{}.csv".format(period))
                #     export_boardings_by_line(desktop, output_transit_boardings_file)

    @_context
    def _setup(self):
        with self._emme_manager.logbook_trace("Transit assignments"):
            try:
                yield
            finally:
                pass
