"""Placeholder docstring for CT-RAMP related components for household residents' model."""

import shutil as _shutil

import openmatrix as omx

from tm2py.components.component import Component
from tm2py.logger import LogStartEnd
from tm2py.tools import run_process
from tm2py.components.demand.prepare_demand import PrepareHighwayDemand


class HouseholdModel(Component):
    """Run household resident model."""

    def validate_inputs(self):
        """Validates inputs for component."""
        pass

    @LogStartEnd()
    def run(self):
        """Run the the household resident travel demand model.

        Steps:
            1. Starts household manager.
            2. Starts matrix manager.
            3. Starts resident travel model (CTRAMP).
            4. Cleans up CTRAMP java.
        """
        self.config = self.controller.config.household
        self._start_household_manager()
        self._start_matrix_manager()
        self._run_resident_model()
        self._stop_java()
        # self._consolidate_demand_for_assign()
        self._prepare_demand_for_assignment()

    def _prepare_demand_for_assignment(self):
        prep_demand = PrepareHighwayDemand(self.controller)
        prep_demand.prepare_household_demand()

    def _start_household_manager(self):
        commands = [
            f"cd /d {self.controller.run_dir}",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\\runtime;C:\\Windows\\System32;%JAVA_PATH%\\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\runHhMgr.cmd %JAVA_PATH% %HOST_IP_ADDRESS%",
        ]
        run_process(commands, name="start_household_manager")

    def _start_matrix_manager(self):
        commands = [
            f"cd /d {self.controller.run_dir}",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\\runtime;C:\\Windows\\System32;%JAVA_PATH%\\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\runMtxMgr.cmd %HOST_IP_ADDRESS% %JAVA_PATH%",
        ]
        run_process(commands, name="start_matrix_manager")

    def _run_resident_model(self):
        sample_rate_iteration = {1: 0.05, 2: 0.5, 3: 1, 4: 0.02, 5: 0.02}
        iteration = self.controller.iteration
        sample_rate = sample_rate_iteration[iteration]
        _shutil.copyfile("CTRAMP\\runtime\\mtctm2.properties", "mtctm2.properties")
        commands = [
            f"cd /d {self.controller.run_dir}",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\\runtime;C:\\Windows\\System32;%JAVA_PATH%\\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\runMTCTM2ABM.cmd {sample_rate} {iteration} %JAVA_PATH%",
        ]
        run_process(commands, name="run_resident_model")

    @staticmethod
    def _stop_java():
        run_process(['taskkill /im "java.exe" /F'])

    def _consolidate_demand_for_assign(self):
        """
        CTRAMP writes out demands in separate omx files, e.g.
        ctramp_output\\auto_@p@_SOV_GP_@p@.mat
        ctramp_output\\auto_@p@_SOV_PAY_@p@.mat
        ctramp_output\\auto_@p@_SR2_GP_@p@.mat
        ctramp_output\\auto_@p@_SR2_HOV_@p@.mat
        ctramp_output\\auto_@p@_SR2_PAY_@p@.mat
        ctramp_output\\auto_@p@_SR3_GP_@p@.mat
        ctramp_output\\auto_@p@_SR3_HOV_@p@.mat
        ctramp_output\\auto_@p@_SR3_PAY_@p@.mat
        ctramp_output\\Nonmotor_@p@_BIKE_@p@.mat
        ctramp_output\\Nonmotor_@p@_WALK_@p@.mat
        ctramp_output\\other_@p@_SCHLBUS_@p@.mat

        Need to combine demands for one period into one omx file.
        """
        time_period_names = self.time_period_names

        # auto TAZ
        for period in time_period_names:
            output_path = (
                self.controller.get_abs_path(self.config.highway_demand_file)
                .__str__()
                .format(period=period, iter=self.controller.iteration)
            )
            output_omx = omx.open_file(output_path, "w")
            for mode_agg in self.config.mode_agg:
                if mode_agg.name == "transit":
                    continue
                for mode in mode_agg.modes:
                    input_path = (
                        self.controller.get_abs_path(
                            self.config.highway_taz_ctramp_output_file
                        )
                        .__str__()
                        .format(period=period, mode_agg=mode_agg.name, mode=mode)
                    )
                    input_omx = omx.open_file(input_path, "r")
                    core_name = mode + "_" + period.upper()
                    output_omx[core_name] = input_omx[core_name][:, :]
                    input_omx.close()

            output_omx.close()

        # auto MAZ
        for period in time_period_names:
            for maz_group in [1, 2, 3]:
                output_path = (
                    self.controller.get_abs_path(
                        self.controller.config.highway.maz_to_maz.demand_file
                    )
                    .__str__()
                    .format(
                        period=period, number=maz_group, iter=self.controller.iteration
                    )
                )

                input_path = (
                    self.controller.get_abs_path(
                        self.config.highway_maz_ctramp_output_file
                    )
                    .__str__()
                    .format(period=period, number=maz_group)
                )

                _shutil.copyfile(input_path, output_path)

        # transit TAP
        # for period in time_period_names:
        #    for set in ["set1", "set2", "set3"]:
        #        output_path = (
        #            self.controller.get_abs_path(self.config.transit_demand_file)
        #            .__str__()
        #            .format(period=period, iter=self.controller.iteration, set=set)
        #        )
        #        output_omx = omx.open_file(output_path, "w")
        #        for mode_agg in self.config.mode_agg:
        #            if mode_agg.name != "transit":
        #                continue
        #            for mode in mode_agg.modes:
        #                input_path = (
        #                    self.controller.get_abs_path(
        #                        self.config.transit_tap_ctramp_output_file
        #                    )
        #                    .__str__()
        #                    .format(
        #                        period=period,
        #                        mode_agg=mode_agg.name,
        #                        mode=mode,
        #                        set=set,
        #                    )
        #                )
        #                input_omx = omx.open_file(input_path, "r")
        #                core_name = mode + "_TRN_" + set + "_" + period.upper()
        #                output_omx[core_name] = input_omx[core_name][:, :]
        #                input_omx.close()
        #
        #        output_omx.close()
        # transit TAZ
        for period in time_period_names:
            output_path = (
                self.controller.get_abs_path(self.config.transit_demand_file)
                .__str__()
                .format(period=period, iter=self.controller.iteration)
            )
            output_omx = omx.open_file(output_path, "w")
            for mode_agg in self.config.mode_agg:
                if mode_agg.name != "transit":
                    continue
                for mode in mode_agg.modes:
                    input_path = (
                        self.controller.get_abs_path(
                            self.config.transit_taz_ctramp_output_file
                        )
                        .__str__()
                        .format(
                            period=period,
                            mode_agg=mode_agg.name,
                            mode=mode,
                        )
                    )
                    input_omx = omx.open_file(input_path, "r")
                    core_name = mode + "_TRN_" + period.upper()
                    output_omx[core_name] = input_omx[core_name][:, :]
                    input_omx.close()

            output_omx.close()
