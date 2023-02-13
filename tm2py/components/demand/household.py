"""Placeholder docstring for CT-RAMP related components for household residents' model."""

import shutil as _shutil

from tm2py.components.component import Component
from tm2py.logger import LogStartEnd
from tm2py.tools import run_process


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
        self._start_household_manager()
        self._start_matrix_manager()
        self._run_resident_model()
        self._stop_java()

    def _start_household_manager(self):
        commands = [
            f"cd /d {self.controller.run_dir}",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\\runtime;C:\\Windows\\System32;%JAVA_PATH%\\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            f'CALL {self.controller.run_dir}\\CTRAMP\\runtime\\runHhMgr.cmd %JAVA_PATH% %HOST_IP_ADDRESS%',
        ]
        run_process(commands, name="start_household_manager")

    def _start_matrix_manager(self):
        commands = [
            f"cd /d {self.controller.run_dir}",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\\runtime;C:\\Windows\\System32;%JAVA_PATH%\\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            f'CALL {self.controller.run_dir}\\CTRAMP\\runtime\\runMtxMgr.cmd %HOST_IP_ADDRESS% %JAVA_PATH%',
        ]
        run_process(commands, name="start_matrix_manager")

    def _run_resident_model(self):
        sample_rate_iteration = {1: 0.3, 2: 0.5, 3: 1, 4: 0.02, 5: 0.02}
        iteration = self.controller.iteration
        sample_rate = sample_rate_iteration[iteration]
        _shutil.copyfile("CTRAMP\\runtime\\mtctm2.properties", "mtctm2.properties")
        commands = [
            f"cd /d {self.controller.run_dir}",
            f"CALL {self.controller.run_dir}\\CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\\runtime;C:\\Windows\\System32;%JAVA_PATH%\\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            f'CALL {self.controller.run_dir}\\CTRAMP\\runtime\\runMTCTM2ABM.cmd {sample_rate} {iteration} %JAVA_PATH%',
        ]
        run_process(commands, name="run_resident_model")

    @staticmethod
    def _stop_java():
        run_process(['taskkill /im "java.exe" /F'])
