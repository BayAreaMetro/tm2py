"""Placeholder docstring for CT-RAMP related components for Residents' model
"""

import os as _os
import shutil as _shutil

from tm2py.core.component import Component as _Component
import tm2py.core.tools as _tools


_join = _os.path.join


class ResidentsModel(_Component):
    """Run residents' model"""

    def __init__(self, controller):
        super().__init__(controller)

    def run(self):
        self._start_household_manager()
        self._start_matrix_manager()
        self._run_resident_model()
        self._stop_java()

    def _start_household_manager(self):
        commands = [
            "CALL CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\runtime;C:\\Windows\\System32;%JAVA_PATH%\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            'CALL CTRAMP\\runtime\\runHhMgr.cmd "%JAVA_PATH%" "%HOST_IP_ADDRESS%"',
        ]
        _tools.run_process(commands, name="start_household_manager")

    def _start_matrix_manager(self):
        commands = [
            "CALL CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\runtime;C:\\Windows\\System32;%JAVA_PATH%\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            'CALL CTRAMP\runtime\runMtxMgr.cmd %HOST_IP_ADDRESS% "%JAVA_PATH%"',
        ]
        _tools.run_process(commands, name="start_matrix_manager")

    def _run_resident_model(self):
        # TODO: move sample rates to config
        sample_rate_iteration = {1: 0.3, 2: 0.5, 3: 1, 4: 0.02, 5: 0.02}
        iteration = self.controller.iteration
        sample_rate = sample_rate_iteration[iteration]
        _shutil.copyfile("CTRAMP\\runtime\\mtctm2.properties", "mtctm2.properties")
        commands = [
            "CALL CTRAMP\\runtime\\CTRampEnv.bat",
            "set PATH=%CD%\\CTRAMP\runtime;C:\\Windows\\System32;%JAVA_PATH%\bin;"
            "%TPP_PATH%;%PYTHON_PATH%;%PYTHON_PATH%\\condabin;%PYTHON_PATH%\\envs",
            'CALL CTRAMP\runtime\runMTCTM2ABM.cmd {sample_rate} {iteration} "%JAVA_PATH%"'.format(
                sample_rate=sample_rate, iteration=iteration
            ),
        ]
        _tools.run_process(commands, name="run_resident_model")

    def _stop_java(self):
        _tools.run_process(['taskkill /im "java.exe" /F'])
