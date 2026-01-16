"""Setup component for model initialization.

This component handles copying input files from source directories to the run directory
and initializing EMME network databases before the model run begins.
"""

from __future__ import annotations
from pathlib import Path
from typing import TYPE_CHECKING
import logging

from tm2py.components.component import Component
from tm2py.setup_model.setup import SetupModel, SetupConfig

if TYPE_CHECKING:
    from tm2py.controller import RunController


class Setup(Component):
    """Setup model component.
    
    This component wraps the SetupModel class to integrate model setup into the
    component workflow. It reads a setupmodel_config.toml file and:
    - Copies input files (highway, transit, land use, etc.) to run directory
    - Unzips and initializes EMME network databases
    - Copies warmstart files if configured
    - Downloads configuration files from GitHub if specified
    
    The setup component should typically be the first component in initial_components.
    """

    def __init__(self, controller: RunController):
        """Initialize Setup component.
        
        Args:
            controller: Reference to the run controller object
        """
        super().__init__(controller)
        self._setup_config_file = None
        self._setup_model = None

    def validate_inputs(self):
        """Validate that setup config file exists and is valid."""
        # Look for setupmodel_config.toml in run directory
        setup_config_path = self.controller.run_dir / "setupmodel_config.toml"
        
        if not setup_config_path.exists():
            raise FileNotFoundError(
                f"Setup component requires setupmodel_config.toml in run directory: {self.controller.run_dir}\n"
                f"Expected file: {setup_config_path}"
            )
        
        self._setup_config_file = setup_config_path
        self.logger.log(f"Found setup config file: {setup_config_path}")
        
        # Initialize SetupModel to validate configuration
        try:
            self._setup_model = SetupModel(
                config_file=self._setup_config_file,
                model_dir=self.controller.run_dir
            )
            # Load the config manually (SetupModel doesn't load it in __init__)
            import toml
            config_dict = toml.load(self._setup_config_file)
            from tm2py.setup_model.setup import SetupConfig
            self._setup_model.setup_config = SetupConfig(config_dict)
            # Validate the setup configuration
            self._setup_model.setup_config.validate()
            self.logger.log("Setup configuration validated successfully")
        except Exception as e:
            raise ValueError(f"Invalid setup configuration: {e}") from e

    def run(self):
        """Run the setup process."""
        self.logger.log("=" * 60)
        self.logger.log("RUNNING SETUP COMPONENT")
        self.logger.log("=" * 60)
        
        # Ensure validate_inputs has been called
        if self._setup_config_file is None:
            self.logger.log("Config file not set, calling validate_inputs first")
            self.validate_inputs()
        
        if self._setup_model is None:
            # Re-initialize if needed (shouldn't happen if validate_inputs was called)
            self._setup_model = SetupModel(
                config_file=self._setup_config_file,
                model_dir=self.controller.run_dir
            )
        
        self.logger.log(f"Setup config file: {self._setup_config_file}")
        self.logger.log(f"Model directory: {self.controller.run_dir}")
        
        # Load config manually to work around run_setup()'s directory existence check
        import toml
        config_dict = toml.load(self._setup_config_file)
        from tm2py.setup_model.setup import SetupConfig
        self._setup_model.setup_config = SetupConfig(config_dict)
        self._setup_model.setup_config.validate()
        
        # Initialize logging for SetupModel
        log_file = self.controller.run_dir / "setup.log"
        self._setup_model._setup_logging(log_file)
        
        # Copy model inputs (hwy, trn, land use, demand, etc.)
        self.logger.log("Copying model input files...")
        self._setup_model._copy_model_inputs()
        self.logger.log("✓ Model inputs copied")
        
        # Copy EMME project and databases
        self.logger.log("Copying and setting up EMME project...")
        self._setup_model._copy_emme_project_and_database()
        self.logger.log("✓ EMME project and databases set up")
        
        self.logger.log("Setup completed successfully")

    def report_progress(self):
        """Report setup progress to log."""
        self.logger.log("Setup component: copying input files and initializing EMME networks")

    def verify(self):
        """Verify that setup completed successfully.
        
        Checks that required directories and files were created:
        - inputs/ directory with subdirectories (hwy, trn, landuse, etc.)
        - emme_project/ with Database folders
        - Land use files specified in scenario config
        """
        run_dir = self.controller.run_dir
        
        # Check for inputs directory
        inputs_dir = run_dir / "inputs"
        if not inputs_dir.exists():
            self.logger.warn(f"Setup verify: inputs directory not found: {inputs_dir}")
            return
        
        # Check for key input subdirectories
        expected_subdirs = ["hwy", "trn", "landuse"]
        missing_dirs = []
        for subdir in expected_subdirs:
            if not (inputs_dir / subdir).exists():
                missing_dirs.append(subdir)
        
        if missing_dirs:
            self.logger.warn(f"Setup verify: missing input subdirectories: {missing_dirs}")
        
        # Check for EMME project
        emme_project_dir = run_dir / "emme_project"
        if not emme_project_dir.exists():
            self.logger.warn(f"Setup verify: emme_project directory not found: {emme_project_dir}")
            return
        
        # Check for EMME databases
        expected_databases = ["Database_highway", "Database_transit", "Database_active_north", "Database_active_south"]
        missing_databases = []
        for db_name in expected_databases:
            db_path = emme_project_dir / db_name
            if not db_path.exists():
                missing_databases.append(db_name)
        
        if missing_databases:
            self.logger.warn(f"Setup verify: missing EMME databases: {missing_databases}")
        else:
            self.logger.log("Setup verify: All EMME databases found")
        
        # Check for land use file specified in scenario config
        if hasattr(self.controller.config.scenario, 'maz_landuse_file'):
            landuse_file = run_dir / self.controller.config.scenario.maz_landuse_file
            if not landuse_file.exists():
                self.logger.warn(f"Setup verify: land use file not found: {landuse_file}")
            else:
                self.logger.log(f"Setup verify: land use file found: {landuse_file}")
        
        self.logger.log("Setup verification complete")

    def write_top_sheet(self):
        """Write setup information to top sheet (not implemented)."""
        pass


def validate_required_files(run_dir: Path, scenario_config, logger: logging.Logger) -> bool:
    """Validate that required input files exist when setup is skipped.
    
    This function should be called when the setup component is not included in
    initial_components to ensure the model can run without setup.
    
    Args:
        run_dir: Model run directory path
        scenario_config: Scenario configuration object
        logger: Logger instance
        
    Returns:
        True if all required files exist, False otherwise
        
    Raises:
        FileNotFoundError: If critical required files are missing
    """
    logger.log("Validating required input files (setup component was skipped)...")
    
    missing_files = []
    missing_dirs = []
    
    # Check for inputs directory and subdirectories
    inputs_dir = run_dir / "inputs"
    if not inputs_dir.exists():
        missing_dirs.append("inputs/")
    else:
        for subdir in ["hwy", "trn", "landuse"]:
            if not (inputs_dir / subdir).exists():
                missing_dirs.append(f"inputs/{subdir}/")
    
    # Check for EMME project and databases
    emme_project_dir = run_dir / "emme_project"
    if not emme_project_dir.exists():
        missing_dirs.append("emme_project/")
    else:
        for db_name in ["Database_highway", "Database_transit"]:
            if not (emme_project_dir / db_name).exists():
                missing_dirs.append(f"emme_project/{db_name}/")
    
    # Check for land use file
    if hasattr(scenario_config, 'maz_landuse_file'):
        landuse_file = run_dir / scenario_config.maz_landuse_file
        if not landuse_file.exists():
            missing_files.append(str(scenario_config.maz_landuse_file))
    
    if hasattr(scenario_config, 'zone_seq_file'):
        zone_seq_file = run_dir / scenario_config.zone_seq_file
        if not zone_seq_file.exists():
            missing_files.append(str(scenario_config.zone_seq_file))
    
    # Report findings
    if missing_dirs or missing_files:
        error_msg = "Required input files/directories are missing:\n"
        if missing_dirs:
            error_msg += f"  Missing directories: {', '.join(missing_dirs)}\n"
        if missing_files:
            error_msg += f"  Missing files: {', '.join(missing_files)}\n"
        error_msg += "\nEither:\n"
        error_msg += "  1. Add 'setup' to initial_components in scenario_config.toml, OR\n"
        error_msg += "  2. Manually copy required files to the run directory\n"
        
        raise FileNotFoundError(error_msg)
    
    logger.log("All required input files validated successfully")
    return True
