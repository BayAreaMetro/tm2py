""" SetupModel implementation."""
import os
import pathlib
import shutil
import requests
import zipfile
import io
import logging
import toml
import re
import socket
import sys

class SetupConfig:
    """ Simple class with attributes required for setting up a model
    """
    def __init__(self, config_dict: dict):
        """Intialize with given dictionary.

        Note that for keys that end with _DIR, the value is assumed to be a
        path, so a pathlib.Path is saved rather than a string.

        Additionally, if this is run on an MTC modeling machine (e.g. USERNAME==MTCPB)
        then the Box directory is assumed to be passed through as a local resource
        from Remote Desktop, so E:\Box is replaced with \\tsclient\E\Box as
        the Box location.

        Args:
            config_dict (dict): Assumes keys that end with _DIR point to
            pathlib.Path objects, otherwise assumes values are strings.
        """
        MTC_BOX_DIR = pathlib.Path("E:\Box")
        username = os.environ['USERNAME'].upper()

        for key, value in config_dict.items():
            # _DIR values are pathlib.Paths
            if key.upper().endswith("_DIR"):
                my_path = pathlib.Path(value)
                # if on an MTC modeling machine, and E:\Box isn't available,
                # assume the E:\Box drive should be accessed via \\tsclient\E\Box
                my_path_root = pathlib.Path(*my_path.parts[:2])
                if (username == "MTCPB") and (my_path_root == MTC_BOX_DIR) and (not MTC_BOX_DIR.exists()):
                    my_path = pathlib.Path("\\\\tsclient\\E\\Box", *my_path.parts[2:])
                setattr(self, key, my_path)
            else:
                setattr(self, key, value)
        
        # Set default values for optional copy flags if not specified
        if not hasattr(self, 'COPY_NETWORK_INPUTS'):
            self.COPY_NETWORK_INPUTS = True
        if not hasattr(self, 'COPY_POPLU_INPUTS'):
            self.COPY_POPLU_INPUTS = True
        if not hasattr(self, 'COPY_NONRES_INPUTS'):
            self.COPY_NONRES_INPUTS = True
        if not hasattr(self, 'COPY_WARMSTART_DEMAND'):
            self.COPY_WARMSTART_DEMAND = True
        if not hasattr(self, 'COPY_WARMSTART_SKIMS'):
            self.COPY_WARMSTART_SKIMS = True
        
    def validate(self):
        """Validates that all required attributes are present.

        Raises:
            ValueError: when required attribute is missing.
        """
        # validate setup configuration
        required_attrs = [
            "INPUT_NETWORK_DIR",
            "INPUT_POPLU_DIR",
            "WARMSTART_FILES_DIR",
            "TRAVEL_MODEL_TWO_RELEASE_TAG",
        ]

        for attr in required_attrs:
            if not getattr(self, attr, None):
                raise ValueError(f"{attr} is required in the setup configuration!")

class SetupModel:
    """
    Main operational interface for setup model process.
    """

    def __init__(self, config_file: pathlib.Path, model_dir: pathlib.Path):
        """Initializes an instance of the SetupModel class.

        Args:
            config_file (pathlib.Path): The TOML file with the model setup attributes.
            model_dir (pathlib.Path): The directory which to setup for a TM2 model run.
        """
        self.config_file = config_file
        self.setup_config = SetupConfig(dict())
        self.model_dir = model_dir

    def _setup_logging(self, log_file: pathlib.Path):
        """
        Setup a logger that logs to both the console and to the given log file.
        """
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        # console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
        self.logger.addHandler(ch)

        # file handler
        fh = logging.FileHandler(log_file, mode='w')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p'))
        self.logger.addHandler(fh)

    def _load_toml(self):
        """
        Load SetupConfig from toml file.

        Args:
            toml_path: path for toml file to read
        """
        with open(self.config_file, "r", encoding="utf-8") as toml_file:
            data = toml.load(toml_file)
        return data

    def run_setup(self):
        """
        Does the work of setting up the model.  
        
        This step will do the following within the model directory.

        1. Intialize logging to write to `setup.log`
        2. Copy the setup config file to `setupmodel_config.toml`
        3. Create the required folder structure
        4. Copy the input from the locations specified:
           a. hwy and trn networks
           b. popsyn and landuse inputs
           c. nonres inputs
           d. warmstart demand matrices
           e. warmstart skims
        5. Copy the Emme template project and Emme network databases 
           (based on the EMME version in sys.path)
        6. Download the travel model CTRAMP core code (runtime, uec) from the 
           [travel-model-two repository](https://github.com/BayAreaMetro/travel-model-two)
        7. Updates the IP address in the CTRAMP runtime properties files
        8. Creates `RunModel.py` for running the model

        Raises:
            FileExistsError: If the model directory to setup already exists.
        """
        # Read setup setup_config
        config_dict = self._load_toml()
        self.setup_config = SetupConfig(config_dict)
        self.setup_config.validate()

        # if the directory already exists - error and quit
        if self.model_dir.exists():
            raise FileExistsError(f"{self.model_dir.resolve()} already exists! Setup terminated.")
        else:
            self.model_dir.mkdir()

        # Initialize logging
        log_file = self.model_dir / "setup.log"
        self._setup_logging(log_file)

        self.logger.info(f"Starting process to setup MTC model in directory: {self.model_dir.resolve()}")

        # Save setup config into model dir as setupmodel_config.toml
        shutil.copy(self.config_file, self.model_dir / "setupmodel_config.toml")
        self.logger.info(f"Copied {self.config_file} to {self.model_dir / 'setupmodel_config.toml'}")

        # List of folders to create
        folders_to_create = [
            "acceptance",
            "CTRAMP",
            "ctramp_output",
            "demand_matrices",
            "demand_matrices/highway",
            "demand_matrices/highway/air_passenger",
            "demand_matrices/highway/household",
            "demand_matrices/highway/maz_demand",
            "demand_matrices/highway/internal_external",
            "demand_matrices/highway/commercial",
            "demand_matrices/transit",
            "emme_project",
            "inputs",
            "logs",
            "notebooks",
            "output_summaries",
            "skim_matrices",
            "skim_matrices/highway",
            "skim_matrices/transit",
            "skim_matrices/non_motorized",
        ]

        # Create folder structure
        self._create_folder_structure(folders_to_create)

        # Copy model inputs
        self._copy_model_inputs()

        # Copy emme project and database
        self._copy_emme_project_and_database()

        # Download toml SetupConfig files from GitHub
        config_files_list = [
            "observed_data.toml",
            "canonical_crosswalk.toml",
            "model_config.toml",
            "scenario_config.toml",
        ]
        acceptance_config_files_list = [
            "observed_data.toml",
            "canonical_crosswalk.toml",
        ]

        for file in config_files_list:
            github_url = self.setup_config.CONFIGS_GITHUB_PATH + "/" + file

            local_file = self.model_dir / file

            self._download_file_from_github(github_url, local_file)

        # Fetch required folders from travel model two github release (zip file)
        org = "BayAreaMetro"
        repo = "travel-model-two"
        tag = self.setup_config.TRAVEL_MODEL_TWO_RELEASE_TAG
        folders_to_extract = ["runtime", "uec"]

        self._download_github_release(
            org,
            repo,
            tag,
            folders_to_extract,
            self.model_dir / "CTRAMP"
        )

        # Rename 'uec' folder to 'model'
        old_path = self.model_dir / "CTRAMP" / "uec"
        old_path.rename(self.model_dir / "CTRAMP" / "model")

        self._create_run_model_batch()

        # update IP addresses in config files
        ips_here = socket.gethostbyname_ex(socket.gethostname())[-1]
        self.logger.info(f"Found the following IPs for this server: {ips_here}; using the first one: {ips_here[0]}")

        # add IP address to mtctm2.properties
        self._replace_in_file(
            self.model_dir / 'CTRAMP' / 'runtime' / 'mtctm2.properties', {
                "(\nRunModel.MatrixServerAddress[ \t]*=[ \t]*)(\S*)": f"\g<1>{ips_here[0]}",
                "(\nRunModel.HouseholdServerAddress[ \t]*=[ \t]*)(\S*)": f"\g<1>{ips_here[0]}"
            }
        )
        # add IP address to logsum.properties
        self._replace_in_file(
            self.model_dir / 'CTRAMP' / 'runtime' / 'logsum.properties', {
                "(\nRunModel.MatrixServerAddress[ \t]*=[ \t]*)(\S*)": f"\g<1>{ips_here[0]}",
                "(\nRunModel.HouseholdServerAddress[ \t]*=[ \t]*)(\S*)": f"\g<1>{ips_here[0]}"
            }
        )
        self.logger.info(f"Setup process completed successfully!")

        # Close logging
        logging.shutdown()


    def _create_run_model_batch(self):
        """
        Creates the RunModel.bat and RunModel.py in the root directory
        """

        if not self.model_dir.exists():
            self.logger.error(f"Directory {self.model_dir} does not exists.")
            raise FileNotFoundError(f"Directory {self.model_dir} does not exists.")
        
        # create RunModel.py
        with open(self.model_dir / 'RunModel.py', 'w', encoding='utf-8') as file:
            self.logger.info(f"Creating RunModel.py in directory {self.model_dir}")
            file.write(_RUN_MODEL_PY_CONTENT)


    def _create_folder_structure(self, folder_names: list[str]):
        """
        Creates empty folder structure in the root directory

        Args:
            folder_names: list of folders to create
            self.model_dir: root directory for the model
        """

        self.logger.info(f"Creating folder structure in directory {self.model_dir.resolve()}")

        if not self.model_dir.exists():
            error_str = f"Directory {self.model_dir} does not exist."
            self.logger.error(error_str)
            raise FileNotFoundError(error_str)

        for folder in folder_names:
            path = self.model_dir / folder
            path.mkdir()
            self.logger.info(f"  Created Empty Folder: {path}")

    def _copy_folder(self, src_dir: pathlib.Path, dest_dir: pathlib.Path):
        """
        Copies a folder from the source directory to the destination directory.

        Args:
            src: source folder
            dest: destination folder
        """

        if not src_dir.exists():
            error_str = f"Source directory {src_dir} to copy from does not exist"
            self.logger.error(error_str)
            raise FileNotFoundError(error_str)
        
        # Copy the entire folder and its contents
        try:
            # Check if the destination directory exists
            if dest_dir.exists():
                # delete the existing destination directory
                # Newer versions supports `dirs_exist_ok` but with this version,
                # the destination directory must not already exist
                shutil.rmtree(dest_dir)

            # Create destination directory first
            dest_dir.mkdir(parents=True, exist_ok=True)
            
            # Copy contents of source directory to destination
            # This avoids creating nested directory structure
            for item in src_dir.iterdir():
                if item.is_dir():
                    shutil.copytree(item, dest_dir / item.name)
                else:
                    shutil.copy2(item, dest_dir / item.name)

            self.logger.info(f"Copied contents of folder from {src_dir} to {dest_dir}")
        except Exception as e:
            error_str = f"Failed to copy {src_dir} to {dest_dir}: {str(e)}"
            self.logger.error(error_str)
            raise Exception(error_str)


    def _download_file_from_github(self, github_url: str, local_file: pathlib.Path):
        """
        Downloads a file from a GitHub URL.

        Args:
            github_url: raw github link for the file to download
            local_file: local path for the file to download
        """
        try:
            response = requests.get(github_url)
            response.raise_for_status()
            self.logger.debug(f"Downloading file from {github_url} to {local_file.resolve()}")

            with open(local_file, "wb") as f:
                # write the content of the response (file content) to the local file
                f.write(response.content)
        except Exception as e:
            error_str = f"Failed to download file {github_url} from GitHub to {local_file.resolve()}: {str(e)}"
            self.logger.error(error_str)
            raise Exception(error_str)

    def _download_github_release(
        self, org_name: str, repo_name: str, release_tag: str, folders_to_extract: list[str], local_dir: pathlib.Path
    ):
        """
        download a release ZIP from a GitHub repository and extract specified sub-folders to a local directory.

        Args:
            org_name: github organization name
            repo_name: github repository name
            release_tag: release tag
            folders_to_extract: list of sub-folders to extract from the ZIP file
            local_dir: local directory to save extracted folders
        """
        release_url = f"https://github.com/{org_name}/{repo_name}/archive/refs/tags/{release_tag}.zip"

        try:
            response = requests.get(release_url)
            response.raise_for_status()

            root_folder = f"{repo_name}-{release_tag}"
            copied_folder = set([])

            z = zipfile.ZipFile(io.BytesIO(response.content))
            for file_info in z.infolist():
                if not file_info.is_dir():
                    if file_info.filename.startswith(root_folder):
                        file_path = file_info.filename[len(root_folder) + 1 :]
                    else:
                        file_path = file_info.filename

                    if any(
                        file_path.startswith(folder) for folder in folders_to_extract
                    ):
                        # create the local path to extract the file
                        extract_path = local_dir / file_path

                        # ensure the directory exists
                        os.makedirs(os.path.dirname(extract_path), exist_ok=True)

                        # extract the file
                        with z.open(file_info.filename) as source, open(
                            extract_path, "wb"
                        ) as target:
                            target.write(source.read())

                        copied_folder.add(file_path.split("/")[0])

            if copied_folder is not None:
                self.logger.info(
                    f"Extracted folders {copied_folder} from GitHub release {release_url} and to directory {local_dir}"
                )

        except Exception as e:
            error_str = f"Failed to download GitHub release {release_url}: {str(e)}"
            self.logger.error(error_str)
            raise Exception(error_str)

    def _copy_model_inputs(self):
        """
        copy required model inputs into their respective directories.
        Uses optional flags in setup config to control what gets copied:
        - COPY_NETWORK_INPUTS (default: True)
        - COPY_POPLU_INPUTS (default: True)
        - COPY_NONRES_INPUTS (default: True)
        - COPY_WARMSTART_DEMAND (default: True)
        - COPY_WARMSTART_SKIMS (default: True)
        """
        # Copy hwy and trn networks
        if self.setup_config.COPY_NETWORK_INPUTS:
            self.logger.info("Copying network inputs (hwy, trn)...")
            self._copy_folder(
                self.setup_config.INPUT_NETWORK_DIR / "hwy",
                self.model_dir / "inputs" / "hwy"
            )
            # Support both 'trn' and 'transit' folder names
            trn_source = self.setup_config.INPUT_NETWORK_DIR / "trn"
            if not trn_source.exists():
                trn_source = self.setup_config.INPUT_NETWORK_DIR / "transit"
            self._copy_folder(
                trn_source,
                self.model_dir / "inputs" / "trn"
            )
        else:
            self.logger.info("Skipping network inputs (COPY_NETWORK_INPUTS=False)")

        # Copy popsyn and landuse inputs
        if self.setup_config.COPY_POPLU_INPUTS:
            self.logger.info("Copying population and land use inputs...")
            self._copy_folder(
                self.setup_config.INPUT_POPLU_DIR / "popsyn",
                self.model_dir / "inputs" / "popsyn"
            )
            self._copy_folder(
                self.setup_config.INPUT_POPLU_DIR /"landuse",
                self.model_dir / "inputs" / "landuse"
            )
        else:
            self.logger.info("Skipping popsyn/landuse inputs (COPY_POPLU_INPUTS=False)")

        # Copy nonres inputs
        if self.setup_config.COPY_NONRES_INPUTS:
            self.logger.info("Copying non-residential inputs...")
            self._copy_folder(
                self.setup_config.INPUT_NONRES_DIR / "nonres",
                self.model_dir / "inputs" / "nonres"
            )
        else:
            self.logger.info("Skipping nonres inputs (COPY_NONRES_INPUTS=False)")

        # Copy warmstart demand if exists
        if self.setup_config.COPY_WARMSTART_DEMAND:
            warmstart_demand = self.setup_config.WARMSTART_FILES_DIR / "demand_matrices"
            if warmstart_demand.exists():
                self.logger.info("Copying warmstart demand matrices...")
                self._copy_folder(
                    warmstart_demand, 
                    self.model_dir / "demand_matrices"
                )
            else:
                self.logger.info(f"Warmstart demand directory not found: {warmstart_demand}")
        else:
            self.logger.info("Skipping warmstart demand (COPY_WARMSTART_DEMAND=False)")

        # Copy warmstart skims
        if self.setup_config.COPY_WARMSTART_SKIMS:
            warmstart_skims = self.setup_config.WARMSTART_FILES_DIR / "skim_matrices"
            if warmstart_skims.exists():
                self.logger.info("Copying warmstart skim matrices...")
                self._copy_folder(
                    warmstart_skims, 
                    self.model_dir /"skim_matrices"
                )
            else:
                self.logger.info(f"Warmstart skims directory not found: {warmstart_skims}")
        else:
            self.logger.info("Skipping warmstart skims (COPY_WARMSTART_SKIMS=False)")

    def _copy_emme_project_and_database(self):
        """
        Copy EMME project from template project and then copy the emme networks databases based
        on the EMME version found in the sys.path.
        """
        # copy template emme project
        # Check if template has nested emme_project subdirectory (common structure)
        template_dir = self.setup_config.EMME_TEMPLATE_PROJECT_DIR
        if (template_dir / "emme_project").exists():
            # Template has emme_project subdirectory, copy its contents
            self.logger.info(f"Template has nested emme_project subdirectory, copying from: {template_dir / 'emme_project'}")
            self._copy_folder(
                template_dir / "emme_project",
                self.model_dir / "emme_project"
            )
        else:
            # Template is the project itself, copy it directly
            self.logger.info(f"Template is the project directory, copying from: {template_dir}")
            self._copy_folder(
                template_dir,
                self.model_dir / "emme_project"
            )

        # get emme version from sys.path
        sys_paths = sys.path
        emme_path = None
        for sys_path in sys_paths:
            if sys_path.find("EMME") >=0 and sys_path.find("Bentley") >= 0:
                emme_path = pathlib.Path(sys_path)
                self.logger.info(f"Found EMME path: {emme_path}")
                break
        if emme_path is None:
            error_str = f"emme_path not found in sys.path {sys_paths}. Please run setup from EMME command prompt"
            self.logger.error(error_str)
            raise ValueError(error_str) 

        EMME_VERSION = None
        for part in emme_path.parts:
            if part.startswith("EMME"):
                EMME_VERSION = part.replace(" ","_")  # replace spaces with underscores
                self.logger.info(f"Found EMME version in emme_path: {EMME_VERSION}")
                break

        if EMME_VERSION is None:
            error_str = f"EMME version not found in emme_path {emme_path}. Please run setup from EMME command prompt"
            self.logger.error(error_str)
            raise ValueError(error_str) 

        # copy versioned, zipped emme network database, falling back to unversioned if necessary
        # Map network_type to legacy folder names (older structure)
        DATABASE_TO_SOURCE = {
            'highway': 'emme_drive_network',
            'transit': 'emme_taz_transit_network',
            'active_north': 'emme_maz_active_modes_network_subregion_north',
            'active_south': 'emme_maz_active_modes_network_subregion_south'
        }
        for network_type in DATABASE_TO_SOURCE.keys():
            source_file = self.setup_config.INPUT_EMME_NETWORK_DIR / f"Database_{network_type}_{EMME_VERSION}.zip"
            dest_dir = self.model_dir / "emme_project" / f"Database_{network_type}"
            if source_file.exists():
                # remove what was there before (if it exists)
                if dest_dir.exists():
                    shutil.rmtree(dest_dir)
                # unzip the EMME version of the ntework
                with zipfile.ZipFile(source_file, 'r') as zf:
                    zf.extractall(dest_dir.parent)
                self.logger.info(f"Unzipped {source_file} to {dest_dir}")
            
            # otherwise, copy folder - try multiple possible source locations
            else:
                # Try new structure first: Database_highway directly in emme_project
                source_dir = self.setup_config.INPUT_EMME_NETWORK_DIR / f"Database_{network_type}"
                if not source_dir.exists():
                    # Fall back to legacy structure: emme_drive_network/Database
                    source_dir = self.setup_config.INPUT_EMME_NETWORK_DIR / DATABASE_TO_SOURCE[network_type] / "Database"
                
                if source_dir.exists():
                    self._copy_folder(source_dir, dest_dir)
                else:
                    self.logger.warning(f"Database source not found for {network_type}, skipping: {source_dir}")

    def _replace_in_file(self, filepath: pathlib.Path, regex_dict: dict[str, str]):
        """
        Copies `filepath` to `filepath.original`
        Opens `filepath.original` and reads it, writing a new version to `filepath`.
        The new version is the same as the old, except that the regexes in the regex_dict keys
        are replaced by the corresponding values.
        """
        original_copy = pathlib.Path(f"{str(filepath.absolute())}.original")
        shutil.move(filepath, original_copy)
        self.logger.info(f"_replace_in_file: Updating {filepath} via {original_copy}")

        # read the contents
        myfile = open(original_copy, 'r', encoding='utf-8')
        file_contents = myfile.read()
        myfile.close()

        # do the regex subs
        for pattern,newstr in regex_dict.items():
            (file_contents, numsubs) = re.subn(pattern,newstr,file_contents,flags=re.IGNORECASE)
            self.logger.info(f"  Made {numsubs} sub for {newstr}")
 
           # Raise exception on failure
            if numsubs < 1:
                error_str = f"  SUBSITUTION FOR PATTERN {pattern} NOT MADE -- Fatal error"
                self.logger.fatal(error_str)
                raise ValueError(error_str)

        # write the result
        myfile = open(filepath, 'w', encoding='utf-8')
        myfile.write(file_contents)
        myfile.close()

_RUN_MODEL_PY_CONTENT = """
import pathlib
import random
import subprocess
import sys
import traceback
import tm2py
import toml

def notify_slack(message, extra_info=None):
    \"\"\"Send notification to Slack using the notify_slack.py script\"\"\"
    try:
        # Check if Slack notifications are enabled in config
        config_file = pathlib.Path("scenario_config.toml")
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                config = toml.load(f)
            slack_enabled = config.get("slack_notifications", {}).get("enabled", False)
        else:
            slack_enabled = False
        
        if not slack_enabled:
            print(f"Slack notifications disabled. Message: {message}")
            return
        
        # Get the path to the tm2py scripts directory
        tm2py_path = pathlib.Path(tm2py.__file__).parent.parent
        notify_script = tm2py_path / "scripts" / "notify_slack.py"
        
        # Check if the notify script exists
        if not notify_script.exists():
            print(f"Slack notification script not found at {notify_script}. Message: {message}")
            return
        
        # Build enhanced message with extra info
        enhanced_message = message
        if extra_info:
            enhanced_message += f"\\n{extra_info}"
        
        # Run the notification script
        subprocess.run([sys.executable, str(notify_script), enhanced_message], 
                      check=True, capture_output=True, text=True)
        print(f"Slack notification sent: {enhanced_message}")
    except Exception as e:
        print(f"Failed to send Slack notification: {e}")
        print(f"Message was: {message}")

if __name__ == "__main__":
    import datetime
    run_successful = False
    error_message = ""
    start_time = datetime.datetime.now()
    
    # Get current directory and scenario info for context
    current_dir = pathlib.Path(".").resolve()
    
    # Try to get scenario name from config
    scenario_name = "Unknown Scenario"
    scenario_year = "Unknown Year"
    try:
        config_file = pathlib.Path("scenario_config.toml")
        if config_file.exists():
            with open(config_file, "r", encoding="utf-8") as f:
                config = toml.load(f)
            scenario_name = config.get("scenario", {}).get("name", "Unknown Scenario")
            scenario_year = config.get("scenario", {}).get("year", "Unknown Year")
    except Exception:
        pass
    
    # Build enhanced start notification with run configuration
    start_info = f"📍 Directory: {current_dir}\\n🏷️ Scenario: {scenario_name} ({scenario_year})\\n⏰ Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}"
    
    # Add iteration info if available in config
    try:
        start_iteration = config.get("run", {}).get("start_iteration", 0)
        end_iteration = config.get("run", {}).get("end_iteration", 1)
        if start_iteration is not None and end_iteration is not None:
            start_info += f"\\n🔄 Iterations: {start_iteration} to {end_iteration}"
    except Exception:
        pass
    
    # Send start notification
    notify_slack(f"🚀 Travel Model Two run starting", start_info)
    
    try:
        controller = tm2py.RunController(
            config_file = ["scenario_config.toml", "model_config.toml"],
            run_dir = pathlib.Path(".")
        )
        controller.run()
        run_successful = True
        
    except Exception as e:
        error_message = str(e)
        print(f"Model run failed with error: {error_message}")
        traceback.print_exc()
    
    # Calculate runtime
    end_time = datetime.datetime.now()
    runtime = end_time - start_time
    runtime_str = str(runtime).split('.')[0]  # Remove microseconds
    
    # Send Slack notification based on run status
    if run_successful:
        rewards = [
            "tiramisu",
            "a long run",
            "bunny pets",
            "a nap",
            "dancing parrot",
            "well-constructed gluten-free vegan cake",
            "a pat on the back from Dave Vautin"
        ]
        reward = random.choice(rewards)
        
        success_info = f"🏷️ Scenario: {scenario_name} ({scenario_year})\\n⏱️ Runtime: {runtime_str}\\n📍 Directory: {current_dir}\\n⏰ Completed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        # Try to add summary results if topsheet exists
        try:
            topsheet_path = current_dir / "output_summaries" / "topsheet.csv"
            if topsheet_path.exists():
                import pandas as pd
                df = pd.read_csv(topsheet_path)
                # Look for key metrics
                vmt_row = df[df['Metric'].str.contains('Total Daily VMT', case=False, na=False)]
                if not vmt_row.empty:
                    vmt_value = vmt_row.iloc[0]['Value']
                    if isinstance(vmt_value, (int, float)):
                        vmt_millions = vmt_value / 1_000_000
                        success_info += f"\\n📊 Total Daily VMT: {vmt_millions:.1f}M"
                        
                        # Check for truck VMT split
                        car_vmt_row = df[df['Metric'].str.contains('Total Daily Car VMT', case=False, na=False)]
                        truck_vmt_row = df[df['Metric'].str.contains('Total Daily Truck VMT', case=False, na=False)]
                        if not car_vmt_row.empty and not truck_vmt_row.empty:
                            car_vmt = car_vmt_row.iloc[0]['Value'] / 1_000_000
                            truck_vmt = truck_vmt_row.iloc[0]['Value'] / 1_000_000
                            truck_pct = (truck_vmt / vmt_millions * 100) if vmt_millions > 0 else 0
                            success_info += f"\\n🚗 Car VMT: {car_vmt:.1f}M | 🚛 Truck VMT: {truck_vmt:.1f}M ({truck_pct:.1f}%)"
        except Exception:
            # Don't fail notification if we can't read results
            pass
        
        notify_slack(f"✅ Travel Model Two run completed successfully! Go get {reward}", success_info)
    else:
        # Random motivating failure messages
        motivating_messages = [
            "They say failure is part of the process in engineering. If that's true, I must be crushing the process.",
            "Every model run teaches us something new. This one taught us patience.",
            "Debugging is like being the detective in a crime movie where you are also the murderer.",
            "Error messages are just the model's way of asking for help.",
            "Rome wasn't built in a day, and neither was a perfect travel model.",
            "This isn't a failure, it's a learning opportunity with attitude.",
            "Even the best models need a timeout sometimes.",
            "Consider this a feature request from reality.",
            "The model is just taking a creative approach to problem-solving.",
            "Sometimes the journey is more important than the destination... but not today.",
            "Think of it as aggressive testing of error handling systems.",
            "The model is practicing mindfulness by stopping to reflect.",
            "Failure is success in progress... very, very slow progress.",
            "This is just the model's way of saying it needs more coffee.",
            "Error: Task failed successfully (at failing).",
            "The model decided to take the scenic route through Errorville.",
            "It's not a bug, it's an undocumented feature of disappointment.",
            "The model is just expressing its artistic side through creative failure.",
            "Congratulations! You've discovered a new way for things to go wrong.",
            "The model is conducting an impromptu stress test on your patience.",
            "Error messages are like fortune cookies, but less helpful.",
            "The model is just really committed to the whole 'fail fast' philosophy.",
            "This failure brought to you by the department of unexpected plot twists.",
            "The model decided to practice interpretive dance instead of running.",
            "At least the model is consistent... consistently surprising.",
            "The model is taking a mental health day.",
            "This is what happens when models try to think outside the box.",
            "The model is just showing off its extensive vocabulary of error codes.",
            "Failure is the spice of life, and this one is extra spicy.",
            "The model is auditioning for a role in a tragedy instead of a success story."
        ]
        motivating_message = random.choice(motivating_messages)
        
        failure_info = f"🏷️ Scenario: {scenario_name} ({scenario_year})\\n⏱️ Runtime: {runtime_str}\\n📍 Directory: {current_dir}\\n❌ Error: {error_message}\\n⏰ Failed: {end_time.strftime('%Y-%m-%d %H:%M:%S')}"
        
        notify_slack(f"❌ Travel Model Two run failed", failure_info + f"\\n\\n💭 {motivating_message}")
    
    # Exit with appropriate code
    sys.exit(0 if run_successful else 1)
"""