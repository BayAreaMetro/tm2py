""" SetupModel implementation."""
import os
import pathlib
import shutil
import requests
import zipfile
import io
import logging
import pprint
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
        """Initializes an instance of the SetupModel class by reading config.

        Args:
            config_file (pathlib.Path): The TOML file with the model setup attributes.
            model_dir (pathlib.Path): The directory which to setup for a TM2 model run.
        """
        self.config_file = config_file
        self.setup_config = SetupConfig(self._load_toml())
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

        # Don't close down logging because custom setup script may need to perform
        # additional steps and want to log it.
        # logging.shutdown()


    def _create_run_model_batch(self):
        """
        Creates the RunModel.bat and RunModel.py in the root directory
        """

        if not self.model_dir.exists():
            self.logger.error(f"Directory {self.model_dir} does not exists.")
            raise FileNotFoundError(f"Directory {self.model_dir} does not exists.")
        
        # copy RunModel.py
        shutil.copy2(
            pathlib.Path(__file__).parent.absolute() / "RunModel.py",
            self.model_dir / "RunModel.py"
        )

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
        - COPY_NETWORK_INPUTS
        - COPY_POPLU_INPUTS
        - COPY_NONRES_INPUTS
        - COPY_WARMSTART_DEMAND
        - COPY_WARMSTART_SKIMS
        """
        self.logger.info(f"setup_config:\n{pprint.pformat(vars(self.setup_config))}")
        # Copy hwy and trn networks
        if self.setup_config.COPY_NETWORK_INPUTS:
            self.logger.info("Copying network inputs (hwy, trn)...")
            self._copy_folder(
                self.setup_config.INPUT_NETWORK_DIR / "hwy",
                self.model_dir / "inputs" / "hwy"
            )
            self._copy_folder(
                self.setup_config.INPUT_NETWORK_DIR / "trn",
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
            
            # otherwise, copy folder
            else:
                self._copy_folder(
                    self.setup_config.INPUT_EMME_NETWORK_DIR / DATABASE_TO_SOURCE[network_type] / "Database",
                    dest_dir
                )

        # Update EMME project file to support dual-database matrix serving
        # PR #223 introduced separate highway/transit databases, but matrix server
        # needs access to both databases to serve matrices from either one
        self._update_emme_project_for_dual_databases()

    def _update_emme_project_for_dual_databases(self):
        """
        Update EMME project file (.emp) to support dual-database architecture.
        
        PR #223 introduced separate highway and transit databases, but the matrix server
        needs access to both databases to serve matrices from either one. This method
        updates the OpenDatabases configuration to include both databases.
        """
        emp_files = list((self.model_dir / "emme_project").glob("*.emp"))
        if not emp_files:
            self.logger.warning("No EMME project file (.emp) found - skipping dual-database configuration")
            return
        
        emp_file = emp_files[0]  # Use first .emp file found
        self.logger.info(f"Updating EMME project file for dual-database support: {emp_file}")
        
        try:
            # Read the current project file
            with open(emp_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Update OpenDatabases to include both highway and transit databases
            # This allows the matrix server to serve matrices from both databases
            import re
            
            # Pattern to match OpenDatabases line
            open_db_pattern = r'(# String OpenDatabases:.*\n)(OpenDatabases = )([^\n]+)'
            
            def update_open_databases(match):
                comment_line = match.group(1)
                key_part = match.group(2)
                current_value = match.group(3)
                
                # Ensure both highway and transit databases are included
                databases_to_add = []
                if 'Database_highway\\emmebank' not in current_value:
                    databases_to_add.append('Database_highway\\emmebank')
                # Don't duplicate transit database if already present
                
                if not databases_to_add:
                    # All required databases already present
                    return match.group(0)
                
                # Add missing databases to existing ones
                if current_value.strip():
                    existing_dbs = [db.strip() for db in current_value.split(',')]
                    all_dbs = existing_dbs + databases_to_add
                else:
                    all_dbs = databases_to_add
                
                new_value = ','.join(all_dbs)
                self.logger.info(f"Updated OpenDatabases from '{current_value}' to '{new_value}'")
                
                return comment_line + key_part + new_value
            
            updated_content = re.sub(open_db_pattern, update_open_databases, content, flags=re.MULTILINE)
            
            if updated_content != content:
                # Write the updated content
                with open(emp_file, 'w', encoding='utf-8') as f:
                    f.write(updated_content)
                self.logger.info("Successfully updated EMME project file for dual-database matrix serving")
            else:
                self.logger.info("EMME project file already configured for dual-database access")
                
        except Exception as e:
            self.logger.error(f"Failed to update EMME project file for dual-database support: {e}")
            self.logger.error("Matrix server may not be able to serve from both highway and transit databases")

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
