""" SetupModel implementation."""
import os
import pathlib
import shutil
import requests
import zipfile
import io
import logging
import toml
import socket
import re

class SetupConfig:
    """ Simple class with attributes required for setting up a model
    """
    def __init__(self, config_dict: dict):
        """Intialize with given dictionary

        Args:
            config_dict (dict): Assumes keys that end with _DIR point to
            pathlib.Path objects, otherwise assumes values are strings.
        """
    
        for key, value in config_dict.items():
            # _DIR values are pathlib.Paths
            if key.upper().endswith("_DIR"):
                setattr(self, key, pathlib.Path(value))
            else:
                setattr(self, key, value)
        
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
        2. Create the required folder structure
        3. Copy the input from the locations specified:
           a. hwy and trn networks
           b. popsyn and landuse inputs
           c. nonres inputs
           d. warmstart demand matrices
           e. warmstart skims
        4. Copy the Emme template project and Emme network databases
        5. Download the travel model CTRAMP core code (runtime, uec) from the 
           [travel-model-two repository](https://github.com/BayAreaMetro/travel-model-two)
        6. Updates the IP address in the CTRAMP runtime properties files
        7. Creates `RunModel.py` for running the model

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
        with open(self.model_dir / 'RunModel.py', 'w') as file:
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

            shutil.copytree(src_dir, dest_dir)

            self.logger.info(f"Copied folder from {src_dir} to {dest_dir}")
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
        """
        # Copy hwy and trn networks
        self._copy_folder(
            self.setup_config.INPUT_NETWORK_DIR / "hwy",
            self.model_dir / "inputs" / "hwy"
        )
        self._copy_folder(
            self.setup_config.INPUT_NETWORK_DIR / "trn",
            self.model_dir / "inputs" / "trn"
        )

        # Copy popsyn and landuse inputs
        self._copy_folder(
            self.setup_config.INPUT_POPLU_DIR / "popsyn",
            self.model_dir / "inputs" / "popsyn"
        )
        self._copy_folder(
            self.setup_config.INPUT_POPLU_DIR /"landuse",
            self.model_dir / "inputs" / "landuse"
        )

        # Copy nonres inputs
        self._copy_folder(
            self.setup_config.INPUT_NONRES_DIR / "nonres",
            self.model_dir / "inputs" / "nonres"
        )

        # Copy warmstart demand if exists
        warmstart_demand = self.setup_config.WARMSTART_FILES_DIR / "demand_matrices"
        if warmstart_demand.exists():
            self._copy_folder(
                warmstart_demand, 
                self.model_dir / "demand_matrices"
            )

        # Copy warmstart skims
        warmstart_skims = self.setup_config.WARMSTART_FILES_DIR / "skim_matrices"
        if warmstart_skims.exists():
            self._copy_folder(
                warmstart_skims, 
                self.model_dir /"skim_matrices"
            )

    def _copy_emme_project_and_database(self):
        """
        copy emme projects from template project and then copy the emme networks databases
        """
        # copy template emme project
        self._copy_folder(
            self.setup_config.EMME_TEMPLATE_PROJECT_DIR,
            self.model_dir / "emme_project"
        )

        # copy emme network database
        self._copy_folder(
            self.setup_config.INPUT_EMME_NETWORK_DIR / "emme_drive_network" / "Database",
            self.model_dir / "emme_project" / "Database_highway"
        )
        self._copy_folder(
            self.setup_config.INPUT_EMME_NETWORK_DIR / "emme_taz_transit_network" / "Database",
            self.model_dir / "emme_project" / "Database_transit"
        )
        self._copy_folder(
            self.setup_config.INPUT_EMME_NETWORK_DIR / "emme_maz_active_modes_network_subregion_north" / "Database",
            self.model_dir / "emme_project" /"Database_active_north"
        )
        self._copy_folder(
            self.setup_config.INPUT_EMME_NETWORK_DIR / "emme_maz_active_modes_network_subregion_south" / "Database",
            self.model_dir / "emme_project" /"Database_active_south"
        )

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
        myfile = open(original_copy, 'r')
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
        myfile = open(filepath, 'w')
        myfile.write(file_contents)
        myfile.close()

_RUN_MODEL_PY_CONTENT = """
import pathlib
import tm2py

if __name__ == "__main__":
    controller = tm2py.RunController(
        config_file = ["scenario_config.toml", "model_config.toml"],
        run_dir = pathlib.Path("."),
        # all components
        run_components = None
    )
    controller.run()
"""