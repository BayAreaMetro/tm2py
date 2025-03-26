import os
import shutil
import requests
import zipfile
import io
import logging
import toml

from pathlib import Path


class SetupModel:
    """
    Main operational interface for setup model process.


    """

    def __init__(self, config_file):
        self.config_file = config_file
        self.configs = Config(dict())
        self.model_dir = Path()

    def _setup_logging(self, log_file):
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%d-%b-%Y (%H:%M:%S)",
        )
        return logging.getLogger()

    def _load_toml(self):
        """
        Load config from toml file.

        Args:
            toml_path: path for toml file to read
        """
        with open(self.config_file, "r", encoding="utf-8") as toml_file:
            data = toml.load(toml_file)
        return data

    def run_setup(self):
        # Read setup configs
        config_dict = self._load_toml()
        configs = Config(config_dict)

        # Validate configs
        configs.validate()

        self.configs = configs

        # Create model directory
        model_dir = os.path.join(configs.ROOT_DIR, configs.MODEL_FOLDER_NAME)
        model_dir = model_dir.replace("\\", "/")
        self.model_dir = Path(model_dir)
        # if the directory already exists - error and quit
        if self.model_dir.exists():
            raise Exception(f"{self.model_dir} already exist! Setup terminated.")
        else:
            os.mkdir(model_dir)

        # Initialize logging
        log_file = os.path.join(model_dir, "setup_log.log")
        logger = self._setup_logging(log_file)

        logger.info(f"Starting process to setup MTC model in directory: {model_dir}")

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
        self._create_folder_structure(folders_to_create, model_dir, logger)

        # Copy model inputs
        self._copy_model_inputs(configs, model_dir, logger)

        # Copy emme project and database
        self._copy_emme_project_and_database(configs, model_dir, logger)

        # Download toml config files from GitHub
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
            github_url = os.path.join(configs.CONFIGS_GITHUB_PATH, file)
            github_url = github_url.replace("\\", "/")

            local_file = os.path.join(
                model_dir,
                "acceptance" if file in acceptance_config_files_list else "",
                file,
            )
            local_file = local_file.replace("\\", "/")

            self._download_file_from_github(github_url, local_file, logger)

        # Fetch required folders from travel model two github release (zip file)
        org = "BayAreaMetro"
        repo = "travel-model-two"
        tag = configs.TRAVEL_MODEL_TWO_RELEASE_TAG
        folders_to_extract = ["runtime", "uec"]

        self._download_github_release(
            org,
            repo,
            tag,
            folders_to_extract,
            os.path.join(model_dir, "CTRAMP"),
            logger,
        )

        # Rename 'uec' folder to 'model'
        os.rename(
            os.path.join(model_dir, "CTRAMP", "uec"),
            os.path.join(model_dir, "CTRAMP", "model"),
        )

        self._create_run_model_batch(logger)

        logger.info(f"Setup process completed successfully!")

        # Close logging
        logging.shutdown()


    def _create_run_model_batch(self, logger):
        """
        Creates the RunModel.bat and RunModel.py in the root directory

        Args:
            logger: logger
        """

        if not self.model_dir.exists():
            logger.error(f"Directory {self.model_dir} does not exists.")
            raise FileNotFoundError(f"Directory {self.model_dir} does not exists.")
        
        # create RunModel.bat
        with open(self.model_dir / 'RunModel.bat', 'w') as file:
            logger.info(f"Creating RunModel.bat in directory {self.model_dir}")
            file.write(_RUN_MODEL_BAT_CONTENT)

        # create RunModel.py
        with open(self.model_dir / 'RunModel.py', 'w') as file:
            logger.info(f"Creating RunModel.py in directory {self.model_dir}")
            file.write(_RUN_MODEL_PY_CONTENT)


    def _create_folder_structure(self, folder_names, root_dir, logger):
        """
        Creates empty folder structure in the root directory

        Args:
            folder_names: list of folders to create
            root_dir: root directory for the model
            logger: logger
        """

        logger.info(f"Creating folder structure in directory {root_dir}")

        if not os.path.exists(root_dir):
            logger.error(f"Directory {root_dir} does not exists.")
            raise FileNotFoundError(f"Directory {root_dir} does not exists.")

        for folder in folder_names:
            path = os.path.join(root_dir, folder)
            os.makedirs(path)
            logger.info(f"  Created Empty Folder: {path}")

    def _copy_folder(self, src_dir, dest_dir, logger):
        """
        Copies a folder from the source directory to the destination directory.

        Args:
            src: source folder
            dest: destination folder
            logger: logger
        """

        src_dir = src_dir.replace("\\", "/")
        dest_dir = dest_dir.replace("\\", "/")

        if os.path.exists(src_dir):
            # Copy the entire folder and its contents
            try:
                # Check if the destination directory exists
                if os.path.exists(dest_dir):
                    # delete the existing destination directory
                    # Newer versions supports `dirs_exist_ok` but with this version,
                    # the destination directory must not already exist
                    shutil.rmtree(dest_dir)

                shutil.copytree(src_dir, dest_dir)

                logger.info(f"Copied folder from {src_dir} to {dest_dir}")
            except Exception as e:
                logger.error(f"Failed to copy {src_dir} to {dest_dir}: {str(e)}")
                raise Exception(f"Failed to copy {src_dir} to {dest_dir}: {str(e)}")
        else:
            logger.error(f"Source directory {src_dir} to copy from does not exists.")
            raise FileNotFoundError(
                f"Source directory {src_dir} to copy from does not exists."
            )

    def _download_file_from_github(self, github_url, local_file, logger):
        """
        Downloads a file from a GitHub URL.

        Args:
            github_url: raw github link for the file to download
            local_file: local path for the file to download
            logger: logger
        """
        try:
            response = requests.get(github_url)
            response.raise_for_status()

            with open(local_file, "wb") as f:
                # write the content of the response (file content) to the local file
                f.write(response.content)
            logger.info(f"Downloaded file from {github_url} to {local_file}")
        except Exception as e:
            logger.error(f"Failed to download file {github_url} from GitHub: {str(e)}")
            raise Exception(
                f"Failed to download file {github_url} from GitHub: {str(e)}"
            )

    def _download_github_release(
        self, org_name, repo_name, release_tag, folders_to_extract, local_dir, logger
    ):
        """
        download a release ZIP from a GitHub repository and extract specified sub-folders to a local directory.

        Args:
            org_name: github organization name
            repo_name: github repository name
            release_tag: release tag
            folders_to_extract: list of sub-folders to extract from the ZIP file
            local_dir: local directory to save extracted folders
            logger: logger
        """
        release_url = f"https://github.com/{org_name}/{repo_name}/archive/refs/tags/{release_tag}.zip"

        try:
            response = requests.get(release_url)
            response.raise_for_status()

            root_folder = f"{repo_name}-{release_tag}"
            copied_folder = set([])
            local_dir = local_dir.replace("\\", "/")

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
                        extract_path = os.path.join(local_dir, file_path)
                        extract_path = extract_path.replace("\\", "/")

                        # ensure the directory exists
                        os.makedirs(os.path.dirname(extract_path), exist_ok=True)

                        # extract the file
                        with z.open(file_info.filename) as source, open(
                            extract_path, "wb"
                        ) as target:
                            target.write(source.read())

                        copied_folder.add(file_path.split("/")[0])

            if copied_folder is not None:
                logger.info(
                    f"Extracted folders {copied_folder} from GitHub release {release_url} and to directory {local_dir}"
                )

        except Exception as e:
            logger.error(f"Failed to download GitHub release {release_url}: {str(e)}")
            raise Exception(
                f"Failed to download GitHub release {release_url}: {str(e)}"
            )

    def _copy_model_inputs(self, configs, model_dir, logger):
        """
        copy required model inputs into their respective directories.

        Args:
            configs: setup model configurations
            model_dir: path to model directory
            logger: logger
        """
        # Copy hwy and trn networks
        self._copy_folder(
            os.path.join(configs.INPUT_NETWORK_DIR, "hwy"),
            os.path.join(model_dir, "inputs", "hwy"),
            logger,
        )
        self._copy_folder(
            os.path.join(configs.INPUT_NETWORK_DIR, "trn"),
            os.path.join(model_dir, "inputs", "trn"),
            logger,
        )

        # Copy popsyn and landuse inputs
        self._copy_folder(
            os.path.join(configs.INPUT_POPLU_DIR, "popsyn"),
            os.path.join(model_dir, "inputs", "popsyn"),
            logger,
        )
        self._copy_folder(
            os.path.join(configs.INPUT_POPLU_DIR, "landuse"),
            os.path.join(model_dir, "inputs", "landuse"),
            logger,
        )

        # Copy nonres inputs
        self._copy_folder(
            os.path.join(configs.INPUT_NONRES_DIR, "nonres"),
            os.path.join(model_dir, "inputs", "nonres"),
            logger,
        )

        # Copy warmstart demand if exists
        if os.path.exists(
            os.path.join(configs.WARMSTART_FILES_DIR, "demand_matrices")
        ):
            self._copy_folder(
                os.path.join(configs.WARMSTART_FILES_DIR, "demand_matrices"), 
                os.path.join(model_dir, "demand_matrices"), 
                logger
            )

        # Copy warmstart skims
        if os.path.exists(
            os.path.join(configs.WARMSTART_FILES_DIR, "skim_matrices")
        ):
            self._copy_folder(
                os.path.join(configs.WARMSTART_FILES_DIR, "skim_matrices"), 
                os.path.join(model_dir, "skim_matrices"), 
                logger
            )

    def _copy_emme_project_and_database(self, configs, model_dir, logger):
        """
        copy emme projects from template project and then copy the emme networks databases

        Args:
            configs: setup model configurations
            model_dir: path to model directory
            ogger: logger
        """
        # copy template emme project
        self._copy_folder(
            configs.EMME_TEMPLATE_PROJECT_DIR,
            os.path.join(model_dir, "emme_project"),
            logger,
        )

        # copy emme network database
        self._copy_folder(
            os.path.join(
                configs.INPUT_EMME_NETWORK_DIR, "emme_drive_network", "Database"
            ),
            os.path.join(model_dir, "emme_project", "Database_highway"),
            logger,
        )
        self._copy_folder(
            os.path.join(
                configs.INPUT_EMME_NETWORK_DIR, "emme_taz_transit_network", "Database"
            ),
            os.path.join(model_dir, "emme_project", "Database_transit"),
            logger,
        )
        self._copy_folder(
            os.path.join(
                configs.INPUT_EMME_NETWORK_DIR,
                "emme_maz_active_modes_network_subregion_north",
                "Database",
            ),
            os.path.join(model_dir, "emme_project", "Database_active_north"),
            logger,
        )
        self._copy_folder(
            os.path.join(
                configs.INPUT_EMME_NETWORK_DIR,
                "emme_maz_active_modes_network_subregion_south",
                "Database",
            ),
            os.path.join(model_dir, "emme_project", "Database_active_south"),
            logger,
        )


class Config:
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)

    def validate(self):
        # validate setup configuration
        required_attrs = [
            "ROOT_DIR",
            "MODEL_FOLDER_NAME",
            "INPUT_NETWORK_DIR",
            "INPUT_POPLU_DIR",
            "WARMSTART_FILES_DIR",
            "CONFIGS_GITHUB_PATH",
            "TRAVEL_MODEL_TWO_RELEASE_TAG",
        ]

        for attr in required_attrs:
            if not getattr(self, attr, None):
                raise ValueError(f"{attr} is required in the setup configuration!")

_RUN_MODEL_BAT_CONTENT = """
:: the directory that this file is in
SET MODEL_RUN_DIR=%~p0
cd /d %MODEL_RUN_DIR%

SET EMMEPATH=C:\Program Files\INRO\Emme\Emme 4\Emme-4.7.0.11
SET PATH=%EMMEPATH%\programs;%PATH%

CALL "C:\ProgramData\Anaconda3\condabin\conda" activate tm2py
python RunModel.py

CALL "C:\ProgramData\Anaconda3\condabin\conda" deactivate
PAUSE
"""

_RUN_MODEL_PY_CONTENT = """
import os
from osgeo import gdal
from tm2py.controller import RunController

controller = RunController(
    [
        os.path.join(os.getcwd(), "scenario_config.toml"),
        os.path.join(os.getcwd(), "model_config.toml")
    ],
    run_dir = os.getcwd()
)

controller.run()
"""