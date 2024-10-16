# %%
import pandas as pd
import openmatrix as omx
from pathlib import Path

import numpy as np

network_fid_path = Path(
    r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\run_result"
)
# network_fid_path = Path(r"D:\TEMP\TM2.2.1.1-0.05")

# %%


def read_matrix_as_long_df(path: Path, run_name):
    run = omx.open_file(path, "r")
    am_time = np.array(run["AM_da_time"])
    index_lables = list(range(am_time.shape[0]))
    return (
        pd.DataFrame(am_time, index=index_lables, columns=index_lables)
        .stack()
        .rename(run_name)
        .to_frame()
    )


a = read_matrix_as_long_df(
    r"D:\TEMP\TM2.2.1.1-New_network_rerun\TM2.2.1.1_new_taz\skim_matrices\highway\HWYSKMAM_taz.omx",
    "test",
)
# %%
all_skims = []
for skim_matrix_path in network_fid_path.rglob("*AM_taz.omx"):
    print(skim_matrix_path)
    run_name = skim_matrix_path.parts[6]
    all_skims.append(read_matrix_as_long_df(skim_matrix_path, run_name))

all_skims = pd.concat(all_skims, axis=1)
# %%
# %%%
all_skims.to_csv(
    r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\output_summaries\skim_data\skims.csv"
)
# %%
# %%
import geopandas as gpd
from importlib import Path
import pandas as pd

# %%
output_paths_to_consolidate = Path(r"D:\TEMP\output_summaries")
all_files = []
for file in output_paths_to_consolidate.glob("*_roadway_network.geojson"):
    run_name = file.name[0:5]
    print(run_name)
    specific_run = gpd.read_file(file)
    specific_run["run_number"] = run_name
    all_files.append(specific_run)
# %%
all_files = pd.concat(all_files)
# %%
all_files.to_file(output_paths_to_consolidate / "all_runs_concat.gdb")

# %%

all_files.drop(columns="geometry").to_csv(output_paths_to_consolidate / "data.csv")
# %%
to_be_shape = all_files[["geometry", "model_link_id"]].drop_duplicates()
print("outputting")
to_be_shape.to_file(output_paths_to_consolidate / "geom_package")
