#%%
import pandas as pd
import openmatrix as omx
from pathlib import Path
import plotly.express as px

import numpy as np

# network_fid_path = Path(r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\run_result")
# output_path = Path(r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\output_summaries\skim_data")

network_fid_path = Path(r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.2_remove_cosmetic_nodes\run_result")
output_path = Path(r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.2_remove_cosmetic_nodes\output_summaries\skim_data")
output_csv = False

def read_matrix_as_long_df(path: Path, run_name):
    run = omx.open_file(path, "r")
    am_time = np.array(run["AM_da_time"])
    index_lables = list(range(am_time.shape[0]))
    return pd.DataFrame(am_time, index=index_lables, columns=index_lables).stack().rename(run_name).to_frame()

#%%
all_skims = []
# runs_to_include = ['run_1\\', 'run_3', 'run_5', 'run_11', 'run_12', 'run_15', 'run_16', 'run_17']
runs_to_include = ['run_18', 'run_20']
for skim_matrix_path in network_fid_path.rglob("*AM_taz.omx"):
    for run_label in runs_to_include:
        if run_label in str(skim_matrix_path):
            print(skim_matrix_path)
            run_name = skim_matrix_path.parts[6]
            all_skims.append(read_matrix_as_long_df(skim_matrix_path, run_name))

all_skims = pd.concat(all_skims, axis=1)
# %%
#%%%
all_skims = all_skims.astype("float32")
if output_csv:
    all_skims.to_csv(output_path / "skims.csv")
else:
    print("warning not outputting")
#%%
scatterplots = []
skims_dropped = all_skims.copy()
for col in skims_dropped.columns:
    skims_dropped = skims_dropped[skims_dropped[col] <= 1e19]

scatter_plot = px.scatter(skims_dropped.sample(100_000), x="run_18", y="run_20")
scatter_plot.write_html(output_path / "run_18_and_20.html")
#%%
import matplotlib.pyplot as plt
plt.scatter(skims_dropped["run_18"], skims_dropped["run_20"])
plt.xlabel("run_18 skim (time)")
plt.ylabel("run_20 skim (time)")

plt.plot([0, 0], [250, 250], color='red', linestyle='--')
#%%
from scipy.stats import pearsonr, linregress
pearsonr(skims_dropped["run_18"], skims_dropped["run_20"])
linregress(skims_dropped["run_18"], skims_dropped["run_20"])
# %%
# %%
# import geopandas as gpd
# from importlib import Path
# import pandas as pd 
# #%%
# output_paths_to_consolidate = Path(r"D:\TEMP\output_summaries")
# all_files = []
# for file in output_paths_to_consolidate.glob("*_roadway_network.geojson"):
#     run_name = file.name[0:5]
#     print(run_name)
#     specific_run = gpd.read_file(file)
#     specific_run["run_number"] = run_name
#     all_files.append(specific_run)
# #%%
# all_files = pd.concat(all_files)
# #%%
# all_files.to_file(output_paths_to_consolidate / "all_runs_concat.gdb")
 
# #%%
 
# all_files.drop(columns="geometry").to_csv(output_paths_to_consolidate / "data.csv")
# #%%
# to_be_shape = all_files[["geometry", "model_link_id"]].drop_duplicates()
# print("outputting")
# to_be_shape.to_file(output_paths_to_consolidate / "geom_package")