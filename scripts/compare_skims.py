#%%
import pandas as pd
import openmatrix as omx
from pathlib import Path

import numpy as np

network_fid_path = Path(r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\run_result")

#%%

def read_matrix_as_long_df(path: Path, run_name):
    run = omx.open_file(path, "r")
    am_time = np.array(run["AM_da_time"])
    index_lables = list(range(am_time.shape[0]))
    return pd.DataFrame(am_time, index=index_lables, columns=index_lables).stack().rename(run_name).to_frame()

all_skims = []
for skim_matrix_path in network_fid_path.rglob("*AM_taz.omx"):
    run_name = skim_matrix_path.parts[6]
    all_skims.append(read_matrix_as_long_df(skim_matrix_path, run_name))

all_skims = pd.concat(all_skims, axis=1)
# %%
#%%%
all_skims.to_csv(r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\run_result\consolidated\skims.csv")
# %%
