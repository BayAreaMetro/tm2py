# %%
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
from shapely.geometry import LineString

input_dir = Path(
    r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\run_result"
)
output_dir = input_dir / "consolidated_3"


# in_file = next(input_dir.rglob('emme_links.shp'))
# print("reading", in_file)
# input2 = gpd.read_file(in_file, engine="pyogrio", use_arrow=True)
# #%%
# print("writing")
# input[["#link_id", "geometry"]].to_file(output_dir / "test_geom.geojson")

scenarios_to_consolidate = (11, 12, 13, 14, 15)
runs_to_consolidate = (3, 4)
# %%


def read_file_and_tag(
    path: Path,
    columns_to_filter=(
        "@ft",
        "VOLAU",
        "@capacity",
        "run_number",
        "scenario_number",
        "#link_id",
        "geometry",
    ),
) -> pd.DataFrame:
    scenario = file.parent.stem
    scenario_number = int(scenario.split("_")[-1])
    if scenario_number not in scenarios_to_consolidate:
        return None

    run = file.parent.parent.stem
    run_number = int(run.split("_")[-1])
    if run_number not in runs_to_consolidate:
        return None

    return_gdf = gpd.read_file(path, engine="pyogrio")

    return_gdf["scenario"] = scenario
    return_gdf["scenario_number"] = scenario_number
    return_gdf["run"] = run
    return_gdf["run_number"] = run_number

    if "VOLAU" not in return_gdf.columns:
        print(return_gdf.columns)
        print("... No VOLAU, filling with zero")
        return_gdf["VOLAU"] = 0

    return_gdf = return_gdf[list(columns_to_filter)]

    # assert return_gdf["#link_id"].is_unique

    return return_gdf


def get_linestring_direction(linestring: LineString) -> str:
    if not isinstance(linestring, LineString) or len(linestring.coords) < 2:
        raise ValueError("Input must be a LineString with at least two coordinates")

    start_point = linestring.coords[0]
    end_point = linestring.coords[-1]

    delta_x = end_point[0] - start_point[0]
    delta_y = end_point[1] - start_point[1]

    if abs(delta_x) > abs(delta_y):
        if delta_x > 0:
            return "East"
        else:
            return "West"
    else:
        if delta_y > 0:
            return "North"
        else:
            return "South"


# %%

print("Reading Links...", end="")
all_links = []
for file in tqdm(input_dir.rglob("run_*/Scenario_*/emme_links.shp")):
    print(file)
    all_links.append(read_file_and_tag(file))
links_table = pd.concat(all_links)

print("done")
# %%
scen_map = {11: "EA", 12: "AM", 13: "MD", 14: "PM", 15: "EV"}


def get_return_first_gem(row):
    geom_columns = [col for col in row.index if "geometry" in col]
    return [
        row[col]
        for col in geom_columns
        if (row[col] is not None) and (row[col] != np.NAN)
    ][0]


def combine_tables(dfs, columns_same):
    return_frame = dfs[0][columns_same]

    for df in dfs:
        run_number = df["run_number"].iloc[0]

        scen_number = df["scenario_number"].iloc[0]
        scen_number = scen_map[scen_number]
        df["saturation"] = df["VOLAU"] / df["@capacity"]

        df = df[["#link_id", "@capacity", "VOLAU", "geometry", "@ft"]].rename(
            columns={
                "@capacity": f"capacity_run{run_number}_scen{scen_number}",
                "VOLAU": f"@volau_run{run_number}_scen{scen_number}",
                "saturation": f"@saturation_run{run_number}_scen{scen_number}",
                "geometry": f"geometry_run{run_number}_scen{scen_number}",
                "@ft": f"ft_run{run_number}_scen{scen_number}",
            }
        )
        # if there are link_ids that are not in the right frame
        return_frame = return_frame.merge(
            df, how="outer", on="#link_id", validate="1:1"
        )
        geometry = return_frame.apply(get_return_first_gem, axis=1)
        # remove geometries that are not main geometry
        return_frame = return_frame.drop(
            columns=[col for col in return_frame.columns if "geometry_" in col]
        )
        return_frame["geometry"] = geometry

    return return_frame


all_links_no_none = [
    links
    for links in all_links
    if (links is not None) and (links["#link_id"].is_unique)
]
links_wide_table = combine_tables(all_links_no_none, ["#link_id", "geometry"])

links_wide_table["direction"] = links_wide_table["geometry"].apply(
    get_linestring_direction
)
# %%
ft_cols = [col for col in links_wide_table.columns if "ft_" in col]

links_wide_table["ft"] = links_wide_table[ft_cols].max(axis=1)
links_wide_table = links_wide_table.drop(columns=ft_cols)

# %%
links_wide_table.to_file(
    Path(
        r"Z:\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\output_summaries\all_links_data"
    )
    / "all_data_wide.geojson"
)


# %%
num_iter = {(3, 11): 3, (3, 12): 10, (3, 13): 10, (3, 14): 19, (3, 15): 4, (4, 12): 20}
# %%
all_links_no_none = [
    links for links in all_links if (links is not None)
]  # and (links["#link_id"].is_unique)]
for df in all_links_no_none:
    df["saturation"] = df["VOLAU"] / df["@capacity"]
ft6_sat = [
    (
        link["run_number"].iloc[0],
        link["scenario_number"].iloc[0],
        (link.loc[link["@ft"] == 6, "saturation"] > 1).mean(),
    )
    for link in all_links_no_none
]

y = [val for val in num_iter.values()]
x = [x[-1] for x in ft6_sat]
col = [val[0] for val in num_iter.keys()]

# %%
import matplotlib.pyplot as plt

plt.scatter(x, y, c=col)

# Calculate the trendline
z = np.polyfit(x, y, 1)
p = np.poly1d(z)

# Plot the trendline
plt.plot(x, p(x), color="red")

plt.xlabel("proportion of ft 6 with saturation > 1")
plt.ylabel("number of iterations to solve")
plt.title("Number of iterations to solve (relative gap = 0.05)")
plt.show()
# %%
import matplotlib.pyplot as plt

data = [links_wide_table[col] for col in links_wide_table.iloc[:, 2:].columns]

fig = plt.boxplot(data)
fig.show()

# --------------------------------------------------------------------------
# %%
links_table["direction"] = links_table["geometry"].apply(get_linestring_direction)
# %%
links_table.to_file(output_dir / "all_data.geojson", index=False)


# %%
def get_link_counts(df: pd.DataFrame):
    ret_val = df.value_counts("@ft").sort_index().to_frame().T
    total = ret_val.sum(axis=1)
    total_minus_8 = total - ret_val[8.0].iloc[0]
    ret_val["total"] = total
    ret_val["total_minus_8"] = total_minus_8

    ret_val["run_number"] = df["run_number"].iloc[0]
    ret_val["scenario_number"] = df["scenario_number"].iloc[0]
    return ret_val


pd.concat([get_link_counts(df) for df in all_links]).sort_values(
    by=["run_number", "scenario_number"]
)
