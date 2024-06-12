print("running")
import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from tqdm import tqdm

input_dir = Path(r"\\corp.pbwan.net\us\CentralData\DCCLDA00\Standard\sag\projects\MTC\US0024934.9168\Task_3_runtime_improvements\3.1_network_fidelity\run_result")
output_dir = input_dir / "consolidated"


in_file = next(input_dir.rglob('emme_links.shp'))
print("reading")
input = gpd.read_file(in_file)
print("writing")
input[["geometry"]].to_file(output_dir / "test_geom.geojson")

# def read_file_and_tag(path: Path) -> gpd.GeoDataFrame:
#     scenario = file.parent.stem
#     scenario_number = int(scenario.split("_")[-1])

#     run = file.parent.parent.stem
#     run_number = int(run.split("_")[-1])

#     return_gdf = gpd.read_file(path)

#     return_gdf["scenario"] = scenario
#     return_gdf["scenario_number"] = scenario_number
#     return_gdf["run"] = run
#     return_gdf["run_number"] = run_number
    
#     return return_gdf


    

# print("Reading Links...", end="")
# all_links = []
# x = 0
# for file in tqdm(input_dir.rglob('emme_links.shp')):
#     all_links.append(read_file_and_tag(file))
#     if x == 0:
#         all_links[-1][["geometry"]].to_file(output_dir / "test_min_geom.geojson")
#     x = x + 1 
# links_table = pd.concat(all_links)
# links_table = gpd.GeoDataFrame(links_table, geometry="geometry", crs=all_links[0].crs)
# print("done")

# print("reading Nodes...", end="")
# all_nodes = []
# for file in tqdm(input_dir.rglob('emme_nodes.shp')):
#     all_nodes.append(read_file_and_tag(file))

# nodes_table = pd.concat(all_nodes)
# nodes_table = gpd.GeoDataFrame(nodes_table, geometry="geometry", crs=all_links[0].crs)
# print("done")

# print("outputting files...")

# links_table.to_file(output_dir/"links.geojson")
# nodes_table.to_file(output_dir/"nodes.geojson")