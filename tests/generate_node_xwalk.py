"""Generate node_xwalk.csv from EMME network.

This creates the model_node_id to emme_node_id crosswalk needed for sequential zone indexing.
Run this from EMME Python environment.
"""

import sys
from pathlib import Path
import pandas as pd


def generate_node_xwalk(emme_database_path: str, output_file: str):
    """Extract node IDs from EMME network and create crosswalk."""
    
    # Import EMME modules
    try:
        from inro.emme.database.emmebank import Emmebank
    except ImportError:
        print("ERROR: Must run from EMME Python environment")
        sys.exit(1)
    
    # Open emmebank
    emmebank_path = Path(emme_database_path)
    if not emmebank_path.exists():
        print(f"ERROR: Emmebank not found: {emmebank_path}")
        sys.exit(1)
    
    print(f"Opening emmebank: {emmebank_path}")
    emmebank = Emmebank(str(emmebank_path))
    
    # Get first available scenario
    scenarios = list(emmebank.scenarios())
    if not scenarios:
        print("ERROR: No scenarios found in emmebank")
        sys.exit(1)
    
    scenario = scenarios[0]
    print(f"Using scenario {scenario.id}: {scenario.title}")
    
    # Get network
    network = scenario.get_network()
    
    # Extract node IDs
    node_data = []
    for node in network.nodes():
        node_data.append({
            'emme_node_id': node.id,
            'model_node_id': node.id  # In this case they're the same
        })
    
    # Create DataFrame
    df = pd.DataFrame(node_data)
    print(f"Extracted {len(df)} nodes")
    
    # Save to CSV
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved node crosswalk to: {output_path}")
    
    emmebank.dispose()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python generate_node_xwalk.py <emmebank_path> <output_file>")
        print("Example: python generate_node_xwalk.py E:/Tests/san_mateo_test/emme_project/Database_highway/emmebank inputs/hwy/node_xwalk.csv")
        sys.exit(1)
    
    emmebank_path = sys.argv[1]
    output_file = sys.argv[2]
    
    generate_node_xwalk(emmebank_path, output_file)
