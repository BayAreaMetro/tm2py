"""Minimal test: Run SOLA assignment directly on Sprint-04 output emmebank.

This bypasses all our test framework to confirm EMME assignment works.
Run this from EMME Shell with tm2pyenv activated.
"""
import os
os.environ["EMMEPATH"] = r"C:\Program Files\Bentley\OpenPaths\EMME 24.01.00"

import inro.modeller as _m

# Path to the Sprint-04 output emmebank (which already worked)
PROJECT_PATH = r"E:\Box\Modeling and Surveys\Development\Travel Model Two Conversion\Model Outputs\2015-tm22-dev-sprint-04\emme_project\TM2.emp"

print(f"Opening project: {PROJECT_PATH}")
modeller = _m.Modeller(PROJECT_PATH)
print(f"Project opened successfully")

# Get the assignment tool
assign = modeller.tool("inro.emme.traffic_assignment.sola_traffic_assignment")
print("Assignment tool loaded")

# Check what scenarios exist
eb = modeller.emmebank
print(f"\nEmmebank: {eb.path}")
print("Scenarios:")
for scen in eb.scenarios():
    print(f"  {scen.id}: {scen.title}")

# Try to get scenario 11 (AM)
scenario = eb.scenario(11)
if scenario:
    print(f"\nUsing scenario 11: {scenario.title}")
else:
    print("\nScenario 11 not found, using first available")
    scenario = list(eb.scenarios())[0]
    print(f"Using scenario {scenario.id}: {scenario.title}")

# Check if demand matrices exist
print("\nChecking for demand matrices:")
for name in ["AM_da", "AM_datoll", "AM_sr2"]:
    full_name = f"mf{name}"
    mat = eb.matrix(full_name)
    if mat:
        print(f"  {full_name}: exists")
    else:
        print(f"  {full_name}: NOT FOUND")

# Build a minimal assignment spec
spec = {
    "type": "SOLA_TRAFFIC_ASSIGNMENT",
    "classes": [{
        "mode": "d",
        "demand": "mfAM_da",
        "generalized_cost": {
            "link_costs": "@cost_da",
            "perception_factor": 1.0
        },
        "results": {
            "link_volumes": None  # Don't save volumes
        }
    }],
    "stopping_criteria": {
        "max_iterations": 1,  # Just one iteration
        "relative_gap": 0.5,
        "normalized_gap": 0.0,
        "best_relative_gap": 0.0
    },
    "performance_settings": {
        "number_of_processors": 4
    }
}

print("\nRunning minimal assignment (1 iteration, single class)...")
print("If this works, EMME is functioning correctly.")
print("-" * 50)

try:
    assign(spec, scenario, chart_log_interval=1)
    print("-" * 50)
    print("SUCCESS! Assignment completed without errors.")
except Exception as e:
    print("-" * 50)
    print(f"FAILED: {e}")
