"""Check what attributes exist in a zipped EMME database.

This verifies if @lanes and other required attributes exist before running the test.
"""
import zipfile
import tempfile
import shutil
from pathlib import Path
import sys

# Add tm2py to path for EMME imports
sys.path.insert(0, str(Path(__file__).parent.parent))

def check_database_attributes(zip_path):
    """Extract and check attributes in an EMME database."""
    print(f"\nChecking database: {zip_path}")
    print("=" * 70)
    
    # Create temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        # Extract zip
        print(f"Extracting to: {temp_path}")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_path)
        
        # Find emmebank file
        emmebank_files = list(temp_path.rglob("emmebank"))
        if not emmebank_files:
            print("ERROR: No emmebank file found in zip!")
            return False
        
        emmebank_path = emmebank_files[0]
        print(f"Found emmebank: {emmebank_path}")
        
        # Try to open with EMME
        try:
            from inro.emme.database.emmebank import Emmebank
        except ImportError:
            print("ERROR: Cannot import EMME. Run this from EMME Python environment!")
            return False
        
        print("\nOpening emmebank...")
        emmebank = Emmebank(str(emmebank_path))
        
        # Get first scenario
        scenarios = list(emmebank.scenarios())
        if not scenarios:
            print("ERROR: No scenarios in database!")
            return False
        
        scenario = scenarios[0]
        print(f"Using scenario: {scenario.id} - {scenario.title}")
        
        # List all extra attributes
        print("\n" + "="*70)
        print("EXTRA ATTRIBUTES IN DATABASE:")
        print("="*70)
        
        link_attrs = []
        node_attrs = []
        turn_attrs = []
        transit_line_attrs = []
        transit_segment_attrs = []
        
        for attr in scenario.extra_attributes():
            if attr.type == "LINK":
                link_attrs.append(attr.name)
            elif attr.type == "NODE":
                node_attrs.append(attr.name)
            elif attr.type == "TURN":
                turn_attrs.append(attr.name)
            elif attr.type == "TRANSIT_LINE":
                transit_line_attrs.append(attr.name)
            elif attr.type == "TRANSIT_SEGMENT":
                transit_segment_attrs.append(attr.name)
        
        print(f"\nLINK attributes ({len(link_attrs)}):")
        for attr in sorted(link_attrs):
            print(f"  {attr}")
        
        print(f"\nNODE attributes ({len(node_attrs)}):")
        for attr in sorted(node_attrs):
            print(f"  {attr}")
        
        if turn_attrs:
            print(f"\nTURN attributes ({len(turn_attrs)}):")
            for attr in sorted(turn_attrs):
                print(f"  {attr}")
        
        if transit_line_attrs:
            print(f"\nTRANSIT_LINE attributes ({len(transit_line_attrs)}):")
            for attr in sorted(transit_line_attrs):
                print(f"  {attr}")
        
        if transit_segment_attrs:
            print(f"\nTRANSIT_SEGMENT attributes ({len(transit_segment_attrs)}):")
            for attr in sorted(transit_segment_attrs):
                print(f"  {attr}")
        
        # Check for required attributes
        print("\n" + "="*70)
        print("REQUIRED ATTRIBUTES CHECK:")
        print("="*70)
        
        required_link_attrs = ["@lanes", "@useclass", "@toll", "@capacity"]
        
        print("\nLINK attributes:")
        all_present = True
        for attr in required_link_attrs:
            present = attr in link_attrs
            status = "✓" if present else "✗ MISSING"
            print(f"  {status} {attr}")
            if not present:
                all_present = False
        
        emmebank.dispose()
        
        return all_present


if __name__ == "__main__":
    # Check the highway database that we'll be using
    database_path = Path("E:/Box/Modeling and Surveys/Development/Travel Model Two Conversion/Model Inputs/2015-tm22-dev-sprint-04/emme_network/Database_highway_EMME_25.00.01.zip")
    
    if not database_path.exists():
        print(f"ERROR: Database not found: {database_path}")
        sys.exit(1)
    
    success = check_database_attributes(database_path)
    
    if success:
        print("\n" + "="*70)
        print("✓ ALL REQUIRED ATTRIBUTES PRESENT")
        print("="*70)
        sys.exit(0)
    else:
        print("\n" + "="*70)
        print("✗ MISSING REQUIRED ATTRIBUTES - Database is incomplete")
        print("="*70)
        sys.exit(1)
