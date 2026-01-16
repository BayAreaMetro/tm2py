"""Configure EMME database extra attribute slots.

This script increases the number of extra attribute slots in an EMME database
to ensure there's enough room for all TM2 model attributes.
"""
import sys
from pathlib import Path

def configure_extra_attributes(emme_project_path, extra_link_attrs=50, extra_node_attrs=20):
    """Increase extra attribute slots in EMME database.
    
    Args:
        emme_project_path: Path to the emme_project folder
        extra_link_attrs: Number of extra link attributes to allocate
        extra_node_attrs: Number of extra node attributes to allocate
    """
    try:
        import inro.emme.desktop.app as _app
        import inro.emme.database.emmebank as _eb
    except ImportError:
        print("ERROR: EMME Python API not found. Must run from EMME Python environment.")
        sys.exit(1)
    
    emme_project = Path(emme_project_path)
    if not emme_project.exists():
        print(f"ERROR: EMME project not found: {emme_project}")
        sys.exit(1)
    
    print(f"Opening EMME project: {emme_project}")
    
    # Start EMME application
    app = _app.start_dedicated(project=str(emme_project), visible=False, user_initials="TM2")
    
    try:
        desktop = app.desktop
        
        # Find all emmebanks in the project
        emmebank_paths = list(emme_project.rglob("emmebank"))
        
        if not emmebank_paths:
            print("ERROR: No emmebank files found in project")
            return
        
        print(f"Found {len(emmebank_paths)} emmebank(s)")
        
        for emmebank_path in emmebank_paths:
            print(f"\nConfiguring: {emmebank_path.parent.name}")
            
            # Open the emmebank
            emmebank = _eb.Emmebank(str(emmebank_path))
            
            # Get current dimensions
            dims = emmebank.dimensions
            print(f"  Current extra link attributes: {dims['extra_attribute_values']}")
            
            # Set new dimensions
            new_dims = {
                'extra_attribute_values': extra_link_attrs,
            }
            
            print(f"  Setting extra link attributes to: {extra_link_attrs}")
            emmebank.change_dimensions(new_dims)
            
            # Verify
            dims = emmebank.dimensions
            print(f"  ✓ New extra link attributes: {dims['extra_attribute_values']}")
            
            emmebank.dispose()
        
        print("\n✓ All databases configured successfully!")
        
    finally:
        app.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python configure_emme_attributes.py <emme_project_path> [extra_link_attrs] [extra_node_attrs]")
        print("\nExample:")
        print("  python configure_emme_attributes.py E:/Tests/san_mateo_test/emme_project 50 20")
        sys.exit(1)
    
    emme_project_path = sys.argv[1]
    extra_link_attrs = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    extra_node_attrs = int(sys.argv[3]) if len(sys.argv) > 3 else 20
    
    configure_extra_attributes(emme_project_path, extra_link_attrs, extra_node_attrs)
