"""Compare attributes between EMME 23 template and EMME 25 database."""
import sys
from pathlib import Path

# Add tm2py to path for EMME imports
sys.path.insert(0, str(Path(__file__).parent.parent))

def check_emmebank_attributes(project_path):
    """Check what attributes exist in an emmebank."""
    import inro.emme.desktop.app as _app
    import inro.modeller as _m
    
    print(f"\nOpening project: {project_path}")
    
    try:
        # Start EMME desktop app with the project directory
        my_app = _app.start_dedicated(visible=False, user_initials="test", project=project_path)
        my_modeller = _m.Modeller(my_app)
        
        # Get the first scenario
        emmebank = my_modeller.emmebank
        scenario = emmebank.scenario(1)
        
        if not scenario:
            print("✗ No scenario found in emmebank")
            my_app.close()
            return
        
        print(f"✓ Found scenario: {scenario.id} - {scenario.title}")
        
        # Check for extra attributes
        network = scenario.get_network()
        
        print("\nExtra Link Attributes:")
        link_attrs = network.attributes('LINK')
        if link_attrs:
            for attr in sorted(link_attrs):
                print(f"  {attr}")
        else:
            print("  None found")
        
        print("\nExtra Node Attributes:")
        node_attrs = network.attributes('NODE')
        if node_attrs:
            for attr in sorted(node_attrs):
                print(f"  {attr}")
        else:
            print("  None found")
        
        # Check for required attributes
        required_attrs = ['@lanes', '@capclass', '@useclass', '@free_flow_time', '@free_flow_speed', '@area_type']
        print("\nRequired Attributes Check:")
        for attr in required_attrs:
            if attr in link_attrs:
                print(f"  ✓ {attr}")
            else:
                print(f"  ✗ {attr} MISSING")
        
        my_app.close()
        
    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print(f"\n{'='*80}")
    print(f"CHECKING EMME 23 PROJECT TEMPLATE (develop branch config)")
    print(f"{'='*80}")
    
    template_23 = Path(r"E:\Box\Modeling and Surveys\Development\Travel Model Two Conversion\Model Inputs\2015-tm22-dev-sprint-04\emme_23_project_template")
    
    if template_23.exists():
        print(f"Found template at: {template_23}")
        check_emmebank_attributes(str(template_23))
    else:
        print(f"✗ Template not found at {template_23}")
    
    print(f"\n{'='*80}")
    print(f"CHECKING EMME 25 PROJECT TEMPLATE")
    print(f"{'='*80}")
    
    template_25 = Path(r"E:\Box\Modeling and Surveys\Development\Travel Model Two Conversion\Model Inputs\2015-tm22-dev-sprint-04\emme_25_project_template")
    
    if template_25.exists():
        print(f"Found template at: {template_25}")
        check_emmebank_attributes(str(template_25))
    else:
        print(f"✗ Template not found at {template_25}")
