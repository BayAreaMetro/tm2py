"""
Configuration Page for TM2PY GUI

Handles editing of scenario_config.toml and model_config.toml files.
"""

import streamlit as st
from pathlib import Path
import toml

def show():
    """Display the Configuration page."""
    
    st.markdown('<div class="section-header">⚙️ Configuration</div>', unsafe_allow_html=True)
    
    if not st.session_state.model_setup_complete:
        st.warning("Please complete model setup first before editing configuration.")
        return
    
    # Configuration file tabs
    tab1, tab2 = st.tabs(["📄 Scenario Config", "🔧 Model Config"])
    
    with tab1:
        show_scenario_config()
    
    with tab2:
        show_model_config()

def show_scenario_config():
    """Display scenario configuration editor."""
    
    st.markdown("### Scenario Configuration")
    st.info("Edit scenario_config.toml to customize your model run components and settings.")
    
    if st.session_state.scenario_config_path:
        config_path = Path(st.session_state.scenario_config_path)
        
        if config_path.exists():
            # Load and display current config
            try:
                with open(config_path, 'r') as f:
                    config_content = f.read()
                
                # Text editor for TOML content
                edited_content = st.text_area(
                    "Edit Scenario Configuration:",
                    value=config_content,
                    height=400,
                    help="Edit the TOML configuration. Be careful with syntax!"
                )
                
                # Save button
                col1, col2, col3 = st.columns([1, 1, 2])
                
                with col1:
                    if st.button("💾 Save Changes", type="primary"):
                        save_config(config_path, edited_content)
                
                with col2:
                    if st.button("🔄 Reset"):
                        st.rerun()
                
                with col3:
                    if st.button("✅ Validate TOML"):
                        validate_toml(edited_content)
                
                # Show parsed config preview
                if st.checkbox("Show Parsed Configuration Preview"):
                    try:
                        parsed_config = toml.loads(edited_content)
                        st.json(parsed_config)
                    except Exception as e:
                        st.error(f"TOML parsing error: {e}")
                        
            except Exception as e:
                st.error(f"Error reading configuration file: {e}")
        else:
            st.error(f"Configuration file not found: {config_path}")
    else:
        st.warning("No scenario configuration path set. Please complete model setup first.")

def show_model_config():
    """Display model configuration editor."""
    
    st.markdown("### Model Configuration")
    st.info("Edit model_config.toml to customize model performance settings and options.")
    
    if st.session_state.model_config_path:
        config_path = Path(st.session_state.model_config_path)
        
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config_content = f.read()
                
                # Text editor for TOML content
                edited_content = st.text_area(
                    "Edit Model Configuration:",
                    value=config_content,
                    height=400,
                    key="model_config_editor"
                )
                
                # Save button
                col1, col2, col3 = st.columns([1, 1, 2])
                
                with col1:
                    if st.button("💾 Save Changes", type="primary", key="save_model_config"):
                        save_config(config_path, edited_content)
                
                with col2:
                    if st.button("🔄 Reset", key="reset_model_config"):
                        st.rerun()
                
                with col3:
                    if st.button("✅ Validate TOML", key="validate_model_config"):
                        validate_toml(edited_content)
                
                # Configuration helpers
                st.markdown("#### Configuration Helpers")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if st.button("🚀 Enable Network Acceleration"):
                        add_network_acceleration_config()
                
                with col2:
                    if st.button("🛣️ Add Parallel Highway Assignment"):
                        add_parallel_highway_config()
                        
            except Exception as e:
                st.error(f"Error reading model configuration file: {e}")
        else:
            st.error(f"Model configuration file not found: {config_path}")
    else:
        st.warning("No model configuration path set. Please complete model setup first.")

def save_config(config_path, content):
    """Save configuration content to file."""
    try:
        # Validate TOML syntax first
        toml.loads(content)
        
        # Save to file
        with open(config_path, 'w') as f:
            f.write(content)
        
        st.success(f"✅ Configuration saved to {config_path}")
        st.session_state.config_modified = True
        
    except toml.TomlDecodeError as e:
        st.error(f"❌ TOML syntax error: {e}")
    except Exception as e:
        st.error(f"❌ Error saving file: {e}")

def validate_toml(content):
    """Validate TOML syntax."""
    try:
        parsed = toml.loads(content)
        st.success("✅ TOML syntax is valid!")
        
        # Show some basic validation info
        sections = list(parsed.keys())
        st.info(f"Found {len(sections)} main sections: {', '.join(sections)}")
        
    except toml.TomlDecodeError as e:
        st.error(f"❌ TOML syntax error: {e}")

def add_network_acceleration_config():
    """Add network acceleration configuration helper."""
    st.info("Network acceleration configuration example:")
    st.code("""
[highway]
network_acceleration = true
    """, language="toml")

def add_parallel_highway_config():
    """Add parallel highway assignment configuration helper."""
    st.info("Parallel highway assignment configuration example:")
    st.code("""
[[emme.highway_distribution]]
    time_periods = ["AM"]
    num_processors = "MAX/3"
    
[[emme.highway_distribution]]
    time_periods = ["PM"] 
    num_processors = "MAX/3"
    
[[emme.highway_distribution]]
    time_periods = ["EA", "MD", "EV"]
    num_processors = "MAX/3"
    """, language="toml")