"""
Model Setup Page for TM2PY GUI

Handles virtual environment activation, file selection,
and model setup operations.
"""

import streamlit as st
import subprocess
from pathlib import Path
import os
import sys

def show():
    """Display the Model Setup page."""
    
    st.markdown('<div class="section-header">🔧 Model Setup</div>', unsafe_allow_html=True)
    
    # Step 1: Virtual Environment Management
    show_environment_section()
    
    # Step 2: Configuration File Selection
    show_config_section()
    
    # Step 3: Model Setup Execution
    show_setup_section()

def show_environment_section():
    """Display virtual environment management section."""
    
    st.markdown("### 1. Virtual Environment")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Virtual environment selection
        venv_options = get_available_venvs()
        
        if venv_options:
            selected_venv = st.selectbox(
                "Select Virtual Environment:",
                venv_options,
                index=0 if st.session_state.venv_path in venv_options else 0
            )
            st.session_state.venv_path = selected_venv
        else:
            st.warning("No virtual environments detected. Please create a tm2py virtual environment first.")
            return
    
    with col2:
        # Environment status
        if st.session_state.venv_active:
            st.success("✅ Active")
        else:
            if st.button("Activate", key="activate_venv"):
                activate_virtual_env(st.session_state.venv_path)
    
    # Show current environment info
    if st.session_state.venv_path:
        with st.expander("Environment Details"):
            st.text(f"Path: {st.session_state.venv_path}")
            st.text(f"Python: {get_python_path(st.session_state.venv_path)}")
            
            # Check if tm2py is installed
            if check_tm2py_installed(st.session_state.venv_path):
                st.success("✅ TM2PY is installed")
            else:
                st.error("❌ TM2PY not found in this environment")

def show_config_section():
    """Display configuration file selection section."""
    
    st.markdown("### 2. Configuration Files")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Setup Configuration:**")
        
        # File browser for setup config
        setup_config_path = st.text_input(
            "Setup Config Path:",
            value=st.session_state.setup_config_path,
            placeholder="Select setup_config.toml file"
        )
        
        if st.button("Browse Setup Config", key="browse_setup"):
            # In a real implementation, this would open a file dialog
            st.info("File browser would open here. For now, please enter the path manually.")
        
        # Validate setup config file
        if setup_config_path:
            if Path(setup_config_path).exists():
                st.success("✅ File found")
                st.session_state.setup_config_path = setup_config_path
            else:
                st.error("❌ File not found")
    
    with col2:
        st.markdown("**Model Run Directory:**")
        
        # Directory selection for model run
        model_run_dir = st.text_input(
            "Model Run Directory:",
            value=st.session_state.model_run_dir,
            placeholder="Select target directory for model run"
        )
        
        if st.button("Browse Directory", key="browse_dir"):
            st.info("Directory browser would open here. For now, please enter the path manually.")
        
        # Validate and create directory if needed
        if model_run_dir:
            model_path = Path(model_run_dir)
            if model_path.parent.exists():
                st.success("✅ Valid path")
                st.session_state.model_run_dir = model_run_dir
                
                if not model_path.exists():
                    if st.button("Create Directory", key="create_dir"):
                        try:
                            model_path.mkdir(parents=True, exist_ok=True)
                            st.success(f"Created directory: {model_path}")
                        except Exception as e:
                            st.error(f"Failed to create directory: {e}")
            else:
                st.error("❌ Invalid path")

def show_setup_section():
    """Display model setup execution section."""
    
    st.markdown("### 3. Run Model Setup")
    
    # Check if we can run setup
    can_run_setup = (
        st.session_state.venv_active and 
        st.session_state.setup_config_path and 
        st.session_state.model_run_dir
    )
    
    if can_run_setup:
        col1, col2 = st.columns([1, 3])
        
        with col1:
            if st.button("🚀 Setup Model", type="primary", disabled=st.session_state.model_running):
                run_model_setup()
        
        with col2:
            st.info("This will run: `python scripts/setup_model.py <config> <run_dir>`")
    else:
        st.warning("Please complete the virtual environment and file selection first.")
        
        # Show what's missing
        missing_items = []
        if not st.session_state.venv_active:
            missing_items.append("Virtual environment not active")
        if not st.session_state.setup_config_path:
            missing_items.append("Setup configuration file not selected")
        if not st.session_state.model_run_dir:
            missing_items.append("Model run directory not selected")
        
        for item in missing_items:
            st.error(f"❌ {item}")
    
    # Show setup status
    if st.session_state.model_setup_complete:
        st.success("✅ Model setup completed successfully!")
        st.info(f"Model ready to run in: {st.session_state.model_run_dir}")

# Utility functions
def get_available_venvs():
    """Get list of available virtual environments."""
    venv_paths = []
    
    # Add the one from session state if it exists
    if st.session_state.venv_path and Path(st.session_state.venv_path).exists():
        venv_paths.append(st.session_state.venv_path)
    
    # Add some common locations (this would be expanded in a real implementation)
    common_paths = [
        "C:/Users/{}/tm2py_env".format(os.getenv("USERNAME", "")),
        "E:/GitHub/tm2/tm2py_env",
        "./tm2py_env",
        "../tm2py_env"
    ]
    
    for path in common_paths:
        if Path(path).exists() and path not in venv_paths:
            venv_paths.append(path)
    
    return venv_paths

def get_python_path(venv_path):
    """Get Python executable path for virtual environment."""
    python_exe = Path(venv_path) / "Scripts" / "python.exe"
    return str(python_exe) if python_exe.exists() else "Not found"

def check_tm2py_installed(venv_path):
    """Check if tm2py is installed in the virtual environment."""
    try:
        python_exe = Path(venv_path) / "Scripts" / "python.exe"
        if not python_exe.exists():
            return False
        
        result = subprocess.run(
            [str(python_exe), "-c", "import tm2py; print(tm2py.__version__)"],
            capture_output=True,
            text=True,
            timeout=10
        )
        return result.returncode == 0
    except:
        return False

def activate_virtual_env(venv_path):
    """Activate virtual environment (simulated for Streamlit)."""
    # In a Streamlit app, we can't actually activate a venv in the traditional sense
    # But we can set environment variables and update paths
    
    venv_path_obj = Path(venv_path)
    if venv_path_obj.exists():
        # Set environment variables
        os.environ["VIRTUAL_ENV"] = str(venv_path_obj)
        os.environ["PATH"] = str(venv_path_obj / "Scripts") + ";" + os.environ.get("PATH", "")
        
        # Update session state
        st.session_state.venv_active = True
        st.success(f"Virtual environment activated: {venv_path}")
    else:
        st.error(f"Virtual environment not found: {venv_path}")

def run_model_setup():
    """Run the TM2PY model setup process."""
    
    # This would be implemented to actually run the setup_model.py script
    # For now, we'll simulate the process
    
    with st.spinner("Running model setup..."):
        # Simulate setup process
        import time
        time.sleep(2)  # Simulate work
        
        # In a real implementation, this would run:
        # subprocess.run([python_exe, "scripts/setup_model.py", config_path, run_dir])
        
        st.session_state.model_setup_complete = True
        st.success("Model setup completed!")
        
        # Update scenario config path for next steps
        scenario_config = Path(st.session_state.model_run_dir) / "scenario_config.toml"
        if scenario_config.exists():
            st.session_state.scenario_config_path = str(scenario_config)
        
        model_config = Path(st.session_state.model_run_dir) / "model_config.toml"
        if model_config.exists():
            st.session_state.model_config_path = str(model_config)