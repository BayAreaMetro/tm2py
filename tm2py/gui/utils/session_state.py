"""
Session State Management for TM2PY GUI

Handles Streamlit session state initialization and management
for persistent data across page navigation.
"""

import streamlit as st
from pathlib import Path
import os

def initialize_session_state():
    """Initialize session state variables for the TM2PY GUI."""
    
    # Environment and paths
    if "venv_path" not in st.session_state:
        st.session_state.venv_path = detect_virtual_env()
    
    if "venv_active" not in st.session_state:
        st.session_state.venv_active = check_venv_active()
    
    if "setup_config_path" not in st.session_state:
        st.session_state.setup_config_path = ""
    
    if "model_run_dir" not in st.session_state:
        st.session_state.model_run_dir = ""
    
    if "scenario_config_path" not in st.session_state:
        st.session_state.scenario_config_path = ""
    
    if "model_config_path" not in st.session_state:
        st.session_state.model_config_path = ""
    
    # Model execution state
    if "model_setup_complete" not in st.session_state:
        st.session_state.model_setup_complete = False
    
    if "model_running" not in st.session_state:
        st.session_state.model_running = False
    
    if "model_run_logs" not in st.session_state:
        st.session_state.model_run_logs = []
    
    if "model_run_progress" not in st.session_state:
        st.session_state.model_run_progress = 0
    
    # Configuration editing
    if "config_modified" not in st.session_state:
        st.session_state.config_modified = False
    
    # Results and analysis
    if "last_run_results" not in st.session_state:
        st.session_state.last_run_results = None

def detect_virtual_env():
    """Detect available virtual environments."""
    possible_venv_paths = []
    
    # Check common virtual environment locations
    venv_names = ["tm2py_env", "tm2pyenv", "venv", ".venv"]
    
    # Check in current directory and parent directories
    current_dir = Path.cwd()
    for parent in [current_dir] + list(current_dir.parents)[:3]:  # Check up to 3 levels up
        for venv_name in venv_names:
            venv_path = parent / venv_name
            if venv_path.exists() and (venv_path / "Scripts" / "python.exe").exists():
                possible_venv_paths.append(str(venv_path))
    
    # Check VIRTUAL_ENV environment variable
    if "VIRTUAL_ENV" in os.environ:
        venv_path = Path(os.environ["VIRTUAL_ENV"])
        if venv_path.exists():
            possible_venv_paths.insert(0, str(venv_path))  # Prioritize active venv
    
    return possible_venv_paths[0] if possible_venv_paths else ""

def check_venv_active():
    """Check if a virtual environment is currently active."""
    return "VIRTUAL_ENV" in os.environ

def reset_session_state():
    """Reset session state to initial values."""
    keys_to_reset = [
        "model_setup_complete", "model_running", "model_run_logs", 
        "model_run_progress", "config_modified", "last_run_results"
    ]
    
    for key in keys_to_reset:
        if key in st.session_state:
            del st.session_state[key]
    
    initialize_session_state()

def update_session_state(**kwargs):
    """Update multiple session state variables."""
    for key, value in kwargs.items():
        st.session_state[key] = value