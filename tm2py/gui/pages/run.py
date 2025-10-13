"""
Model Run Page for TM2PY GUI

Handles model execution with real-time progress monitoring and log display.
"""

import streamlit as st
import subprocess
import threading
import time
from pathlib import Path

def show():
    """Display the Model Run page."""
    
    st.markdown('<div class="section-header">▶️ Run Model</div>', unsafe_allow_html=True)
    
    if not st.session_state.model_setup_complete:
        st.warning("Please complete model setup first before running the model.")
        return
    
    # Component selection
    show_component_selection()
    
    # Model run controls
    show_run_controls()
    
    # Progress monitoring
    show_progress_section()
    
    # Log display
    show_logs_section()

def show_component_selection():
    """Display component selection interface."""
    
    st.markdown("### 🎛️ Component Selection")
    st.markdown("Select which TM2PY components to run:")
    
    # Define all available components with descriptions
    available_components = {
        "Network & Setup": {
            "create_tod_scenarios": {
                "name": "Create TOD Scenarios",
                "description": "Initialize time-of-day scenarios in EMME",
                "default": True
            },
            "prepare_network_highway": {
                "name": "Prepare Highway Network", 
                "description": "Set up highway network for assignment",
                "default": True
            },
            "prepare_network_transit": {
                "name": "Prepare Transit Network",
                "description": "Set up transit network for assignment", 
                "default": True
            }
        },
        "Demand Models": {
            "household": {
                "name": "Household Model",
                "description": "Generate household travel demand",
                "default": True
            },
            "truck": {
                "name": "Commercial Vehicle Model",
                "description": "Generate truck and commercial vehicle trips",
                "default": True
            },
            "air_passenger": {
                "name": "Air Passenger Model",
                "description": "Generate airport passenger trips",
                "default": False
            },
            "internal_external": {
                "name": "Internal-External Model", 
                "description": "Generate external trips at border crossings",
                "default": False
            }
        },
        "Network Assignment": {
            "highway": {
                "name": "Highway Assignment",
                "description": "Assign vehicle trips to highway network",
                "default": True
            },
            "transit_assign": {
                "name": "Transit Assignment",
                "description": "Assign transit trips to transit network", 
                "default": True
            },
            "highway_maz_assign": {
                "name": "Highway MAZ Assignment",
                "description": "Assign MAZ-level demand to highway network",
                "default": False
            }
        },
        "Skims & Access": {
            "highway_maz_skim": {
                "name": "Highway MAZ Skims",
                "description": "Generate MAZ-to-MAZ highway skims",
                "default": False
            },
            "transit_skim": {
                "name": "Transit Skims", 
                "description": "Generate transit level-of-service skims",
                "default": True
            },
            "drive_access_skims": {
                "name": "Drive Access Skims",
                "description": "Generate drive-to-transit access skims",
                "default": False
            },
            "active_modes": {
                "name": "Active Mode Skims",
                "description": "Generate walk and bike skims",
                "default": False
            }
        },
        "Analysis & Output": {
            "post_processor": {
                "name": "Post Processor",
                "description": "Generate standard model outputs and summaries",
                "default": True
            },
            "network_summary": {
                "name": "Network Summary",
                "description": "Detailed network performance analysis",
                "default": False
            }
        }
    }
    
    # Initialize component selections in session state
    if 'selected_components' not in st.session_state:
        st.session_state.selected_components = {}
        for category, components in available_components.items():
            for comp_id, comp_info in components.items():
                st.session_state.selected_components[comp_id] = comp_info["default"]
    
    # Quick selection buttons
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        if st.button("✅ Select All"):
            for category, components in available_components.items():
                for comp_id in components.keys():
                    st.session_state.selected_components[comp_id] = True
            st.rerun()
    
    with col2:
        if st.button("❌ Clear All"):
            for comp_id in st.session_state.selected_components.keys():
                st.session_state.selected_components[comp_id] = False
            st.rerun()
            
    with col3:
        if st.button("🎯 Standard Run"):
            for category, components in available_components.items():
                for comp_id, comp_info in components.items():
                    st.session_state.selected_components[comp_id] = comp_info["default"]
            st.rerun()
            
    with col4:
        if st.button("📊 Analysis Only"):
            for comp_id in st.session_state.selected_components.keys():
                st.session_state.selected_components[comp_id] = False
            # Select only analysis components
            st.session_state.selected_components["post_processor"] = True
            st.session_state.selected_components["network_summary"] = True
            st.rerun()
    
    st.markdown("---")
    
    # Component selection by category
    for category, components in available_components.items():
        with st.expander(f"📁 {category}", expanded=True):
            for comp_id, comp_info in components.items():
                col1, col2 = st.columns([1, 3])
                with col1:
                    st.session_state.selected_components[comp_id] = st.checkbox(
                        comp_info["name"],
                        value=st.session_state.selected_components.get(comp_id, comp_info["default"]),
                        key=f"checkbox_{comp_id}"
                    )
                with col2:
                    st.caption(comp_info["description"])
    
    # Show selected components summary
    selected_count = sum(st.session_state.selected_components.values())
    total_count = len(st.session_state.selected_components)
    
    if selected_count > 0:
        st.success(f"✅ {selected_count} of {total_count} components selected")
        selected_names = [
            available_components[cat][comp_id]["name"] 
            for cat, components in available_components.items()
            for comp_id in components.keys()
            if st.session_state.selected_components.get(comp_id, False)
        ]
        st.info(f"Selected: {', '.join(selected_names)}")
    else:
        st.warning("⚠️ No components selected - model will not run")

def show_run_controls():
    """Display model run control buttons."""
    
    st.markdown("---")
    st.markdown("### ▶️ Model Execution")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    selected_count = sum(st.session_state.get('selected_components', {}).values())
    
    with col1:
        if st.button(
            "🚀 Run Selected Components", 
            type="primary", 
            disabled=st.session_state.model_running or selected_count == 0,
            help="Run the selected TM2PY components"
        ):
            start_custom_model_run()
    
    with col2:
        if st.button(
            "⏹️ Stop Model", 
            disabled=not st.session_state.model_running,
            help="Stop the running model"
        ):
            stop_model_run()
    
    with col3:
        # Run directory info
        if st.session_state.model_run_dir:
            st.info(f"Run Directory: {st.session_state.model_run_dir}")
    
    # Model status
    if st.session_state.model_running:
        st.success("🟢 Model is running...")
    else:
        st.info("⚪ Model ready to run")

def show_network_summary_controls():
    """Display NetworkSummary standalone execution controls."""
    
    st.markdown("---")
    st.markdown("### NetworkSummary Analysis")
    st.markdown("Run network performance analysis independently of the full model.")
    
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button(
            "📊 Run NetworkSummary",
            disabled=st.session_state.model_running,
            help="Run NetworkSummary component for network analysis"
        ):
            run_network_summary_standalone()
    
    with col2:
        # Check if previous results exist
        if st.session_state.model_run_dir:
            output_dir = Path(st.session_state.model_run_dir) / "outputs" / "network_summary"
            if output_dir.exists():
                st.success("✅ Results available")
            else:
                st.info("⚪ No results yet")
    
    with col3:
        if st.session_state.model_run_dir:
            st.info(f"Output: {Path(st.session_state.model_run_dir) / 'outputs' / 'network_summary'}")
    
    # Info about NetworkSummary
    with st.expander("ℹ️ About NetworkSummary Analysis"):
        st.markdown("""
        **NetworkSummary** analyzes network performance including:
        - Highway link volumes and speeds
        - Transit ridership and performance
        - Network-wide performance metrics
        - Data validation and quality checks
        
        This analysis can be run independently after model execution to generate 
        detailed reports and visualizations of network performance.
        """)

def show_progress_section():
    """Display model run progress."""
    
    st.markdown("### Progress")
    
    # Progress bar
    progress_bar = st.progress(st.session_state.model_run_progress / 100)
    
    # Progress details
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Progress", f"{st.session_state.model_run_progress}%")
    
    with col2:
        if st.session_state.model_running:
            st.metric("Status", "Running")
        else:
            st.metric("Status", "Idle")
    
    with col3:
        # Estimated time remaining (placeholder)
        st.metric("ETA", "-- min")
    
    # Current step indicator
    if st.session_state.model_running:
        current_step = get_current_step()
        if current_step:
            st.info(f"Current Step: {current_step}")

def show_logs_section():
    """Display real-time model logs."""
    
    st.markdown("### Model Logs")
    
    # Log controls
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("🔄 Refresh Logs"):
            st.rerun()
    
    with col2:
        if st.button("🗑️ Clear Logs"):
            st.session_state.model_run_logs = []
            st.rerun()
    
    with col3:
        auto_refresh = st.checkbox("Auto-refresh", value=True)
    
    # Log display
    log_container = st.container()
    
    with log_container:
        if st.session_state.model_run_logs:
            # Display logs in a scrollable text area
            log_text = "\n".join(st.session_state.model_run_logs)
            st.text_area(
                "Log Output:",
                value=log_text,
                height=400,
                disabled=True
            )
        else:
            st.info("No logs available. Start a model run to see output.")
    
    # Auto-refresh functionality
    if auto_refresh and st.session_state.model_running:
        # Refresh every 5 seconds when model is running
        time.sleep(5)
        st.rerun()

def start_model_run():
    """Start the TM2PY model run."""
    
    try:
        # Update session state
        st.session_state.model_running = True
        st.session_state.model_run_progress = 0
        st.session_state.model_run_logs = []
        
        # Add initial log entry
        add_log("🚀 Starting TM2PY model run...")
        add_log(f"📁 Run directory: {st.session_state.model_run_dir}")
        
        # In a real implementation, this would start the actual model run
        # For now, we'll simulate the process
        simulate_model_run()
        
        st.success("✅ Model run started!")
        
    except Exception as e:
        st.error(f"❌ Error starting model run: {e}")
        st.session_state.model_running = False

def stop_model_run():
    """Stop the running model."""
    
    st.session_state.model_running = False
    add_log("⏹️ Model run stopped by user")
    st.warning("Model run stopped")

def simulate_model_run():
    """Simulate a model run for demonstration purposes."""
    
    def run_simulation():
        steps = [
            "Initializing model components...",
            "Loading highway network...",
            "Loading transit network...", 
            "Running highway assignment (AM)...",
            "Running highway assignment (PM)...",
            "Running transit assignment...",
            "Generating outputs...",
            "Model run completed!"
        ]
        
        for i, step in enumerate(steps):
            if not st.session_state.model_running:
                break
                
            add_log(f"📋 {step}")
            st.session_state.model_run_progress = int((i + 1) / len(steps) * 100)
            time.sleep(3)  # Simulate work
        
        st.session_state.model_running = False
        
        if st.session_state.model_run_progress >= 100:
            add_log("✅ Model run completed successfully!")
    
    # Start simulation in a separate thread
    thread = threading.Thread(target=run_simulation)
    thread.daemon = True
    thread.start()

def get_current_step():
    """Get the current model execution step."""
    
    if not st.session_state.model_run_logs:
        return None
    
    # Get the last log entry that looks like a step
    for log in reversed(st.session_state.model_run_logs):
        if "📋" in log:
            return log.replace("📋 ", "")
    
    return None

def add_log(message):
    """Add a log message to the session state."""
    
    timestamp = time.strftime("%H:%M:%S")
    log_entry = f"[{timestamp}] {message}"
    
    if len(st.session_state.model_run_logs) > 1000:  # Keep last 1000 logs
        st.session_state.model_run_logs = st.session_state.model_run_logs[-900:]
    
    st.session_state.model_run_logs.append(log_entry)

def run_network_summary_standalone():
    """Run NetworkSummary component independently."""
    
    try:
        from tm2py.config import Configuration
        from tm2py.controller import RunController
        from tm2py.components.network_summary import NetworkSummary
        
        add_log("🔍 Starting NetworkSummary analysis...")
        
        # Load configuration
        model_dir = Path(st.session_state.model_directory)
        config_files = []
        
        # Add scenario config if it exists
        scenario_config = model_dir / "scenario_config.toml"
        if scenario_config.exists():
            config_files.append(str(scenario_config))
            
        # Add model config if it exists
        model_config = model_dir / "model_config.toml"
        if model_config.exists():
            config_files.append(str(model_config))
            
        if not config_files:
            st.error("No configuration files found in model directory")
            add_log("❌ No configuration files found")
            return
            
        add_log(f"📁 Loading configuration from: {', '.join(config_files)}")
        
        # Create controller
        config = Configuration.build_config(config_files)
        controller = RunController(config)
        controller.setup()
        
        add_log("🏗️ Controller setup complete")
        
        # Create and run NetworkSummary component
        component = NetworkSummary(controller)
        add_log("📊 Running NetworkSummary component...")
        
        # Run in a separate thread to avoid blocking the UI
        def run_component():
            try:
                component.run()
                add_log("✅ NetworkSummary analysis completed successfully!")
                st.success("NetworkSummary analysis completed!")
                # Force a rerun to show the success message
                st.rerun()
            except Exception as e:
                add_log(f"❌ NetworkSummary analysis failed: {str(e)}")
                st.error(f"NetworkSummary analysis failed: {str(e)}")
                
        import threading
        thread = threading.Thread(target=run_component, daemon=True)
        thread.start()
        
        st.info("🚀 NetworkSummary analysis started! Check the logs for progress.")
        
    except Exception as e:
        error_msg = f"Error starting NetworkSummary: {str(e)}"
        st.error(error_msg)
        add_log(f"❌ {error_msg}")