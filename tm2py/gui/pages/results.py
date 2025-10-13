"""
Results and Analysis Page for TM2PY GUI

Displays model run results, integrates with NetworkSummary component,
and provides visualization tools.
"""

import streamlit as st
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time

def show():
    """Display the Results & Analysis page."""
    
    st.markdown('<div class="section-header">📊 Results & Analysis</div>', unsafe_allow_html=True)
    
    if not st.session_state.model_run_dir:
        st.warning("No model run directory selected. Please complete model setup first.")
        return
    
    # Check for results
    results_available = check_for_results()
    
    if results_available:
        # Results overview
        show_results_overview()
        
        # NetworkSummary integration
        show_network_summary_section()
        
        # Custom analysis tools
        show_analysis_tools()
    else:
        show_no_results_message()

def check_for_results():
    """Check if model results are available."""
    
    run_dir = Path(st.session_state.model_run_dir)
    
    # Check for common result files
    result_files = [
        "outputs",
        "emme_project",
        "logs"
    ]
    
    available_results = []
    for result_file in result_files:
        result_path = run_dir / result_file
        if result_path.exists():
            available_results.append(result_file)
    
    return len(available_results) > 0

def show_results_overview():
    """Show overview of available results."""
    
    st.markdown("### Results Overview")
    
    run_dir = Path(st.session_state.model_run_dir)
    
    # Results summary cards
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        outputs_dir = run_dir / "outputs"
        if outputs_dir.exists():
            output_files = list(outputs_dir.glob("*"))
            st.metric("Output Files", len(output_files))
        else:
            st.metric("Output Files", 0)
    
    with col2:
        logs_dir = run_dir / "logs"
        if logs_dir.exists():
            log_files = list(logs_dir.glob("*.log"))
            st.metric("Log Files", len(log_files))
        else:
            st.metric("Log Files", 0)
    
    with col3:
        emme_dir = run_dir / "emme_project"
        if emme_dir.exists():
            st.metric("EMME Project", "✅")
        else:
            st.metric("EMME Project", "❌")
    
    with col4:
        # Check for NetworkSummary results
        network_summary_dir = run_dir / "outputs" / "network_summary"
        if network_summary_dir.exists():
            st.metric("Network Summary", "✅")
        else:
            st.metric("Network Summary", "❌")

def show_network_summary_section():
    """Show NetworkSummary component integration."""
    
    st.markdown("### Network Summary Analysis")
    
    run_dir = Path(st.session_state.model_run_dir)
    network_summary_dir = run_dir / "outputs" / "network_summary"
    
    if network_summary_dir.exists():
        # NetworkSummary results available
        st.success("✅ Network summary results found!")
        
        # Run NetworkSummary component
        col1, col2 = st.columns([1, 2])
        
        with col1:
            if st.button("🔄 Generate Network Summary", type="primary"):
                run_network_summary()
        
        with col2:
            st.info("Generate comprehensive network performance analysis")
        
        # Display existing results
        show_network_summary_results(network_summary_dir)
        
    else:
        # No NetworkSummary results yet
        st.warning("No network summary results found.")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            if st.button("🚀 Run Network Summary", type="primary"):
                run_network_summary()
        
        with col2:
            st.info("Run NetworkSummary component to analyze network performance")

def show_network_summary_results(summary_dir):
    """Display NetworkSummary results with visualizations."""
    
    st.markdown("#### Network Summary Results")
    
    # Look for result files
    excel_files = list(summary_dir.glob("*.xlsx"))
    csv_files = list(summary_dir.glob("*.csv"))
    
    if excel_files or csv_files:
        # File browser
        tab1, tab2, tab3 = st.tabs(["📊 Visualizations", "📄 Data Tables", "📁 Files"])
        
        with tab1:
            show_network_visualizations(summary_dir)
        
        with tab2:
            show_data_tables(summary_dir)
        
        with tab3:
            show_file_browser(summary_dir)
    else:
        st.info("No result files found in network summary directory.")

def show_network_visualizations(summary_dir):
    """Show network performance visualizations."""
    
    st.markdown("##### Network Performance Charts")
    
    # Try to load and visualize data
    try:
        # Look for overall summary CSV
        summary_csv = summary_dir / "overall_summary.csv"
        
        if summary_csv.exists():
            df = pd.read_csv(summary_csv)
            
            if not df.empty:
                # Volume/Capacity chart
                if 'time_period' in df.columns and 'avg_vc_ratio' in df.columns:
                    fig_vc = px.bar(
                        df, 
                        x='time_period', 
                        y='avg_vc_ratio',
                        title="Average Volume/Capacity Ratio by Time Period",
                        color='avg_vc_ratio',
                        color_continuous_scale='RdYlGn_r'
                    )
                    st.plotly_chart(fig_vc, use_container_width=True)
                
                # Speed chart
                if 'avg_speed_mph' in df.columns:
                    fig_speed = px.line(
                        df,
                        x='time_period',
                        y='avg_speed_mph',
                        title="Average Speed by Time Period",
                        markers=True
                    )
                    st.plotly_chart(fig_speed, use_container_width=True)
            else:
                st.info("No data available for visualization.")
        else:
            st.info("Summary CSV file not found. Run network summary analysis first.")
            
    except Exception as e:
        st.error(f"Error creating visualizations: {e}")

def show_data_tables(summary_dir):
    """Show data tables from NetworkSummary results."""
    
    st.markdown("##### Data Tables")
    
    # CSV file selector
    csv_files = list(summary_dir.glob("*.csv"))
    
    if csv_files:
        selected_file = st.selectbox(
            "Select data file:",
            csv_files,
            format_func=lambda x: x.name
        )
        
        if selected_file:
            try:
                df = pd.read_csv(selected_file)
                
                # Display data with filtering options
                st.markdown(f"**File:** {selected_file.name}")
                st.markdown(f"**Rows:** {len(df)}, **Columns:** {len(df.columns)}")
                
                # Column filter
                if len(df.columns) > 10:
                    selected_columns = st.multiselect(
                        "Select columns to display:",
                        df.columns.tolist(),
                        default=df.columns.tolist()[:10]
                    )
                    if selected_columns:
                        df = df[selected_columns]
                
                # Display dataframe
                st.dataframe(df, use_container_width=True)
                
                # Download button
                csv_data = df.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv_data,
                    f"{selected_file.stem}_filtered.csv",
                    "text/csv"
                )
                
            except Exception as e:
                st.error(f"Error reading file: {e}")
    else:
        st.info("No CSV files found.")

def show_file_browser(summary_dir):
    """Show file browser for NetworkSummary results."""
    
    st.markdown("##### Result Files")
    
    # List all files
    all_files = list(summary_dir.iterdir())
    
    if all_files:
        for file_path in sorted(all_files):
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                if file_path.is_file():
                    file_size = file_path.stat().st_size / (1024 * 1024)  # MB
                    st.text(f"📄 {file_path.name} ({file_size:.1f} MB)")
                else:
                    st.text(f"📁 {file_path.name}/")
            
            with col2:
                if file_path.is_file() and file_path.suffix in ['.xlsx', '.csv']:
                    if st.button("👁️ View", key=f"view_{file_path.name}"):
                        view_file(file_path)
            
            with col3:
                if file_path.is_file():
                    with open(file_path, 'rb') as f:
                        st.download_button(
                            "📥 Download",
                            f.read(),
                            file_path.name,
                            key=f"download_{file_path.name}"
                        )
    else:
        st.info("No files found in results directory.")

def show_analysis_tools():
    """Show custom analysis tools."""
    
    st.markdown("### Custom Analysis Tools")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Quick Actions")
        
        if st.button("📈 Generate Performance Report"):
            st.info("Performance report generation would be implemented here.")
        
        if st.button("🗺️ Create Network Maps"):
            st.info("Network mapping functionality would be implemented here.")
        
        if st.button("📊 Export to Dashboard"):
            st.info("Dashboard export functionality would be implemented here.")
    
    with col2:
        st.markdown("#### Analysis Options")
        
        analysis_type = st.selectbox(
            "Analysis Type:",
            ["Volume Analysis", "Speed Analysis", "V/C Ratio Analysis", "Transit Analysis"]
        )
        
        time_periods = st.multiselect(
            "Time Periods:",
            ["EA", "AM", "MD", "PM", "EV"],
            default=["AM", "PM"]
        )
        
        if st.button("🔍 Run Analysis"):
            run_custom_analysis(analysis_type, time_periods)

def show_no_results_message():
    """Show message when no results are available."""
    
    st.info("No model results found in the current run directory.")
    
    st.markdown("### What you can do:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Run Model")
        st.markdown("Go to the **Run Model** page to execute a TM2PY model run.")
        
    with col2:
        st.markdown("#### Select Different Directory")
        st.markdown("Go to **Model Setup** to select a different model run directory with existing results.")

def run_network_summary():
    """Run the NetworkSummary component."""
    
    try:
        from tm2py.config import Configuration
        from tm2py.controller import RunController
        from tm2py.components.network_summary import NetworkSummary
        
        with st.spinner("Running NetworkSummary analysis..."):
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
                return
                
            # Create controller
            config = Configuration.build_config(config_files)
            controller = RunController(config)
            controller.setup()
            
            # Create and run NetworkSummary component
            component = NetworkSummary(controller)
            
            with st.container():
                st.write("Starting network summary analysis...")
                progress_bar = st.progress(0)
                
                # Run component in thread to allow progress updates
                import threading
                result = {"success": False, "error": None}
                
                def run_component():
                    try:
                        component.run()
                        result["success"] = True
                    except Exception as e:
                        result["error"] = str(e)
                
                thread = threading.Thread(target=run_component)
                thread.start()
                
                # Update progress while running
                while thread.is_alive():
                    progress_bar.progress(50)  # Show halfway progress
                    time.sleep(1)
                    
                thread.join()
                progress_bar.progress(100)
                
                if result["success"]:
                    st.success("✅ NetworkSummary analysis completed!")
                    st.info("Check the outputs/network_summary directory for results.")
                else:
                    st.error(f"❌ NetworkSummary analysis failed: {result['error']}")
                    
    except Exception as e:
        st.error(f"Error running NetworkSummary: {e}")
        st.exception(e)

def view_file(file_path):
    """View file contents."""
    
    try:
        if file_path.suffix == '.csv':
            df = pd.read_csv(file_path)
            st.dataframe(df)
        elif file_path.suffix == '.xlsx':
            df = pd.read_excel(file_path)
            st.dataframe(df)
        else:
            st.info(f"File viewer not implemented for {file_path.suffix} files.")
    except Exception as e:
        st.error(f"Error viewing file: {e}")

def run_custom_analysis(analysis_type, time_periods):
    """Run custom analysis based on user selection."""
    
    st.info(f"Running {analysis_type} for time periods: {', '.join(time_periods)}")
    # Custom analysis implementation would go here