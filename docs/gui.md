# TM2PY GUI - Streamlit Interface

A modern, web-based graphical user interface for TM2PY model operations.

## Overview

The TM2PY GUI provides an intuitive interface for:

- **Model Setup**: Virtual environment management, configuration file selection, and model initialization
- **Configuration Editing**: Interactive TOML editing for scenario and model configurations  
- **Model Execution**: Real-time model run monitoring with progress tracking and log display
- **Results Analysis**: Integration with NetworkSummary component and data visualization

## Usage Modes

The TM2PY system supports both **manual** and **GUI-based** operation:

### Manual Usage (Command Line)
1. **Full Model Runs**: Add components to `scenario_config.toml` and run `python RunModel.py`
2. **Component Integration**: NetworkSummary is automatically included if configured in `final_components`

```toml
# In scenario_config.toml
[final_components] 
network_summary = {}
```

### GUI Usage (Web Interface)
1. **Full Model Runs**: Use the "🚀 Run Model" button for complete TM2PY execution
2. **Standalone NetworkSummary**: Use "📊 Run NetworkSummary" for independent network analysis

Both modes use the same underlying TM2PY infrastructure and produce identical results.

## Installation

### Install GUI Dependencies

```bash
# Activate your tm2py virtual environment first
pip install -r tm2py/gui/requirements-gui.txt
```

### Launch the GUI

```bash
# From the tm2py root directory
python scripts/tm2py_gui.py
```

The GUI will open in your default web browser at `http://localhost:8501`

## Features

### 🔧 Model Setup Page

**Environment Management:**
- Auto-detect virtual environments
- Virtual environment activation
- TM2PY installation verification

**File Selection:**
- Setup configuration file browser
- Model run directory selection  
- Directory creation and validation

**Model Setup Execution:**
- One-click model setup process
- Integration with `setup_model.py`

### ⚙️ Configuration Page

**Scenario Configuration:**
- Interactive TOML editor for `scenario_config.toml`
- Real-time syntax validation
- Configuration preview and parsing

**Model Configuration:**
- Edit `model_config.toml` with helper tools
- Network acceleration settings
- Parallel highway assignment configuration

### ▶️ Run Model Page

**Full Model Execution:**
- Start/stop complete TM2PY model runs
- Real-time progress monitoring
- Current step indication

**NetworkSummary Standalone:**
- Run NetworkSummary component independently
- Analyze existing model results
- Generate network performance reports

**Live Monitoring:**
- Progress bar and metrics
- Real-time log streaming  
- Auto-refresh functionality

### 📊 Results & Analysis Page

**Results Overview:**
- Output file counting
- EMME project validation
- NetworkSummary status checking

**NetworkSummary Integration:**
- Launch NetworkSummary analysis
- Visualize network performance data
- Interactive charts and data tables

**Data Tools:**
- File browser for results
- CSV/Excel data viewing
- Export and download functionality

## Architecture

```
tm2py/gui/
├── app.py              # Main Streamlit application
├── pages/              # Individual page modules
│   ├── setup.py        # Model setup page
│   ├── config.py       # Configuration editing page
│   ├── run.py          # Model execution page
│   └── results.py      # Results and analysis page
├── components/         # Reusable UI components
├── utils/              # Utility functions
│   └── session_state.py  # Session state management
└── requirements-gui.txt   # GUI-specific dependencies
```

## Usage Workflow

1. **Setup**: Select virtual environment, configuration files, and run directory
2. **Configure**: Edit scenario and model configurations as needed
3. **Run**: Execute the model with real-time monitoring
4. **Analyze**: View results and run NetworkSummary analysis

## Session State Management

The GUI maintains persistent state across page navigation:

- Virtual environment paths and status
- Configuration file selections
- Model run progress and logs
- Results availability

## Integration with TM2PY

The GUI directly integrates with TM2PY components:

- Uses `tm2py.SetupModel` for model initialization
- Executes `RunModel.py` for model runs
- Integrates `NetworkSummary` component for analysis
- Reads and writes standard TM2PY configuration files

## Development

### Adding New Pages

1. Create a new module in `tm2py/gui/pages/`
2. Implement a `show()` function for the page content
3. Add the page to the navigation in `app.py`

### Adding Components

1. Create reusable components in `tm2py/gui/components/`
2. Import and use in page modules

### Extending Functionality

The modular design allows easy extension:

- Add new analysis tools to the results page
- Implement additional configuration helpers
- Integrate new TM2PY components
- Add custom visualizations

## Troubleshooting

### Common Issues

**Streamlit Import Error:**
```bash
pip install streamlit>=1.28.0
```

**Port Already in Use:**
```bash
streamlit run tm2py/gui/app.py --server.port 8502
```

**Virtual Environment Not Detected:**
- Ensure the virtual environment has `python.exe` in `Scripts/` directory
- Check that tm2py is installed in the environment

### Debug Mode

Enable session state debugging in the sidebar to inspect application state.

## Future Enhancements

- File upload/download via web interface
- Remote model execution capabilities
- Advanced data visualization with maps
- Configuration templates and presets
- Multi-user session management