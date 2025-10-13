"""
TM2PY GUI Launcher Script

Launch the Streamlit-based TM2PY GUI application.
"""

import sys
import subprocess
from pathlib import Path

def main():
    """Launch the TM2PY GUI application."""
    
    # Get the path to the GUI app
    gui_app_path = Path(__file__).parent.parent / "tm2py" / "gui" / "app.py"
    
    if not gui_app_path.exists():
        print(f"Error: GUI app not found at {gui_app_path}")
        return 1
    
    print("🚀 Launching TM2PY GUI...")
    print(f"📁 App location: {gui_app_path}")
    print("🌐 Opening in your default web browser...")
    print()
    print("Press Ctrl+C to stop the application")
    print("-" * 50)
    
    try:
        # Launch Streamlit app
        cmd = [
            sys.executable, 
            "-m", "streamlit", "run", 
            str(gui_app_path),
            "--server.port", "8501",
            "--server.headless", "false",
            "--browser.gatherUsageStats", "false"
        ]
        
        subprocess.run(cmd)
        
    except KeyboardInterrupt:
        print("\n👋 TM2PY GUI stopped by user")
        return 0
    except Exception as e:
        print(f"❌ Error launching GUI: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())