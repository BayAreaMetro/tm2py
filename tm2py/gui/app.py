"""
TM2PY Streamlit GUI Application

A modern, web-based interface for TM2PY model operations including:
- Model setup and configuration
- Real-time model execution monitoring
- Results visualization and analysis
- NetworkSummary integration
"""

import streamlit as st
import sys
from pathlib import Path

# Add the tm2py package to the path for imports
tm2py_root = Path(__file__).parent.parent.parent
if str(tm2py_root) not in sys.path:
    sys.path.insert(0, str(tm2py_root))

# Import pages
try:
    from .pages import setup, config, run, results
    from .utils.session_state import initialize_session_state
except ImportError as e:
    st.error(f"Import error: {e}")
    st.stop()

def main():
    """Main Streamlit application entry point."""
    
    # Configure the Streamlit page
    st.set_page_config(
        page_title="TM2PY Model Runner",
        page_icon="🚀",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state
    initialize_session_state()
    
    # Custom CSS for better styling
    st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #2c3e50;
        margin-top: 1rem;
        margin-bottom: 1rem;
    }
    .status-box {
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .status-success {
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .status-warning {
        background-color: #fff3cd;
        border: 1px solid #ffeaa7;
        color: #856404;
    }
    .status-error {
        background-color: #f8d7da;
        border: 1px solid #f5c6cb;
        color: #721c24;
    }
    </style>
    """, unsafe_allow_html=True)
    
    # Main header
    st.markdown('<div class="main-header">🚀 TM2PY Model Runner</div>', unsafe_allow_html=True)
    
    # Sidebar navigation
    st.sidebar.title("Navigation")
    st.sidebar.markdown("---")
    
    # Navigation options
    pages = {
        "🔧 Model Setup": "setup",
        "⚙️ Configuration": "config", 
        "▶️ Run Model": "run",
        "📊 Results & Analysis": "results"
    }
    
    # Page selection
    selected_page = st.sidebar.selectbox(
        "Choose a page:",
        list(pages.keys()),
        index=0
    )
    
    # Page routing
    page_key = pages[selected_page]
    
    try:
        if page_key == "setup":
            setup.show()
        elif page_key == "config":
            config.show()
        elif page_key == "run":
            run.show()
        elif page_key == "results":
            results.show()
    except Exception as e:
        st.error(f"Error loading page '{selected_page}': {e}")
        st.exception(e)
    
    # Sidebar info
    st.sidebar.markdown("---")
    st.sidebar.markdown("### About")
    st.sidebar.info(
        "TM2PY GUI provides an intuitive interface for transportation model operations. "
        "Navigate through the pages above to set up, configure, and run TM2PY models."
    )
    
    # Session state debug (only in development)
    if st.sidebar.checkbox("Show Session State (Debug)"):
        st.sidebar.json(dict(st.session_state))

if __name__ == "__main__":
    main()