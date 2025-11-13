# Mock dependencies for documentation generation
"""
This file provides mock implementations of external dependencies
that are not available during documentation build.
"""

class MockToml:
    @staticmethod
    def load(file):
        return {}
    
    @staticmethod
    def loads(string):
        return {}

class MockInro:
    pass

# Mock the modules
import sys
sys.modules['toml'] = MockToml()
sys.modules['inro'] = MockInro()
sys.modules['inro.emme'] = MockInro()
sys.modules['inro.emme.database'] = MockInro()
sys.modules['inro.modeller'] = MockInro()