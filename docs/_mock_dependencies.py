"""Mock problematic dependencies for documentation generation."""

import sys
from unittest.mock import MagicMock

# Mock GDAL and related geospatial libraries
sys.modules["osgeo"] = MagicMock()
sys.modules["osgeo.gdal"] = MagicMock()
sys.modules["osgeo.ogr"] = MagicMock()
sys.modules["osgeo.osr"] = MagicMock()
sys.modules["gdal"] = MagicMock()

# Mock EMME libraries
sys.modules["inro"] = MagicMock()
sys.modules["inro.emme"] = MagicMock()
sys.modules["inro.emme.database"] = MagicMock()
sys.modules["inro.emme.database.emmebank"] = MagicMock()
sys.modules["inro.emme.database.scenario"] = MagicMock()
sys.modules["inro.emme.database.matrix"] = MagicMock()
sys.modules["inro.emme.network"] = MagicMock()
sys.modules["inro.emme.network.node"] = MagicMock()
sys.modules["inro.emme.desktop"] = MagicMock()
sys.modules["inro.emme.desktop.app"] = MagicMock()
sys.modules["inro.modeller"] = MagicMock()