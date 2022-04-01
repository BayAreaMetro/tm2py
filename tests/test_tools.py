from unittest.mock import MagicMock
import sys
import os
import pytest

@pytest.mark.menow
def test_download():

    sys.modules['inro.emme.database.emmebank'] = MagicMock()
    sys.modules['inro.emme.network']=MagicMock()
    sys.modules['inro.emme.database.scenario']=MagicMock()
    sys.modules['inro.emme.database.matrix']=MagicMock()
    sys.modules['inro.emme.network.node']=MagicMock()
    sys.modules['inro.emme.desktop.app']=MagicMock()
    sys.modules['inro']=MagicMock()
    sys.modules['inro.modeller']=MagicMock()
    #tm2py.emme.network.EmmeNetwork = Mock()
    #EmmeNetwork.links = MagicMock(return_value=[])

    from tm2py.tools import _download
    _EXAMPLE_URL = r"https://mtcdrive.box.com/s/3entr016e9teq2wt46x1os3fjqylfoge"
    import tempfile
    temp_file = os.path.join(tempfile.gettempdir(), "test_download.zip")
    downloaded_file = _download(_EXAMPLE_URL,temp_file)
    file_size = os.path.getsize(downloaded_file)
    print(f"Downloaded file size: {file_size}")
    print(f"Downloaded file size: {file_size}")
    assert file_size>0


@pytest.mark.menow
def test_download_unzip():
    pass
    ##TODO KEVIN