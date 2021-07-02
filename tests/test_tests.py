"""Test basic test operation.
"""

import pytest


@pytest.mark.skipci
def test_skipci():
    """Shouldn't be run on CI server."""
    print("If this is a CI server, the marker isn't working!!!")


def test_testing():
    """Tests that tests are run."""
    print("Tests are being run!")
    # Skip non-top level import and unused import warnings
    # pylint: disable=C0415
    # pylint: disable=W0611
    import tm2py
