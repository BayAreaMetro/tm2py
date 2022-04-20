import pytest


@pytest.mark.skipci
def test_skipci():
    """Shouldn't be run on CI server."""
    print("If this is a CI server, the marker isn't working!!!")


def test_testing():
    """Tests that tests are run."""
    print("Tests are being run!")
