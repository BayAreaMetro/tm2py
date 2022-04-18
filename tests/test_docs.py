import pytest


def test_docs_build():
    """
    Test that the documentation build is successful.
    """
    import subprocess
    import os

    # Get the path to the base directory
    base_dir = os.path.join(os.path.dirname(__file__), "..")
    assert os.path.exists("mkdocs.yml")

    # Build the docs
    try:
        subprocess.run(["mkdocs", "build"], check=True, cwd=base_dir,capture_output=True)
    except subprocess.CalledProcessError as e:
        msg = e.stderr.decode('utf-8')
        pytest.fail(f"Documentation Failed to Build.\n {msg}")

    # Check that the docs were built successfully
    assert os.path.exists(os.path.join(base_dir, "site", "index.html"))