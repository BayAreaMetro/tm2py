"""Installation script for tm2py package."""

import os

from setuptools import setup

version = "0.0.1"

classifiers = [
    "Development Status :: 1 - Planning",
    "License :: OSI Approved :: Apache Software License",
    "Natural Language :: English",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.11",
]

with open("README.md") as f:
    long_description = f.read()

with open("requirements.txt") as f:
    requirements = f.readlines()
install_requires = [r.strip() for r in requirements]

with open("dev-requirements.txt") as f:
    dev_requirements = f.readlines()
install_requires_dev = [r.strip() for r in dev_requirements]

if os.path.exists(os.path.join("docs", "requirements.txt")):
    with open(os.path.join("docs", "requirements.txt")) as f:
        doc_requirements = f.readlines()
    install_requires_doc = [r.strip() for r in doc_requirements]
else:
    install_requires_doc = []

# While version is in active development, install both development and base requirements.
major_version_number = int(version.split(".")[0])

setup(
    name="tm2py",
    version=version,
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BayAreaMetro/tm2py",
    license="Apache 2",
    platforms="any",
    packages=["tm2py"],
    include_package_data=True,
    install_requires=install_requires,
    extras_require={
        "dev": install_requires_dev,
        "doc": install_requires_doc,
    },
    scripts=["bin/tm2py", "bin/get_test_data"],
)
