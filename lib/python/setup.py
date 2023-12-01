#  Copyright 2023 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import glob
import os
import sys
from setuptools import setup
from typing import Union

HERE = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(HERE)
VERSION_FILE = os.path.join(HERE, "pathling", "_version.py")

#
# Read __version__ from file
#
__version__: Union[str, None] = None
if os.path.isfile(VERSION_FILE):
    with open(VERSION_FILE) as vf:
        exec(vf.read())
if not __version__:
    print(
        "ERROR: Cannot read __version__ from file '%s'." % VERSION_FILE, file=sys.stderr
    )
    exit(1)

#
# Read the long description
#
with open("README.md") as f:
    long_description = f.read()

setup(
    name="pathling",
    packages=["pathling"],
    version=__version__,
    description="Python API for Pathling",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Australian e-Health Research Centre, CSIRO",
    author_email="ontoserver-support@csiro.au",
    url="https://github.com/aehrc/pathling",
    keywords=["pathling", "fhir", "analytics", "spark", "standards", "terminology"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    license="Apache License, version 2.0",
    python_requires=">=3.7",
    install_requires=["pyspark>=3.4.0,<3.5.0", "deprecated>=1.2.13"],
    include_package_data=True,
    data_files=[
        ("share/pathling/examples", glob.glob("examples/*.py")),
        (
            "share/pathling/examples/data/resources",
            glob.glob("examples/data/resources/*.ndjson"),
        ),
        (
            "share/pathling/examples/data/bundles",
            glob.glob("examples/data/bundles/*.json"),
        ),
    ],
)
