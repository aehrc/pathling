import os
import sys

from setuptools import setup, find_packages

HERE = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(HERE)
VERSION_FILE = os.path.join(HERE, 'pathling', '_version.py')

## This should read version from the file
__version__ = None
with open(VERSION_FILE) as vf:
    exec(vf.read())
if not __version__:
    print("ERROR: Cannot read __version__ from file '%s'." % VERSION_FILE, file=sys.stderr)
    exit(1)

setup(
    name='pathling',
    packages=find_packages() + ['pathling.jars'],  # this must be the same as the name above
    version=__version__,
    description='Python API to Pathling',
    author='Piotr Szul',
    author_email='piotr.szul@csiro.au',
    url='https://github.com/aehrc/pathling',
    keywords=["pathling",
              "fhir",
              "analytics",
              "spark",
              "standards",
              "terminology"],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: Other/Proprietary License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    license="CSIRO Open Source Software Licence Agreement (variation of the BSD / MIT License)",
    python_requires=">=3.7",
    install_requires=[
        'pyspark>=3.1.0'
    ],
    include_package_data=True,
    package_dir={
        'pathling.jars': 'target/dependency',
    },
    package_data={
        'pathling.jars': ['*-all.jar'],
    },
)
