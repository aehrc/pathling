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
    keywords=['testing', 'logging', 'example'],  # arbitrary keywords
    classifiers=[],
    license="CSIRO Open Source Software Licence Agreement",
    python_requires=">=3.7",
    #  install_requires=[
    #    'typedecorator==0.0.5'
    #  ],
    extras_require={
        'spark': [
            'pyspark>=3.2.1',
        ]
    },
    include_package_data=True,
    package_dir={
        'pathling.jars': 'target/dependency',
    },
    package_data={
        'pathling.jars': ['*-all.jar'],
    },
)
