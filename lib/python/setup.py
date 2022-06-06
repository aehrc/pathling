import glob
import os
import sys
from typing import Union

from setuptools import setup, find_packages

HERE = os.path.dirname(__file__)
ROOT_DIR = os.path.abspath(HERE)
VERSION_FILE = os.path.join(HERE, 'pathling', '_version.py')

#
# Read __version__ from file
#
__version__: Union[str, None] = None
if os.path.isfile(VERSION_FILE):
    with open(VERSION_FILE) as vf:
        exec(vf.read())
if not __version__:
    print("ERROR: Cannot read __version__ from file '%s'." % VERSION_FILE, file=sys.stderr)
    exit(1)

#
# Check for existence of uber-jar for packaging
#
JARS_DIR = os.path.join(HERE, 'target', 'dependency')
UBER_JAR_GLOB = os.path.join(JARS_DIR, 'encoders-*-all.jar')
jar_files = glob.glob(UBER_JAR_GLOB)
if not jar_files:
    print(
            "ERROR: Cannot find encoders uber-jar in: '%s'.\nMaybe try to run 'mvn package' ..." %
            JARS_DIR,
            file=sys.stderr)
    exit(1)
if len(jar_files) > 1:
    print(
            "ERROR: Multiple encoders uber-jars found in: '%s'.\nMaybe try to run 'mvn clean' ..." %
            JARS_DIR,
            file=sys.stderr)
    exit(1)
#
# Read the long description
#
with open('README.md') as f:
    long_description = f.read()

setup(
        name='pathling',
        packages=find_packages() + ['pathling.jars'],  # this must be the same as the name above
        version=__version__,
        description='Python API to Pathling',
        long_description=long_description,
        long_description_content_type="text/markdown",
        author='Piotr Szul',
        author_email='piotr.szul@csiro.au',
        url='https://github.com/aehrc/pathling',
        keywords=[
            "pathling",
            "fhir",
            "analytics",
            "spark",
            "standards",
            "terminology"
        ],
        classifiers=[
            'Development Status :: 3 - Alpha',
            'License :: Other/Proprietary License',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Programming Language :: Python :: 3.9',
            'Programming Language :: Python :: 3.10',
        ],
        license="CSIRO Open Source Software Licence Agreement",
        python_requires=">=3.7",
        extras_require={'spark': ['pyspark>=3.1.0']},
        include_package_data=True,
        package_dir={
            'pathling.jars': 'target/dependency',
        },
        package_data={
            'pathling.jars': ['*-all.jar'],
        },
        data_files=[
            ('share/pathling/examples', glob.glob('examples/*.py')),
            ('share/pathling/examples/data/resources',
             glob.glob('examples/data/resources/*.ndjson')),
            ('share/pathling/examples/data/bundles', glob.glob('examples/data/bundles/*.json')),
        ],
)
