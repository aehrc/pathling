Python API for Pathling
=======================

This is the Python API for [Pathling](https://pathling.csiro.au). It currently
supports encoding of [FHIR JSON bundles](https://hl7.org/fhir/R4/bundle.html)
and NDJSON into Apache Spark dataframes.

## Installation

Prerequisites:

- Python 3.8+ with pip
- PySpark 3.1+

To install, run this command:

```bash
pip install pathling  
```

## Usage

The code below shows an example of using the Pathling API to encode Patient
resources from FHIR JSON bundles:

```python
from pyspark.sql import SparkSession
from pathling import PathlingContext
from pathling.etc import find_jar

spark = SparkSession.builder \
    .appName('pathling-test') \
    .master('local[*]') \
    .config('spark.jars', find_jar()) \
    .getOrCreate()

ptl = PathlingContext.create(spark)
        
json_bundles = spark.read.text('examples/data/bundles/', wholetext=True)

patients_df = ptl.encodeBundle(json_bundles, 'Patient')
patients_df.show()
```

More usage examples can be found in the `examples` directory.

## Development setup

Create an isolated python environment with
[Miniconda](https://docs.conda.io/en/latest/miniconda.html), e.g:

```bash
conda create -n pathling-dev python=3.8
conda activate pathling-dev
```

Prerequisites:

- maven (Ubuntu 20.04: `apt install maven`)
- java (Ubuntu 20.04: `apt install default-jdk`)
- make (Ubuntu 20.04: `apt install make`)

To run the tests and install the package, run this command from the project
root:

```bash
mvn install -pl lib/python -am
```

Pathling is copyright © 2018-2022, Commonwealth Scientific and Industrial
Research Organisation
(CSIRO) ABN 41 687 119 230. Licensed under
the [CSIRO Open Source Software Licence Agreement](./LICENSE.md).
