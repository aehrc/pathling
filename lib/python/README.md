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
from pathling import PathlingContext

pc = PathlingContext.create()

# Read each Bundle into a row within a Spark data set.
bundles_dir = '/some/path/bundles/'
bundles = pc.spark.read.text(bundles_dir, wholetext=True)

# Convert the data set of strings into a structured FHIR data set.
patients = pc.encode_bundle(bundles, 'Patient')

# JSON is the default format, XML Bundles can be encoded using input type.
# patients = pc.encodeBundle(bundles, 'Patient', inputType=MimeType.FHIR_XML)

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
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
the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
