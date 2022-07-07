#!/usr/bin/env python

import os

from pathling import PathlingContext

HERE = os.path.abspath(os.path.dirname(__file__))

pc = PathlingContext.create()

# Read each line from the NDJSON into a row within a Spark data set.
ndjson_dir = os.path.join(HERE, 'data/resources/')
json_resources = pc.spark.read.text(ndjson_dir)

# Convert the data set of strings into a structured FHIR data set.
patients = pc.encode(json_resources, 'Patient')

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
